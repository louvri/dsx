// Package dsx provides a type-safe, generic wrapper around Google Cloud Datastore.
// It simplifies common operations like querying, upserting, and deleting entities
// while providing a fluent API for building queries.
//
// Every operation takes a context.Context on the call that performs I/O, and
// every error is wrapped with the operation and kind that produced it. Lookups
// that find nothing report [ErrNotFound] rather than a nil entity, so a missing
// row can never be mistaken for a zero value.
//
// Example usage:
//
//	// Connect to Datastore
//	db, err := dsx.Connect(ctx, "my-project", "my-database")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Query entities
//	users, err := dsx.Query[User](db, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    WithOrderDesc("CreatedAt").
//	    WithLimit(50).
//	    Select(ctx)
//
//	// Upsert an entity
//	err = dsx.Query[User](db, "User").Upsert(ctx, "user-123", &user)
package dsx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Sentinel errors reported by dsx. Compare them with errors.Is; the returned
// errors are wrapped with the operation and kind that produced them.
var (
	// ErrNotFound reports that a lookup matched no entity. It is returned by
	// [QueryBuilder.Get] and [GetByKey] instead of a nil entity and a nil error.
	//
	//	user, err := dsx.GetByKey[User](ctx, db, "User", "user-123")
	//	if errors.Is(err, dsx.ErrNotFound) {
	//	    // no such user
	//	}
	ErrNotFound = errors.New("dsx: entity not found")

	// ErrPaginationConflict reports that a query mixes offset-based and
	// cursor-based pagination, which Datastore cannot satisfy together.
	ErrPaginationConflict = errors.New("dsx: offset and cursor pagination are mutually exclusive")
)

// Datastore service limits. Batch operations are split into chunks of these
// sizes so that callers never have to know or enforce them.
const (
	// maxCommitSize is the maximum number of mutations Datastore accepts in a
	// single commit.
	maxCommitSize = 500

	// maxLookupSize is the maximum number of keys Datastore accepts in a single
	// lookup.
	maxLookupSize = 1000

	// maxPrealloc caps how much of a caller's limit is allocated before any
	// result arrives, since a limit often comes straight from a request
	// parameter and need not reflect how many entities exist.
	maxPrealloc = 1024
)

type (
	// DB represents a connection to a Google Cloud Datastore database.
	// It wraps the datastore.Client and stores connection metadata, including
	// the default namespace applied to the keys and queries it creates.
	DB struct {
		client     *datastore.Client
		projectId  string
		databaseId string
		namespace  string
	}

	// QueryBuilder provides a fluent interface for constructing and executing
	// Datastore queries. It is generic over T, the entity type being queried.
	//
	// Builder methods never return an error. The first problem encountered while
	// building - an undecodable cursor, a malformed key filter - is recorded and
	// returned by the terminal call (Select, Get, Delete, ...). [QueryBuilder.Err]
	// exposes it earlier if a caller wants to check before executing.
	//
	// QueryBuilder tracks whether offset or cursor-based pagination is being used
	// to prevent incompatible combinations.
	QueryBuilder[T any] struct {
		db          *DB
		query       *datastore.Query
		kind        string
		namespace   string
		keyFilters  []keyFilter
		limit       int
		usingOffset bool
		usingCursor bool
		err         error
	}

	// keyFilter is a filter on FieldKey held until the terminal call, so that
	// the keys it resolves to pick up the query's final namespace regardless of
	// the order in which the builder methods were called.
	keyFilter struct {
		operator FilterOperator
		refs     []keyRef
		multi    bool // the filter value was a slice, as "in" and "not in" require
	}

	// keyRef is a key a filter was given, either already built by the caller or
	// still to be resolved from a name in the query's namespace.
	keyRef struct {
		key  *datastore.Key
		name string
	}

	// FilterOperator represents valid comparison operators for Datastore queries.
	// Use the predefined constants (OpEqual, OpGreater, etc.) for type safety.
	FilterOperator string

	// Option configures [Connect].
	Option func(*connectOptions)

	connectOptions struct {
		credentialsJSON string
		namespace       string
		clientOptions   []option.ClientOption
	}
)

const (
	// OpEqual filters for equality (=)
	OpEqual FilterOperator = "="
	// OpGreaterEqual filters for greater than or equal (>=)
	OpGreaterEqual FilterOperator = ">="
	// OpGreater filters for strictly greater than (>)
	OpGreater FilterOperator = ">"
	// OpLessEqual filters for less than or equal (<=)
	OpLessEqual FilterOperator = "<="
	// OpLess filters for strictly less than (<)
	OpLess FilterOperator = "<"
	// OpIn filters for membership in a list (in)
	// Value must be a slice, e.g., []string{"a", "b", "c"}
	OpIn FilterOperator = "in"
	// OpNotIn filters for non-membership in a list (not-in)
	// Value must be a slice
	OpNotIn FilterOperator = "not-in"
	// OpNotEqual filters for inequality (!=)
	OpNotEqual FilterOperator = "!="

	// FieldKey is a special field name used to filter by entity key.
	// When used with WithFilter, the value should be the entity's key name.
	//
	// Example:
	//   WithFilter(dsx.FieldKey, dsx.OpEqual, "entity-key")
	FieldKey string = "__key__"
)

// WithCredentialsJSON authenticates using an explicit service account
// credentials document rather than the ambient default credentials.
func WithCredentialsJSON(credentialsJSON string) Option {
	return func(o *connectOptions) { o.credentialsJSON = credentialsJSON }
}

// WithNamespace sets the default Datastore namespace for the connection.
// Every key and query created from the resulting DB uses it unless overridden
// by [DB.WithNamespace] or [QueryBuilder.WithNamespace].
//
// An empty namespace means the default namespace, which is also the behaviour
// when this option is not supplied.
func WithNamespace(namespace string) Option {
	return func(o *connectOptions) { o.namespace = namespace }
}

// WithClientOptions passes additional options straight through to the
// underlying datastore.Client. Use it for anything dsx does not model itself,
// such as a custom endpoint, a pre-dialled gRPC connection, or telemetry.
func WithClientOptions(opts ...option.ClientOption) Option {
	return func(o *connectOptions) { o.clientOptions = append(o.clientOptions, opts...) }
}

// Connect establishes a connection to Google Cloud Datastore.
//
// Parameters:
//   - ctx: Context for establishing the connection
//   - projectId: Google Cloud project ID
//   - databaseId: Datastore database ID (use "" for default database)
//   - opts: Optional settings, see [WithCredentialsJSON], [WithNamespace] and
//     [WithClientOptions]
//
// Returns a DB instance and any connection error.
//
// Example:
//
//	// Using default credentials (e.g., GOOGLE_APPLICATION_CREDENTIALS)
//	db, err := dsx.Connect(ctx, "my-project", "")
//
//	// Using explicit credentials, scoped to a tenant namespace
//	db, err := dsx.Connect(ctx, "my-project", "my-db",
//	    dsx.WithCredentialsJSON(credJSON),
//	    dsx.WithNamespace("tenant-42"))
func Connect(ctx context.Context, projectId, databaseId string, opts ...Option) (*DB, error) {
	var cfg connectOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	clientOptions := cfg.clientOptions
	if cfg.credentialsJSON != "" {
		clientOptions = append(clientOptions, option.WithCredentialsJSON([]byte(cfg.credentialsJSON)))
	}

	client, err := datastore.NewClientWithDatabase(ctx, projectId, databaseId, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("dsx: connect project=%s database=%s: %w", projectId, databaseId, err)
	}
	return &DB{client: client, projectId: projectId, databaseId: databaseId, namespace: cfg.namespace}, nil
}

// ProjectId returns the Google Cloud project ID for this connection.
func (db *DB) ProjectId() string {
	return db.projectId
}

// DatabaseId returns the Datastore database ID for this connection.
func (db *DB) DatabaseId() string {
	return db.databaseId
}

// Namespace returns the default Datastore namespace for this connection.
// An empty string means the default namespace.
func (db *DB) Namespace() string {
	return db.namespace
}

// WithNamespace returns a copy of db that uses the given namespace by default.
// Use it to scope a request to a tenant without reconnecting:
//
//	tenant := db.WithNamespace("tenant-42")
//	user, err := dsx.GetByKey[User](ctx, tenant, "User", "user-123")
//
// The copy shares the underlying datastore.Client, so it costs nothing to
// create and must not be closed separately - closing either copy closes the
// connection for all of them.
func (db *DB) WithNamespace(namespace string) *DB {
	clone := *db
	clone.namespace = namespace
	return &clone
}

// Client returns the underlying datastore.Client for advanced operations
// not covered by this wrapper.
func (db *DB) Client() *datastore.Client {
	return db.client
}

// Close releases the resources held by the database connection.
// It should be called when the DB is no longer needed.
func (db *DB) Close() error {
	return db.client.Close()
}

// nameKey builds a named key in this connection's namespace.
func (db *DB) nameKey(kind, name string) *datastore.Key {
	key := datastore.NameKey(kind, name, nil)
	key.Namespace = db.namespace
	return key
}

// nameKeys builds named keys in this connection's namespace.
func (db *DB) nameKeys(kind string, names []string) []*datastore.Key {
	keys := make([]*datastore.Key, len(names))
	for i, name := range names {
		keys[i] = db.nameKey(kind, name)
	}
	return keys
}

// GetMulti retrieves multiple entities by their string keys, in batches of
// [maxLookupSize] so that any number of keys can be requested at once.
//
// The result slice maintains the same order as the input keys.
//
// Note: entities that do not exist are left zero-valued in the result rather
// than reported as an error, so a caller that cannot distinguish a zero value
// from a missing entity should use [GetByKey] instead, which returns
// [ErrNotFound].
//
// Parameters:
//   - ctx: Context for the operation
//   - db: Database connection
//   - kind: Entity kind (table name)
//   - keys: Slice of string key names to retrieve
//
// Example:
//
//	users, err := dsx.GetMulti[User](ctx, db, "User", []string{"user-1", "user-2"})
func GetMulti[T any](ctx context.Context, db *DB, kind string, keys []string) ([]T, error) {
	if len(keys) == 0 {
		return []T{}, nil
	}

	nameKeys := db.nameKeys(kind, keys)
	result := make([]T, len(keys))
	for start := 0; start < len(nameKeys); start += maxLookupSize {
		end := min(start+maxLookupSize, len(nameKeys))
		// A MultiError of nothing but ErrNoSuchEntity means the lookup itself
		// succeeded and the absent entities stay zero-valued.
		if err := db.client.GetMulti(ctx, nameKeys[start:end], result[start:end]); err != nil && !onlyNoSuchEntity(err) {
			return nil, fmt.Errorf("dsx: get-multi %s [%d:%d]: %w", kind, start, end, err)
		}
	}

	return result, nil
}

// onlyNoSuchEntity reports whether err is a datastore.MultiError whose every
// non-nil element is ErrNoSuchEntity.
func onlyNoSuchEntity(err error) bool {
	var multi datastore.MultiError
	if !errors.As(err, &multi) {
		return false
	}
	for _, e := range multi {
		if e != nil && !errors.Is(e, datastore.ErrNoSuchEntity) {
			return false
		}
	}
	return true
}

// Query creates a new QueryBuilder for the specified entity kind.
// The type parameter T specifies the Go struct type that entities will be
// unmarshaled into.
//
// The query inherits db's default namespace; override it with
// [QueryBuilder.WithNamespace].
//
// Example:
//
//	type User struct {
//	    Name   string
//	    Email  string
//	    Status string
//	}
//
//	users, err := dsx.Query[User](db, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    Select(ctx)
func Query[T any](db *DB, kind string) *QueryBuilder[T] {
	return &QueryBuilder[T]{
		db:        db,
		query:     datastore.NewQuery(kind).Namespace(db.namespace),
		kind:      kind,
		namespace: db.namespace,
	}
}

// DB returns the database connection associated with this query.
func (qb *QueryBuilder[T]) DB() *DB {
	return qb.db
}

// Kind returns the entity kind (table name) being queried.
func (qb *QueryBuilder[T]) Kind() string {
	return qb.kind
}

// Namespace returns the namespace this query runs in.
// An empty string means the default namespace.
func (qb *QueryBuilder[T]) Namespace() string {
	return qb.namespace
}

// Err returns the first error recorded while building the query, or nil.
// Terminal calls report the same error, so checking it is optional.
func (qb *QueryBuilder[T]) Err() error {
	return qb.err
}

// fail records err as the builder's first error, if none was recorded yet.
func (qb *QueryBuilder[T]) fail(err error) *QueryBuilder[T] {
	if qb.err == nil {
		qb.err = err
	}
	return qb
}

// nameKey builds a named key in this query's namespace.
func (qb *QueryBuilder[T]) nameKey(name string) *datastore.Key {
	key := datastore.NameKey(qb.kind, name, nil)
	key.Namespace = qb.namespace
	return key
}

// incompleteKey builds an auto-ID key in this query's namespace.
func (qb *QueryBuilder[T]) incompleteKey() *datastore.Key {
	key := datastore.IncompleteKey(qb.kind, nil)
	key.Namespace = qb.namespace
	return key
}

// build returns the finished query, resolving any deferred key filters against
// the namespace the builder ended up with, or the first build error.
func (qb *QueryBuilder[T]) build() (*datastore.Query, error) {
	if qb.err != nil {
		return nil, qb.err
	}
	query := qb.query
	for _, filter := range qb.keyFilters {
		if !filter.multi {
			query = query.FilterField(FieldKey, string(filter.operator), qb.resolveKeyRef(filter.refs[0]))
			continue
		}
		keys := make([]any, len(filter.refs))
		for i, ref := range filter.refs {
			keys[i] = qb.resolveKeyRef(ref)
		}
		query = query.FilterField(FieldKey, string(filter.operator), keys)
	}
	return query, nil
}

// resolveKeyRef turns a recorded key reference into a key in the query's
// namespace. A key the caller built themselves is used as it was given.
func (qb *QueryBuilder[T]) resolveKeyRef(ref keyRef) *datastore.Key {
	if ref.key != nil {
		return ref.key
	}
	return qb.nameKey(ref.name)
}

// membershipValues converts the value given to an "in" or "not in" filter into
// the []any that Datastore's value encoding accepts, so that an ordinary
// []string or []int works rather than only a []any.
func membershipValues(value any) ([]any, error) {
	if elements, ok := value.([]any); ok {
		return elements, nil
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || (reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array) {
		return nil, fmt.Errorf("value must be a slice, got %T", value)
	}
	elements := make([]any, reflected.Len())
	for i := range elements {
		elements[i] = reflected.Index(i).Interface()
	}
	return elements, nil
}

// valid reports whether the operator is one dsx defines. An unknown operator
// would otherwise be rejected inside the Datastore client, which Count does not
// surface - the filter would be dropped and the count taken over more rows.
func (o FilterOperator) valid() bool {
	switch o {
	case OpEqual, OpNotEqual, OpGreater, OpGreaterEqual, OpLess, OpLessEqual, OpIn, OpNotIn:
		return true
	}
	return false
}

// keyRefOf records how a key filter value should later become a key.
func keyRefOf(value any) (keyRef, error) {
	switch typed := value.(type) {
	case string:
		if typed == "" {
			return keyRef{}, errors.New("key name must not be empty")
		}
		return keyRef{name: typed}, nil
	case *datastore.Key:
		if typed == nil {
			return keyRef{}, errors.New("key must not be nil")
		}
		return keyRef{key: typed}, nil
	default:
		return keyRef{}, fmt.Errorf("value must be a string or *datastore.Key, got %T", value)
	}
}

// WithNamespace runs the query in the given namespace, overriding the
// connection default. It also applies to the keys written or deleted through
// this builder, and may be called at any point in the chain.
//
// An empty namespace selects the default namespace.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	users, err := dsx.Query[User](db, "User").
//	    WithNamespace("tenant-42").
//	    Select(ctx)
func (qb *QueryBuilder[T]) WithNamespace(namespace string) *QueryBuilder[T] {
	qb.namespace = namespace
	qb.query = qb.query.Namespace(namespace)
	return qb
}

// WithDistinct marks the query to return only distinct results.
// Typically used with projection queries.
//
// Returns the QueryBuilder for method chaining.
func (qb *QueryBuilder[T]) WithDistinct() *QueryBuilder[T] {
	qb.query = qb.query.Distinct()
	return qb
}

// WithLimit sets the maximum number of entities to return.
// A limit of 0 or negative is ignored.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// Get at most 10 users
//	users, err := dsx.Query[User](db, "User").
//	    WithLimit(10).
//	    Select(ctx)
func (qb *QueryBuilder[T]) WithLimit(limit int) *QueryBuilder[T] {
	if limit > 0 {
		qb.query = qb.query.Limit(limit)
		qb.limit = limit
	}
	return qb
}

// WithOffset sets the number of entities to skip before returning results.
// An offset of 0 or negative is ignored.
//
// Note: Using offset marks the query as offset-based pagination, which is
// incompatible with cursor-based pagination (SelectWithCursor). Combining the
// two records [ErrPaginationConflict] on the builder.
//
// Warning: Datastore has a maximum offset of 1000. For larger offsets,
// use cursor-based pagination instead.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// Skip first 20, get next 10 (page 3 with limit 10)
//	users, err := dsx.Query[User](db, "User").
//	    WithOffset(20).
//	    WithLimit(10).
//	    Select(ctx)
func (qb *QueryBuilder[T]) WithOffset(offset int) *QueryBuilder[T] {
	if offset <= 0 {
		return qb
	}
	if qb.usingCursor {
		return qb.fail(ErrPaginationConflict)
	}
	qb.query = qb.query.Offset(offset)
	qb.usingOffset = true
	return qb
}

// WithOrder adds an ascending sort order on the specified field.
// Can be called multiple times to sort by multiple fields.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// Sort by Status ascending, then by Name ascending
//	users, err := dsx.Query[User](db, "User").
//	    WithOrder("Status").
//	    WithOrder("Name").
//	    Select(ctx)
func (qb *QueryBuilder[T]) WithOrder(field string) *QueryBuilder[T] {
	qb.query = qb.query.Order(field)
	return qb
}

// WithOrderDesc adds a descending sort order on the specified field.
// Can be called multiple times to sort by multiple fields.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// Get newest users first
//	users, err := dsx.Query[User](db, "User").
//	    WithOrderDesc("CreatedAt").
//	    Select(ctx)
func (qb *QueryBuilder[T]) WithOrderDesc(field string) *QueryBuilder[T] {
	qb.query = qb.query.Order("-" + field)
	return qb
}

// WithCursor sets the starting point for cursor-based pagination.
// The cursor string should be obtained from a previous SelectWithCursor call.
// An empty cursor is ignored.
//
// Note: Using a cursor marks the query as cursor-based pagination, which is
// incompatible with offset-based pagination (Select with WithOffset).
//
// An undecodable cursor is recorded on the builder and returned by the terminal
// call.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// First page
//	users, cursor, err := dsx.Query[User](db, "User").
//	    WithLimit(50).
//	    SelectWithCursor(ctx)
//
//	// Next page
//	users, cursor, err = dsx.Query[User](db, "User").
//	    WithLimit(50).
//	    WithCursor(cursor).
//	    SelectWithCursor(ctx)
func (qb *QueryBuilder[T]) WithCursor(cursor string) *QueryBuilder[T] {
	if cursor == "" {
		return qb
	}
	if qb.usingOffset {
		return qb.fail(ErrPaginationConflict)
	}
	decoded, err := datastore.DecodeCursor(cursor)
	if err != nil {
		return qb.fail(fmt.Errorf("dsx: %s: decode cursor: %w", qb.kind, err))
	}
	qb.query = qb.query.Start(decoded)
	qb.usingCursor = true
	return qb
}

// WithFilter adds a filter condition to the query.
// Can be called multiple times to add multiple filters (AND logic).
//
// When filtering by FieldKey ("__key__"), pass the entity's key name as the value;
// it will be automatically converted to a datastore.Key in the query's namespace.
// A value of any other type records an error on the builder, which the terminal
// call returns.
//
// OpIn and OpNotIn take any slice - []string, []int, []any - and the elements
// are converted for you.
//
// Parameters:
//   - field: Field name to filter on (use FieldKey for entity key)
//   - operator: Comparison operator (OpEqual, OpGreater, etc.)
//   - value: Value to compare against, or the slice to match against for
//     OpIn and OpNotIn
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// Single filter
//	users, err := dsx.Query[User](db, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    Select(ctx)
//
//	// Multiple filters (AND)
//	users, err := dsx.Query[User](db, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    WithFilter("Age", dsx.OpGreaterEqual, 18).
//	    Select(ctx)
//
//	// Filter by key
//	users, err := dsx.Query[User](db, "User").
//	    WithFilter(dsx.FieldKey, dsx.OpEqual, "user-123").
//	    Select(ctx)
//
//	// IN filter
//	users, err := dsx.Query[User](db, "User").
//	    WithFilter("Status", dsx.OpIn, []string{"active", "pending"}).
//	    Select(ctx)
func (qb *QueryBuilder[T]) WithFilter(field string, operator FilterOperator, value any) *QueryBuilder[T] {
	if !operator.valid() {
		return qb.fail(fmt.Errorf("dsx: %s: unknown filter operator %q on %s", qb.kind, operator, field))
	}

	membership := operator == OpIn || operator == OpNotIn

	var elements []any
	if membership {
		var err error
		if elements, err = membershipValues(value); err != nil {
			return qb.fail(fmt.Errorf("dsx: %s: filter %s %s: %w", qb.kind, field, operator, err))
		}
	}

	if field != FieldKey {
		if membership {
			qb.query = qb.query.FilterField(field, string(operator), elements)
			return qb
		}
		qb.query = qb.query.FilterField(field, string(operator), value)
		return qb
	}

	// Key filters are recorded now but resolved at build time, so that a
	// WithNamespace call later in the chain still applies to them.
	if !membership {
		elements = []any{value}
	}
	refs := make([]keyRef, len(elements))
	for i, element := range elements {
		ref, err := keyRefOf(element)
		if err != nil {
			return qb.fail(fmt.Errorf("dsx: %s: filter on %s: %w", qb.kind, FieldKey, err))
		}
		refs[i] = ref
	}
	qb.keyFilters = append(qb.keyFilters, keyFilter{operator: operator, refs: refs, multi: membership})
	return qb
}

// WithAncestorKey filters the query to only return entities that are
// descendants of the specified ancestor key. This enables strongly
// consistent queries within an entity group.
//
// A nil ancestor key is ignored.
//
// The ancestor key is used exactly as given, including its namespace. On a
// namespaced connection, build it in the same namespace - Datastore rejects a
// query whose ancestor sits in a different partition:
//
//	companyKey := datastore.NameKey("Company", "acme", nil)
//	companyKey.Namespace = db.Namespace()
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	companyKey := datastore.NameKey("Company", "acme", nil)
//	employees, err := dsx.Query[Employee](db, "Employee").
//	    WithAncestorKey(companyKey).
//	    Select(ctx)
func (qb *QueryBuilder[T]) WithAncestorKey(ancestorKey *datastore.Key) *QueryBuilder[T] {
	if ancestorKey != nil {
		qb.query = qb.query.Ancestor(ancestorKey)
	}
	return qb
}

// WithProject sets the query to return only the specified fields (projection query).
// This is more efficient when you only need a subset of entity fields, as it
// avoids loading the full entity.
//
// Note: Projected fields must be indexed. Properties with noindex tags cannot be projected.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// Only fetch Name and Email fields
//	users, err := dsx.Query[User](db, "User").
//	    WithProject("Name", "Email").
//	    Select(ctx)
func (qb *QueryBuilder[T]) WithProject(fields ...string) *QueryBuilder[T] {
	qb.query = qb.query.Project(fields...)
	return qb
}

// Count returns the count of entities matching the current query filters.
// It uses Datastore's aggregation query to efficiently count without loading entities into memory.
//
// Example:
//
//	count, err := dsx.Query[User](db, "User").
//		WithFilter("Status", dsx.OpEqual, "active").
//		Count(ctx)
//
// Returns 0 and an error if the aggregation query fails or the count result is missing.
// Note: Datastore count aggregations have a default limit of approximately 1 million entities.
func (qb *QueryBuilder[T]) Count(ctx context.Context) (int64, error) {
	query, err := qb.build()
	if err != nil {
		return 0, err
	}

	results, err := qb.db.client.RunAggregationQuery(ctx, query.NewAggregationQuery().WithCount("total"))
	if err != nil {
		return 0, fmt.Errorf("dsx: count %s: %w", qb.kind, err)
	}
	count, ok := results["total"]
	if !ok {
		return 0, fmt.Errorf("dsx: count %s: aggregation result missing", qb.kind)
	}
	value, ok := count.(*datastorepb.Value)
	if !ok {
		return 0, fmt.Errorf("dsx: count %s: unexpected aggregation result type %T", qb.kind, count)
	}
	return value.GetIntegerValue(), nil
}

// SelectWithCursor executes the query and returns results with a cursor
// for pagination. The cursor can be passed to WithCursor in subsequent
// queries to fetch the next page.
//
// This method uses an iterator internally, which may be slightly slower
// than Select for simple queries, but enables efficient deep pagination.
//
// Returns [ErrPaginationConflict] if the query was configured with WithOffset,
// as offset and cursor pagination are mutually exclusive.
//
// Example:
//
//	// Paginate through all active users
//	var allUsers []User
//	cursor := ""
//	for {
//	    users, nextCursor, err := dsx.Query[User](db, "User").
//	        WithFilter("Status", dsx.OpEqual, "active").
//	        WithLimit(100).
//	        WithCursor(cursor).
//	        SelectWithCursor(ctx)
//	    if err != nil {
//	        return err
//	    }
//	    allUsers = append(allUsers, users...)
//	    if len(users) < 100 {
//	        break // last page
//	    }
//	    cursor = nextCursor
//	}
func (qb *QueryBuilder[T]) SelectWithCursor(ctx context.Context) ([]T, string, error) {
	query, err := qb.build()
	if err != nil {
		return nil, "", err
	}
	if qb.usingOffset {
		return nil, "", ErrPaginationConflict
	}

	result := make([]T, 0, min(qb.limit, maxPrealloc))
	it := qb.db.client.Run(ctx, query)
	for {
		var entity T
		if _, err := it.Next(&entity); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, "", fmt.Errorf("dsx: select-with-cursor %s: %w", qb.kind, err)
		}
		result = append(result, entity)
	}

	cursor, err := it.Cursor()
	if err != nil {
		return nil, "", fmt.Errorf("dsx: select-with-cursor %s: cursor: %w", qb.kind, err)
	}

	return result, cursor.String(), nil
}

// Select executes the query and returns all matching entities.
// This uses GetAll internally, which is slightly faster than iterator-based
// methods for simple queries.
//
// Returns [ErrPaginationConflict] if the query was configured with WithCursor,
// as cursor pagination requires SelectWithCursor.
//
// Example:
//
//	users, err := dsx.Query[User](db, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    WithOrderDesc("CreatedAt").
//	    WithLimit(50).
//	    Select(ctx)
func (qb *QueryBuilder[T]) Select(ctx context.Context) ([]T, error) {
	query, err := qb.build()
	if err != nil {
		return nil, err
	}
	if qb.usingCursor {
		return nil, ErrPaginationConflict
	}

	var result []T
	if _, err := qb.db.client.GetAll(ctx, query, &result); err != nil {
		return nil, fmt.Errorf("dsx: select %s: %w", qb.kind, err)
	}

	return result, nil
}

// SelectKeys executes the query and returns only the keys of the matching
// entities, without loading the entities themselves. It is the cheap way to
// find out which entities match - for an existence check, to hand the keys to
// [RunInTransaction], or to count identifiers you then look up selectively.
//
// Returns [ErrPaginationConflict] if the query was configured with WithCursor.
//
// Example:
//
//	keys, err := dsx.Query[User](db, "User").
//	    WithFilter("Status", dsx.OpEqual, "inactive").
//	    SelectKeys(ctx)
func (qb *QueryBuilder[T]) SelectKeys(ctx context.Context) ([]*datastore.Key, error) {
	query, err := qb.build()
	if err != nil {
		return nil, err
	}
	if qb.usingCursor {
		return nil, ErrPaginationConflict
	}

	keys, err := qb.db.client.GetAll(ctx, query.KeysOnly(), nil)
	if err != nil {
		return nil, fmt.Errorf("dsx: select-keys %s: %w", qb.kind, err)
	}

	return keys, nil
}

// Get executes the query and returns the first matching entity.
// Useful for queries expected to return a single result; the query is limited
// to one entity regardless of any WithLimit already applied.
//
// Returns [ErrNotFound] if no entity matches, and [ErrPaginationConflict] if the
// query was configured with WithCursor.
//
// Example:
//
//	user, err := dsx.Query[User](db, "User").
//	    WithFilter("Email", dsx.OpEqual, "john@example.com").
//	    Get(ctx)
//	if errors.Is(err, dsx.ErrNotFound) {
//	    // no such user
//	}
func (qb *QueryBuilder[T]) Get(ctx context.Context) (*T, error) {
	query, err := qb.build()
	if err != nil {
		return nil, err
	}
	if qb.usingCursor {
		return nil, ErrPaginationConflict
	}

	var result []T
	if _, err := qb.db.client.GetAll(ctx, query.Limit(1), &result); err != nil {
		return nil, fmt.Errorf("dsx: get %s: %w", qb.kind, err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("dsx: get %s: %w", qb.kind, ErrNotFound)
	}

	return &result[0], nil
}

// Upsert inserts or updates a single entity with the specified key name.
// If an entity with the key exists, it is overwritten; otherwise, a new
// entity is created.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: String key name for the entity
//   - data: Pointer to the entity data
//
// Example:
//
//	user := User{Name: "John", Email: "john@example.com", Status: "active"}
//	err := dsx.Query[User](db, "User").Upsert(ctx, "user-123", &user)
func (qb *QueryBuilder[T]) Upsert(ctx context.Context, key string, data *T) error {
	if qb.err != nil {
		return qb.err
	}

	if _, err := qb.db.client.Put(ctx, qb.nameKey(key), data); err != nil {
		return fmt.Errorf("dsx: upsert %s/%s: %w", qb.kind, key, err)
	}

	return nil
}

// InsertWithAutoKey inserts a new entity with an auto-generated key and returns the complete key.
// This always creates a new entity since Datastore assigns a unique key.
//
// Use this when you don't need to control the entity's key but need to know
// the generated key after insertion (e.g., for returning the key to a client or logging).
//
// Parameters:
//   - ctx: Context for the operation
//   - data: Pointer to the entity data
//
// Returns the complete key, or an error if insertion fails.
//
// Example:
//
//	order := Order{
//	    CustomerID: "cust-123",
//	    Total:      99.99,
//	    CreatedAt:  time.Now(),
//	}
//	key, err := dsx.Query[Order](db, "Order").InsertWithAutoKey(ctx, &order)
//	if err != nil {
//	    return err
//	}
//	fmt.Printf("Created order with key ID: %d\n", key.ID)
func (qb *QueryBuilder[T]) InsertWithAutoKey(ctx context.Context, data *T) (*datastore.Key, error) {
	if qb.err != nil {
		return nil, qb.err
	}

	completeKey, err := qb.db.client.Put(ctx, qb.incompleteKey(), data)
	if err != nil {
		return nil, fmt.Errorf("dsx: insert-with-auto-key %s: %w", qb.kind, err)
	}
	return completeKey, nil
}

// UpsertMulti inserts or updates multiple entities, in batches of
// [maxCommitSize] so that any number of entities can be written at once.
//
// Parameters:
//   - ctx: Context for the operation
//   - items: Map of string key name to entity pointer
//
// Note: each batch is committed independently, so a failure part-way through a
// large write leaves the batches before it applied. Keys are written in sorted
// order, so batch boundaries and the keys named by an error are stable across
// runs; the error names the first and last key of the batch that failed.
//
// Example:
//
//	users := map[string]*User{
//	    "user-1": {Name: "Alice", Status: "active"},
//	    "user-2": {Name: "Bob", Status: "active"},
//	}
//	err := dsx.Query[User](db, "User").UpsertMulti(ctx, users)
func (qb *QueryBuilder[T]) UpsertMulti(ctx context.Context, items map[string]*T) error {
	if qb.err != nil {
		return qb.err
	}
	if len(items) == 0 {
		return nil
	}

	// Sorted so that batch boundaries, and so the keys named by a failure, are
	// the same on every run rather than following Go's map iteration order.
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	slices.Sort(names)

	keys := make([]*datastore.Key, len(names))
	entities := make([]*T, len(names))
	for i, name := range names {
		keys[i] = qb.nameKey(name)
		entities[i] = items[name]
	}

	for start := 0; start < len(keys); start += maxCommitSize {
		end := min(start+maxCommitSize, len(keys))
		if _, err := qb.db.client.PutMulti(ctx, keys[start:end], entities[start:end]); err != nil {
			return fmt.Errorf("dsx: upsert-multi %s [%s..%s]: %w", qb.kind, names[start], names[end-1], err)
		}
	}

	return nil
}

// InsertMultiWithAutoKey inserts multiple entities with auto-generated keys,
// in batches of [maxCommitSize], and returns the complete keys in input order.
//
// Note: each batch is committed independently. If a batch fails, the keys of
// the batches already committed are returned alongside the error, since those
// entities exist and the caller would otherwise have no way to reference or
// clean them up.
//
// Example:
//
//	orders := []*Order{
//	    {CustomerID: "cust-1", Total: 10.00},
//	    {CustomerID: "cust-2", Total: 20.00},
//	}
//	keys, err := dsx.Query[Order](db, "Order").InsertMultiWithAutoKey(ctx, orders)
func (qb *QueryBuilder[T]) InsertMultiWithAutoKey(ctx context.Context, entities []*T) ([]*datastore.Key, error) {
	if qb.err != nil {
		return nil, qb.err
	}
	if len(entities) == 0 {
		return []*datastore.Key{}, nil
	}

	keys := make([]*datastore.Key, len(entities))
	for i := range keys {
		keys[i] = qb.incompleteKey()
	}

	completeKeys := make([]*datastore.Key, 0, len(entities))
	for start := 0; start < len(entities); start += maxCommitSize {
		end := min(start+maxCommitSize, len(entities))
		batch, err := qb.db.client.PutMulti(ctx, keys[start:end], entities[start:end])
		if err != nil {
			// The batches before this one are committed. Return their keys so
			// the caller can reference or clean up what was written.
			return completeKeys, fmt.Errorf("dsx: insert-multi-with-auto-key %s [%d:%d]: %w", qb.kind, start, end, err)
		}
		completeKeys = append(completeKeys, batch...)
	}
	return completeKeys, nil
}

// Delete removes all entities matching the current query filters.
//
// Keys are streamed from a keys-only query and deleted in batches of
// [maxCommitSize], so memory use stays constant no matter how many entities
// match. Each batch is committed independently, so a failure part-way through
// leaves the batches before it deleted.
//
// Warning: Without filters, this will delete ALL entities of the kind.
// Use with caution.
//
// Example:
//
//	// Delete all inactive users
//	err := dsx.Query[User](db, "User").
//	    WithFilter("Status", dsx.OpEqual, "inactive").
//	    Delete(ctx)
//
//	// Delete a specific user
//	err := dsx.Query[User](db, "User").
//	    WithFilter(dsx.FieldKey, dsx.OpEqual, "user-123").
//	    Delete(ctx)
func (qb *QueryBuilder[T]) Delete(ctx context.Context) error {
	query, err := qb.build()
	if err != nil {
		return err
	}

	batch := make([]*datastore.Key, 0, maxCommitSize)
	it := qb.db.client.Run(ctx, query.KeysOnly())
	for {
		key, err := it.Next(nil)
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return fmt.Errorf("dsx: delete %s: scan keys: %w", qb.kind, err)
		}

		batch = append(batch, key)
		if len(batch) < maxCommitSize {
			continue
		}
		if err := qb.db.client.DeleteMulti(ctx, batch); err != nil {
			return fmt.Errorf("dsx: delete %s: %w", qb.kind, err)
		}
		batch = batch[:0]
	}

	if len(batch) > 0 {
		if err := qb.db.client.DeleteMulti(ctx, batch); err != nil {
			return fmt.Errorf("dsx: delete %s: %w", qb.kind, err)
		}
	}
	return nil
}

// GetByKey retrieves a single entity by its string key name, in db's namespace.
//
// Returns [ErrNotFound] if the entity does not exist.
//
// Example:
//
//	user, err := dsx.GetByKey[User](ctx, db, "User", "user-123")
//	if errors.Is(err, dsx.ErrNotFound) {
//	    // no such user
//	}
func GetByKey[T any](ctx context.Context, db *DB, kind string, key string) (*T, error) {
	var entity T
	if err := db.client.Get(ctx, db.nameKey(kind, key), &entity); err != nil {
		if errors.Is(err, datastore.ErrNoSuchEntity) {
			return nil, fmt.Errorf("dsx: get-by-key %s/%s: %w", kind, key, ErrNotFound)
		}
		return nil, fmt.Errorf("dsx: get-by-key %s/%s: %w", kind, key, err)
	}
	return &entity, nil
}

// DeleteByKey deletes a single entity by its string key name, in db's namespace.
// Deleting an entity that does not exist is not an error.
//
// Example:
//
//	err := dsx.DeleteByKey(ctx, db, "User", "user-123")
func DeleteByKey(ctx context.Context, db *DB, kind string, key string) error {
	if err := db.client.Delete(ctx, db.nameKey(kind, key)); err != nil {
		return fmt.Errorf("dsx: delete-by-key %s/%s: %w", kind, key, err)
	}
	return nil
}

// DeleteMultiByKey deletes multiple entities by their string key names, in
// batches of [maxCommitSize] so that any number of keys can be deleted at once.
//
// Note: each batch is committed independently, so a failure part-way through a
// large delete leaves the batches before it deleted.
//
// Example:
//
//	err := dsx.DeleteMultiByKey(ctx, db, "User", []string{"user-1", "user-2"})
func DeleteMultiByKey(ctx context.Context, db *DB, kind string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	nameKeys := db.nameKeys(kind, keys)
	for start := 0; start < len(nameKeys); start += maxCommitSize {
		end := min(start+maxCommitSize, len(nameKeys))
		if err := db.client.DeleteMulti(ctx, nameKeys[start:end]); err != nil {
			return fmt.Errorf("dsx: delete-multi-by-key %s [%d:%d]: %w", kind, start, end, err)
		}
	}
	return nil
}

// RunInTransaction executes the given function within a Datastore transaction.
// If fn returns nil, the transaction is committed. If fn returns an error,
// the transaction is rolled back.
//
// Keys are built by the caller, so a transaction on a namespaced database must
// set the namespace on the keys it uses; [DB.Namespace] reports the one in
// effect.
//
// Datastore transactions are limited to 25 entity groups and have a maximum
// duration of 270 seconds.
//
// Example:
//
//	err := dsx.RunInTransaction(ctx, db, func(tx *datastore.Transaction) error {
//	    var user User
//	    key := datastore.NameKey("User", "user-123", nil)
//	    key.Namespace = db.Namespace()
//	    if err := tx.Get(key, &user); err != nil {
//	        return err
//	    }
//	    user.Balance += 100
//	    _, err := tx.Put(key, &user)
//	    return err
//	})
func RunInTransaction(ctx context.Context, db *DB, fn func(tx *datastore.Transaction) error) error {
	if _, err := db.client.RunInTransaction(ctx, fn); err != nil {
		return fmt.Errorf("dsx: transaction: %w", err)
	}
	return nil
}
