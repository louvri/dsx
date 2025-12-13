// Package dsx provides a type-safe, generic wrapper around Google Cloud Datastore.
// It simplifies common operations like querying, upserting, and deleting entities
// while providing a fluent API for building queries.
//
// Example usage:
//
//	// Connect to Datastore
//	db, err := dsx.Connect(ctx, "my-project", "my-database", "")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Query entities
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    WithOrderDesc("CreatedAt").
//	    WithLimit(50).
//	    Select()
//
//	// Upsert an entity
//	err = dsx.Query[User](db, ctx, "User").Upsert("user-123", &user)
package dsx

import (
	"context"
	"errors"
	"fmt"
	"log"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type (
	// DB represents a connection to a Google Cloud Datastore database.
	// It wraps the datastore.Client and stores connection metadata.
	DB struct {
		client     *datastore.Client
		projectId  string
		databaseId string
	}

	// QueryBuilder provides a fluent interface for constructing and executing
	// Datastore queries. It is generic over T, the entity type being queried.
	//
	// QueryBuilder tracks whether offset or cursor-based pagination is being used
	// to prevent incompatible combinations.
	QueryBuilder[T any] struct {
		context     context.Context
		db          *DB
		query       *datastore.Query
		kind        string
		limit       int
		usingOffset bool
		usingCursor bool
	}

	// FilterOperator represents valid comparison operators for Datastore queries.
	// Use the predefined constants (OpEqual, OpGreater, etc.) for type safety.
	FilterOperator string
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
	// OpNotIn filters for non-membership in a list (not in)
	// Value must be a slice
	OpNotIn FilterOperator = "not in"

	// FieldKey is a special field name used to filter by entity key.
	// When used with WithFilter, the value should be the string ID of the entity.
	//
	// Example:
	//   WithFilter(dsx.FieldKey, dsx.OpEqual, "entity-id")
	FieldKey string = "__key__"
)

// Connect establishes a connection to Google Cloud Datastore.
//
// Parameters:
//   - ctx: Context for the connection
//   - projectId: Google Cloud project ID
//   - databaseId: Datastore database ID (use "" for default database)
//   - credentialsJSON: JSON credentials string (use "" to use default credentials)
//
// Returns a DB instance and any connection error.
//
// Example:
//
//	// Using default credentials (e.g., GOOGLE_APPLICATION_CREDENTIALS)
//	db, err := dsx.Connect(ctx, "my-project", "", "")
//
//	// Using explicit credentials
//	db, err := dsx.Connect(ctx, "my-project", "my-db", credJSON)
func Connect(ctx context.Context, projectId, databaseId, credentialsJSON string) (result *DB, err error) {
	var client *datastore.Client
	if credentialsJSON != "" {
		client, err = datastore.NewClientWithDatabase(ctx, projectId, databaseId, option.WithCredentialsJSON([]byte(credentialsJSON)))
	} else {
		client, err = datastore.NewClientWithDatabase(ctx, projectId, databaseId)
	}
	return &DB{client: client, projectId: projectId, databaseId: databaseId}, err
}

// ProjectId returns the Google Cloud project ID for this connection.
func (db *DB) ProjectId() string {
	return db.projectId
}

// DatabaseId returns the Datastore database ID for this connection.
func (db *DB) DatabaseId() string {
	return db.databaseId
}

// Client returns the underlying datastore.Client for advanced operations
// not covered by this wrapper.
func (db *DB) Client() *datastore.Client {
	return db.client
}

// Query creates a new QueryBuilder for the specified entity kind.
// The type parameter T specifies the Go struct type that entities will be
// unmarshaled into.
//
// Example:
//
//	type User struct {
//	    Name   string
//	    Email  string
//	    Status string
//	}
//
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    Select()
func Query[T any](db *DB, ctx context.Context, kind string) *QueryBuilder[T] {
	return &QueryBuilder[T]{
		context:     ctx,
		db:          db,
		query:       datastore.NewQuery(kind),
		kind:        kind,
		usingOffset: false,
		usingCursor: false,
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
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithLimit(10).
//	    Select()
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
// incompatible with cursor-based pagination (SelectWithCursor).
//
// Warning: Datastore has a maximum offset of 1000. For larger offsets,
// use cursor-based pagination instead.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// Skip first 20, get next 10 (page 3 with limit 10)
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithOffset(20).
//	    WithLimit(10).
//	    Select()
func (qb *QueryBuilder[T]) WithOffset(offset int) *QueryBuilder[T] {
	if offset > 0 {
		qb.query = qb.query.Offset(offset)
		qb.usingOffset = true
	}
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
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithOrder("Status").
//	    WithOrder("Name").
//	    Select()
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
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithOrderDesc("CreatedAt").
//	    Select()
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
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// First page
//	users, cursor, err := dsx.Query[User](db, ctx, "User").
//	    WithLimit(50).
//	    SelectWithCursor()
//
//	// Next page
//	users, cursor, err = dsx.Query[User](db, ctx, "User").
//	    WithLimit(50).
//	    WithCursor(cursor).
//	    SelectWithCursor()
func (qb *QueryBuilder[T]) WithCursor(cursor string) *QueryBuilder[T] {
	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err == nil {
			qb.query = qb.query.Start(c)
			qb.usingCursor = true
		}
	}
	return qb
}

// WithFilter adds a filter condition to the query.
// Can be called multiple times to add multiple filters (AND logic).
//
// When filtering by FieldKey ("__key__"), pass the string ID as the value;
// it will be automatically converted to a datastore.Key.
//
// Parameters:
//   - key: Field name to filter on (use FieldKey for entity key)
//   - operator: Comparison operator (OpEqual, OpGreater, etc.)
//   - value: Value to compare against
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	// Single filter
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    Select()
//
//	// Multiple filters (AND)
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    WithFilter("Age", dsx.OpGreaterEqual, 18).
//	    Select()
//
//	// Filter by key
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithFilter(dsx.FieldKey, dsx.OpEqual, "user-123").
//	    Select()
//
//	// IN filter
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithFilter("Status", dsx.OpIn, []string{"active", "pending"}).
//	    Select()
func (qb *QueryBuilder[T]) WithFilter(key string, operator FilterOperator, value interface{}) *QueryBuilder[T] {
	if key == FieldKey {
		if tmp, ok := value.(string); ok {
			qb.query = qb.query.FilterField(key, string(operator), datastore.NameKey(qb.kind, tmp, nil))
		}
	} else {
		qb.query = qb.query.FilterField(key, string(operator), value)
	}
	return qb
}

// WithAncestorKey filters the query to only return entities that are
// descendants of the specified ancestor key. This enables strongly
// consistent queries within an entity group.
//
// A nil ancestor key is ignored.
//
// Returns the QueryBuilder for method chaining.
//
// Example:
//
//	companyKey := datastore.NameKey("Company", "acme", nil)
//	employees, err := dsx.Query[Employee](db, ctx, "Employee").
//	    WithAncestorKey(companyKey).
//	    Select()
func (qb *QueryBuilder[T]) WithAncestorKey(ancestorKey *datastore.Key) *QueryBuilder[T] {
	if ancestorKey != nil {
		qb.query = qb.query.Ancestor(ancestorKey)
	}
	return qb
}

// KeysOnly marks the query to return only entity keys, not full entities.
// This is more efficient when you only need keys (e.g., for counting or
// batch deletion).
//
// Note: After calling KeysOnly, use SelectKeys instead of Select.
//
// Returns the QueryBuilder for method chaining.
func (qb *QueryBuilder[T]) KeysOnly() *QueryBuilder[T] {
	qb.query = qb.query.KeysOnly()
	return qb
}

// Total returns the count of entities matching the current query filters.
// It uses Datastore's aggregation query to efficiently count without loading entities into memory.
//
// Example:
//
//	count, err := dsx.From[User](ctx, db).
//		Where("Status", "=", "active").
//		Total()
//
// Returns 0 and an error if the aggregation query fails or the count result is missing.
// Note: Datastore count aggregations have a default limit of approximately 1 million entities.
func (qb *QueryBuilder[T]) Total() (int64, error) {
	aggQuery := qb.query.NewAggregationQuery().WithCount("total")
	results, err := qb.db.client.RunAggregationQuery(qb.context, aggQuery)
	if err != nil {
		return 0, err
	}
	count, ok := results["total"]
	if !ok {
		return 0, errors.New("count result not found")
	}
	val, ok := count.(*datastorepb.Value)
	if !ok {
		return 0, fmt.Errorf("unexpected count type: %T", count)
	}
	return val.GetIntegerValue(), nil
}

// SelectWithCursor executes the query and returns results with a cursor
// for pagination. The cursor can be passed to WithCursor in subsequent
// queries to fetch the next page.
//
// This method uses an iterator internally, which may be slightly slower
// than Select for simple queries, but enables efficient deep pagination.
//
// Returns an error if the query was configured with WithOffset, as offset
// and cursor pagination are mutually exclusive.
//
// Example:
//
//	// Paginate through all active users
//	var allUsers []User
//	cursor := ""
//	for {
//	    users, nextCursor, err := dsx.Query[User](db, ctx, "User").
//	        WithFilter("Status", dsx.OpEqual, "active").
//	        WithLimit(100).
//	        WithCursor(cursor).
//	        SelectWithCursor()
//	    if err != nil {
//	        return err
//	    }
//	    allUsers = append(allUsers, users...)
//	    if len(users) < 100 {
//	        break // last page
//	    }
//	    cursor = nextCursor
//	}
func (qb *QueryBuilder[T]) SelectWithCursor() ([]T, string, error) {
	if qb.usingOffset {
		return nil, "", errors.New("query defined to use offset instead of cursor")
	}

	result := make([]T, 0, qb.limit)
	it := qb.db.client.Run(qb.context, qb.query)
	for {
		var entity T
		_, err := it.Next(&entity)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			log.Println("datastore", qb.kind, "select-error", err)
			return nil, "", err
		}
		result = append(result, entity)
	}

	cursor, err := it.Cursor()
	if err != nil {
		return nil, "", err
	}

	return result, cursor.String(), nil
}

// Select executes the query and returns all matching entities.
// This uses GetAll internally, which is slightly faster than iterator-based
// methods for simple queries.
//
// Returns an error if the query was configured with WithCursor, as cursor
// pagination requires SelectWithCursor.
//
// Example:
//
//	users, err := dsx.Query[User](db, ctx, "User").
//	    WithFilter("Status", dsx.OpEqual, "active").
//	    WithOrderDesc("CreatedAt").
//	    WithLimit(50).
//	    Select()
func (qb *QueryBuilder[T]) Select() ([]T, error) {
	if qb.usingCursor {
		return nil, errors.New("query defined to use cursor")
	}

	var result []T
	if _, err := qb.db.client.GetAll(qb.context, qb.query, &result); err != nil {
		log.Println("datastore", qb.kind, "select-error", err)
		return nil, err
	}

	return result, nil
}

// Get executes the query and returns the first matching entity.
// Useful for queries expected to return a single result.
//
// Returns nil (not an error) if no entities match the query.
// Returns an error if the query was configured with WithCursor.
//
// Tip: Use WithLimit(1) for efficiency when you only need one result.
//
// Example:
//
//	user, err := dsx.Query[User](db, ctx, "User").
//	    WithFilter("Email", dsx.OpEqual, "john@example.com").
//	    WithLimit(1).
//	    Get()
//	if err != nil {
//	    return err
//	}
//	if user == nil {
//	    // not found
//	}
func (qb *QueryBuilder[T]) Get() (*T, error) {
	if qb.usingCursor {
		return nil, errors.New("query defined to use cursor")
	}

	tmp, err := qb.Select()
	if err != nil {
		return nil, err
	}

	if len(tmp) > 0 {
		return &tmp[0], nil
	}

	return nil, nil
}

// Upsert inserts or updates a single entity with the specified string ID.
// If an entity with the ID exists, it is overwritten; otherwise, a new
// entity is created.
//
// Parameters:
//   - id: String ID for the entity key
//   - data: Pointer to the entity data
//
// Example:
//
//	user := User{Name: "John", Email: "john@example.com", Status: "active"}
//	err := dsx.Query[User](db, ctx, "User").Upsert("user-123", &user)
func (qb *QueryBuilder[T]) Upsert(id string, data *T) error {
	key := datastore.NameKey(qb.kind, id, nil)
	if _, err := qb.db.client.Put(qb.context, key, data); err != nil {
		log.Println("datastore", qb.kind, "upsert-error", err)
		return err
	}

	return nil
}

// UpsertMulti inserts or updates multiple entities in a single batch operation.
// This is more efficient than calling Upsert multiple times.
//
// Parameters:
//   - items: Map of string ID to entity pointer
//
// Note: Datastore has a limit of 500 entities per batch operation.
// For larger batches, split into multiple calls.
//
// Example:
//
//	users := map[string]*User{
//	    "user-1": {Name: "Alice", Status: "active"},
//	    "user-2": {Name: "Bob", Status: "active"},
//	}
//	err := dsx.Query[User](db, ctx, "User").UpsertMulti(users)
func (qb *QueryBuilder[T]) UpsertMulti(items map[string]*T) error {
	if len(items) == 0 {
		return nil
	}

	keys := make([]*datastore.Key, 0, len(items))
	entities := make([]*T, 0, len(items))
	for id, data := range items {
		keys = append(keys, datastore.NameKey(qb.kind, id, nil))
		entities = append(entities, data)
	}

	if _, err := qb.db.client.PutMulti(qb.context, keys, entities); err != nil {
		log.Println("datastore", qb.kind, "upsert-multi-error", err)
		return err
	}

	return nil
}

// Delete removes all entities matching the current query filters.
// Entities are deleted in batches of 500 (Datastore's limit per operation).
//
// Warning: Without filters, this will delete ALL entities of the kind.
// Use with caution.
//
// Example:
//
//	// Delete all inactive users
//	err := dsx.Query[User](db, ctx, "User").
//	    WithFilter("Status", dsx.OpEqual, "inactive").
//	    Delete()
//
//	// Delete a specific user
//	err := dsx.Query[User](db, ctx, "User").
//	    WithFilter(dsx.FieldKey, dsx.OpEqual, "user-123").
//	    Delete()
func (qb *QueryBuilder[T]) Delete() (err error) {
	keys, err := qb.db.client.GetAll(qb.context, qb.query.KeysOnly(), nil)
	if err != nil {
		log.Println("datastore", qb.kind, "delete", "get-all", "error", err)
		return err
	}
	totalKey := len(keys)
	if totalKey > 0 {
		for i := 0; i < totalKey; i += 500 {
			end := i + 500
			if end > totalKey {
				end = totalKey
			}

			batch := keys[i:end]
			if err = qb.db.client.DeleteMulti(qb.context, batch); err != nil {
				log.Println("datastore", qb.kind, "delete", "delete-multi", "error", err)
				return err
			}
		}
	}

	return nil
}
