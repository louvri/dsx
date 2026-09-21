package dsx

import (
	"context"
	"math"
	"strings"
	"testing"

	"cloud.google.com/go/datastore"
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Every operator constant must be one the Datastore client recognises. OpNotIn
// was spelled "not in" where the client expects "not-in", so it always failed.
func TestEveryOperatorReachesTheServer(t *testing.T) {
	tests := []struct {
		operator FilterOperator
		value    any
		want     string
	}{
		{OpEqual, "a", "Status EQUAL a"},
		{OpNotEqual, "a", "Status NOT_EQUAL a"},
		{OpGreater, "a", "Status GREATER_THAN a"},
		{OpGreaterEqual, "a", "Status GREATER_THAN_OR_EQUAL a"},
		{OpLess, "a", "Status LESS_THAN a"},
		{OpLessEqual, "a", "Status LESS_THAN_OR_EQUAL a"},
		{OpIn, []string{"a", "b"}, "Status IN [a b]"},
		{OpNotIn, []string{"a", "b"}, "Status NOT_IN [a b]"},
	}

	for _, test := range tests {
		t.Run(string(test.operator), func(t *testing.T) {
			db, fake := newTestDB(t)

			if _, err := Query[testUser](db, "User").
				WithFilter("Status", test.operator, test.value).
				Select(context.Background()); err != nil {
				t.Fatalf("Select: %v", err)
			}
			assertEqualSlices(t, "filters", propertyFilters(fake.lastQuery(t).GetQuery()), []string{test.want})
		})
	}
}

// An unknown operator must stop the query rather than be silently dropped.
// Count is the dangerous one: the Datastore client does not surface the query
// error for aggregations, so an unvalidated bad filter would be omitted and the
// count taken over more rows than the caller asked for.
func TestUnknownOperatorFailsClosed(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()

	builder := func() *QueryBuilder[testUser] {
		return Query[testUser](db, "User").WithFilter("Status", FilterOperator("bogus"), "x")
	}

	if err := builder().Err(); err == nil || !strings.Contains(err.Error(), "unknown filter operator") {
		t.Fatalf("Err() = %v, want an unknown filter operator error", err)
	}
	if _, err := builder().Count(ctx); err == nil {
		t.Error("Count succeeded with an unknown operator; it must not count over unfiltered rows")
	}
	if _, err := builder().Select(ctx); err == nil {
		t.Error("Select succeeded with an unknown operator")
	}
	if err := builder().Delete(ctx); err == nil {
		t.Error("Delete succeeded with an unknown operator")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.queries)+len(fake.aggregations)+len(fake.commits) != 0 {
		t.Error("an unknown operator still let a request reach the server")
	}
}

func TestSelectKeysReturnsKeys(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("tenant-2"))
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return keysOnlyResponse([]*pb.EntityResult{
			keyResult("tenant-2", "User", "alice"),
			keyResult("tenant-2", "User", "bob"),
		}), nil
	}

	keys, err := Query[testUser](db, "User").
		WithFilter("Status", OpEqual, "inactive").
		SelectKeys(context.Background())
	if err != nil {
		t.Fatalf("SelectKeys: %v", err)
	}
	if len(keys) != 2 || keys[0].Name != "alice" || keys[1].Name != "bob" {
		t.Fatalf("keys = %v, want alice and bob", keys)
	}
	if keys[0].Namespace != "tenant-2" {
		t.Errorf("key namespace = %q, want tenant-2", keys[0].Namespace)
	}

	query := fake.lastQuery(t).GetQuery()
	assertEqualSlices(t, "projection", projectedFields(query), []string{"__key__"})
	assertEqualSlices(t, "filters", propertyFilters(query), []string{"Status EQUAL inactive"})
}

func TestSelectKeysWrapsServerError(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "nope")
	}

	if _, err := Query[testUser](db, "User").SelectKeys(context.Background()); err == nil ||
		!strings.HasPrefix(err.Error(), "dsx: select-keys User:") {
		t.Fatalf("err = %v, want a dsx: select-keys User: prefix", err)
	}
}

// A typed-nil key and an empty key name both used to build an incomplete key
// and silently match nothing, instead of reporting the mistake.
func TestKeyFilterRejectsNilKeyAndEmptyName(t *testing.T) {
	db, _ := newTestDB(t)

	tests := map[string]struct {
		value any
		want  string
	}{
		"typed nil key":    {value: (*datastore.Key)(nil), want: "key must not be nil"},
		"empty key name":   {value: "", want: "key name must not be empty"},
		"nil inside slice": {value: []any{"ok", (*datastore.Key)(nil)}, want: "key must not be nil"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			operator := OpEqual
			if _, isSlice := test.value.([]any); isSlice {
				operator = OpIn
			}
			builder := Query[testUser](db, "User").WithFilter(FieldKey, operator, test.value)
			if builder.Err() == nil || !strings.Contains(builder.Err().Error(), test.want) {
				t.Fatalf("Err() = %v, want %q", builder.Err(), test.want)
			}
		})
	}
}

// Entities in batches that already committed exist and have server-allocated
// IDs, so their keys must survive a later batch failing.
func TestInsertMultiWithAutoKeyReturnsKeysOfCommittedBatches(t *testing.T) {
	db, fake := newTestDB(t)

	calls := 0
	fake.onCommit = func(req *pb.CommitRequest) (*pb.CommitResponse, error) {
		calls++
		if calls == 3 {
			return nil, status.Error(codes.PermissionDenied, "nope")
		}
		return commitResponse(req), nil
	}

	entities := make([]*testUser, 1200)
	for i := range entities {
		entities[i] = &testUser{Name: "u"}
	}

	keys, err := Query[testUser](db, "Order").InsertMultiWithAutoKey(context.Background(), entities)
	if err == nil {
		t.Fatal("InsertMultiWithAutoKey succeeded, want an error")
	}
	if len(keys) != 1000 {
		t.Fatalf("returned %d keys, want the 1000 from the two committed batches", len(keys))
	}
	for i, key := range keys {
		if key.Incomplete() {
			t.Fatalf("key %d is incomplete", i)
		}
	}
}

// Sorted batching makes the boundaries, and the keys an error names, stable.
func TestUpsertMultiBatchesDeterministicallyAndNamesKeys(t *testing.T) {
	db, fake := newTestDB(t)

	items := make(map[string]*testUser, 1200)
	for _, name := range keyNames(1200) {
		items[name] = &testUser{Name: name}
	}

	if err := Query[testUser](db, "User").UpsertMulti(context.Background(), items); err != nil {
		t.Fatalf("UpsertMulti: %v", err)
	}
	// Sorted order, so the written keys come out in the same order every run.
	assertEqualSlices(t, "written keys", fake.mutatedKeyNames(), keyNames(1200))

	// And a failing batch names the keys it covered, which a map index cannot.
	db2, fake2 := newTestDB(t)
	calls := 0
	fake2.onCommit = func(req *pb.CommitRequest) (*pb.CommitResponse, error) {
		calls++
		if calls == 2 {
			return nil, status.Error(codes.PermissionDenied, "nope")
		}
		return commitResponse(req), nil
	}

	err := Query[testUser](db2, "User").UpsertMulti(context.Background(), items)
	if err == nil {
		t.Fatal("UpsertMulti succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "[key-0500..key-0999]") {
		t.Errorf("error = %q, want it to name the failing key range", err)
	}
}

// An empty key name builds an incomplete key, which Datastore commits as an
// auto-ID insert: a brand-new duplicate entity on every call, unreachable by
// name. Upsert and UpsertMulti were the only entry points that accepted it.
func TestWritesRejectEmptyKeyName(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()

	if err := Query[testUser](db, "User").Upsert(ctx, "", &testUser{}); err == nil ||
		!strings.Contains(err.Error(), "key name must not be empty") {
		t.Errorf("Upsert err = %v, want a key name must not be empty error", err)
	}

	items := map[string]*testUser{"": {}, "ok": {}}
	if err := Query[testUser](db, "User").UpsertMulti(ctx, items); err == nil ||
		!strings.Contains(err.Error(), "key name must not be empty") {
		t.Errorf("UpsertMulti err = %v, want a key name must not be empty error", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.commits) != 0 {
		t.Errorf("made %d commits, want none: an empty key name must not write", len(fake.commits))
	}
}

// Datastore's client records an internal error for an out-of-range limit or
// offset and leaves the bound unset. Select surfaces that error but Count does
// not, so an unchecked bound made Count report more rows than the caller asked
// for. The builder now rejects it before either can run.
func TestOutOfRangeLimitAndOffsetFailClosed(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()

	if math.MaxInt <= math.MaxInt32 {
		t.Skip("int is 32 bits on this platform, so a value above MaxInt32 cannot be expressed")
	}
	// Held in a variable so the expression is not constant-folded: a constant
	// above MaxInt32 does not compile where int is 32 bits, conversion or not.
	bound := int64(math.MaxInt32)
	over := int(bound + 1)

	tests := map[string]*QueryBuilder[testUser]{
		"limit":  Query[testUser](db, "User").WithLimit(over),
		"offset": Query[testUser](db, "User").WithOffset(over),
	}

	for name, builder := range tests {
		t.Run(name, func(t *testing.T) {
			if err := builder.Err(); err == nil || !strings.Contains(err.Error(), "exceeds the maximum") {
				t.Fatalf("Err() = %v, want an exceeds the maximum error", err)
			}
			if _, err := builder.Count(ctx); err == nil {
				t.Error("Count succeeded; it must not count over a silently dropped bound")
			}
			if _, err := builder.Select(ctx); err == nil {
				t.Error("Select succeeded")
			}
		})
	}

	// In-range values at the boundary must still work.
	if err := Query[testUser](db, "User").WithLimit(math.MaxInt32).Err(); err != nil {
		t.Errorf("WithLimit(MaxInt32) = %v, want nil", err)
	}
	if err := Query[testUser](db, "User").WithOffset(math.MaxInt32).Err(); err != nil {
		t.Errorf("WithOffset(MaxInt32) = %v, want nil", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.queries)+len(fake.aggregations) != 0 {
		t.Error("an out-of-range bound still let a request reach the server")
	}
}

// A caller-built key with no name and no ID encodes as a key value with an
// empty path element and matches nothing, the same mistake the empty string
// spelling already reported.
func TestKeyFilterRejectsIncompleteKey(t *testing.T) {
	db, _ := newTestDB(t)
	incomplete := datastore.IncompleteKey("User", nil)

	tests := map[string]struct {
		operator FilterOperator
		value    any
	}{
		"single":         {OpEqual, incomplete},
		"inside a slice": {OpIn, []any{"good", incomplete}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			builder := Query[testUser](db, "User").WithFilter(FieldKey, test.operator, test.value)
			if builder.Err() == nil || !strings.Contains(builder.Err().Error(), "key must not be incomplete") {
				t.Fatalf("Err() = %v, want a key must not be incomplete error", builder.Err())
			}
		})
	}
}

// datastore.Key.Incomplete only inspects the leaf, so a key whose *parent* is
// incomplete used to pass the guard and reach the wire, matching nothing.
func TestIncompleteAncestorIsRejected(t *testing.T) {
	db, fake := newTestDB(t)
	childOfIncomplete := datastore.NameKey("User", "u1", datastore.IncompleteKey("Org", nil))

	t.Run("as a key filter value", func(t *testing.T) {
		builder := Query[testUser](db, "User").WithFilter(FieldKey, OpEqual, childOfIncomplete)
		if builder.Err() == nil || !strings.Contains(builder.Err().Error(), "key must not be incomplete") {
			t.Fatalf("Err() = %v, want a key must not be incomplete error", builder.Err())
		}
	})

	t.Run("as an ancestor", func(t *testing.T) {
		builder := Query[testUser](db, "Employee").WithAncestorKey(datastore.IncompleteKey("Company", nil))
		if builder.Err() == nil || !strings.Contains(builder.Err().Error(), "ancestor key must not be incomplete") {
			t.Fatalf("Err() = %v, want an ancestor key must not be incomplete error", builder.Err())
		}
	})

	t.Run("a complete chain is still accepted", func(t *testing.T) {
		parent := datastore.NameKey("Org", "acme", nil)
		builder := Query[testUser](db, "User").
			WithAncestorKey(parent).
			WithFilter(FieldKey, OpEqual, datastore.NameKey("User", "u1", parent))
		if err := builder.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
		if _, err := builder.Select(context.Background()); err != nil {
			t.Fatalf("Select: %v", err)
		}
	})

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.queries) != 1 {
		t.Errorf("queries reaching the server = %d, want only the valid one", len(fake.queries))
	}
}
