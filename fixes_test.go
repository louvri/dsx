package dsx

import (
	"context"
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
