package dsx

import (
	"context"
	"strings"
	"testing"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// An empty kind is a valid kindless query in Datastore, matching every entity
// of every kind. Select and Count would quietly return rows from outside T, and
// Delete would remove the entire namespace through a Query[T] that names one
// type. This is the one remaining path where an empty string produced a valid
// but catastrophically different operation.
func TestEmptyKindIsRejected(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()

	// The fake would return entities of another kind if the query ever ran.
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return keysOnlyResponse([]*pb.EntityResult{
			keyResult("", "User", "u1"),
			keyResult("", "Order", "o1"),
		}), nil
	}

	builder := func() *QueryBuilder[testUser] { return Query[testUser](db, "") }

	if err := builder().Err(); err == nil || !strings.Contains(err.Error(), "kind must not be empty") {
		t.Fatalf("Err() = %v, want a kind must not be empty error", builder().Err())
	}

	if err := builder().Delete(ctx); err == nil {
		t.Error("Delete succeeded with an empty kind; it would have emptied the namespace")
	}
	if _, err := builder().Select(ctx); err == nil {
		t.Error("Select succeeded with an empty kind")
	}
	if _, err := builder().SelectKeys(ctx); err == nil {
		t.Error("SelectKeys succeeded with an empty kind")
	}
	if _, err := builder().Count(ctx); err == nil {
		t.Error("Count succeeded with an empty kind")
	}
	if _, err := builder().Get(ctx); err == nil {
		t.Error("Get succeeded with an empty kind")
	}
	if err := builder().Upsert(ctx, "k", &testUser{}); err == nil {
		t.Error("Upsert succeeded with an empty kind")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.queries)+len(fake.commits)+len(fake.aggregations) != 0 {
		t.Errorf("an empty kind still reached the server: %d queries, %d commits, %d aggregations",
			len(fake.queries), len(fake.commits), len(fake.aggregations))
	}
}

// Datastore rejects an empty membership list, and a filter that can match
// nothing is more often an unpopulated slice than a deliberate query.
func TestEmptyMembershipSliceIsRejected(t *testing.T) {
	db, fake := newTestDB(t)

	tests := map[string]struct {
		field string
		value any
	}{
		"typed slice":  {"Status", []string{}},
		"any slice":    {"Status", []any{}},
		"nil slice":    {"Status", []string(nil)},
		"on key field": {FieldKey, []string{}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			builder := Query[testUser](db, "User").WithFilter(test.field, OpIn, test.value)
			if builder.Err() == nil || !strings.Contains(builder.Err().Error(), "must not be an empty slice") {
				t.Fatalf("Err() = %v, want an empty slice error", builder.Err())
			}
			if _, err := builder.Select(context.Background()); err == nil {
				t.Error("Select succeeded with an empty membership filter")
			}
		})
	}

	// A populated slice still works.
	if err := Query[testUser](db, "User").WithFilter("Status", OpIn, []string{"active"}).Err(); err != nil {
		t.Errorf("Err() = %v, want nil for a populated slice", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.queries) != 0 {
		t.Error("an empty membership filter still reached the server")
	}
}

// Count does not surface the Datastore client's internal query error, so an
// empty order or projected field was dropped there while Select failed.
func TestEmptyOrderAndProjectionFailClosed(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()
	fake.onAggregation = func(*pb.RunAggregationQueryRequest) (*pb.RunAggregationQueryResponse, error) {
		return aggregationResponse(7), nil
	}

	tests := map[string]func() *QueryBuilder[testUser]{
		"order":         func() *QueryBuilder[testUser] { return Query[testUser](db, "User").WithOrder("") },
		"order desc":    func() *QueryBuilder[testUser] { return Query[testUser](db, "User").WithOrderDesc("") },
		"projection":    func() *QueryBuilder[testUser] { return Query[testUser](db, "User").WithProject("Name", "") },
		"no projection": func() *QueryBuilder[testUser] { return Query[testUser](db, "User").WithProject() },
	}

	for name, builder := range tests {
		t.Run(name, func(t *testing.T) {
			if builder().Err() == nil {
				t.Fatal("Err() = nil, want an error")
			}
			if _, err := builder().Count(ctx); err == nil {
				t.Error("Count succeeded; the bad clause would have been dropped silently")
			}
			if _, err := builder().Select(ctx); err == nil {
				t.Error("Select succeeded")
			}
		})
	}

	// Valid clauses still work.
	valid := Query[testUser](db, "User").WithOrder("Name").WithOrderDesc("CreatedAt").WithProject("Name")
	if err := valid.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.aggregations) != 0 {
		t.Error("an invalid clause still reached the aggregation endpoint")
	}
}
