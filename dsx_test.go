package dsx

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// propertyFilters renders every property filter in the query, flattening the
// composite AND the client builds for multi-filter queries.
func propertyFilters(query *pb.Query) []string {
	var rendered []string
	var walk func(*pb.Filter)
	walk = func(filter *pb.Filter) {
		switch {
		case filter == nil:
		case filter.GetCompositeFilter() != nil:
			for _, sub := range filter.GetCompositeFilter().GetFilters() {
				walk(sub)
			}
		case filter.GetPropertyFilter() != nil:
			property := filter.GetPropertyFilter()
			rendered = append(rendered, fmt.Sprintf("%s %s %s",
				property.GetProperty().GetName(), property.GetOp(), renderValue(property.GetValue())))
		}
	}
	walk(query.GetFilter())
	return rendered
}

func renderValue(value *pb.Value) string {
	switch typed := value.GetValueType().(type) {
	case *pb.Value_StringValue:
		return typed.StringValue
	case *pb.Value_IntegerValue:
		return strconv.FormatInt(typed.IntegerValue, 10)
	case *pb.Value_KeyValue:
		path := typed.KeyValue.GetPath()
		return fmt.Sprintf("key(ns=%q %s/%s)",
			typed.KeyValue.GetPartitionId().GetNamespaceId(), path[0].GetKind(), path[0].GetName())
	case *pb.Value_ArrayValue:
		parts := make([]string, 0, len(typed.ArrayValue.GetValues()))
		for _, element := range typed.ArrayValue.GetValues() {
			parts = append(parts, renderValue(element))
		}
		return "[" + strings.Join(parts, " ") + "]"
	default:
		return fmt.Sprintf("%v", value.GetValueType())
	}
}

func orderFields(query *pb.Query) []string {
	fields := make([]string, 0, len(query.GetOrder()))
	for _, order := range query.GetOrder() {
		prefix := ""
		if order.GetDirection() == pb.PropertyOrder_DESCENDING {
			prefix = "-"
		}
		fields = append(fields, prefix+order.GetProperty().GetName())
	}
	return fields
}

func projectedFields(query *pb.Query) []string {
	fields := make([]string, 0, len(query.GetProjection()))
	for _, projection := range query.GetProjection() {
		fields = append(fields, projection.GetProperty().GetName())
	}
	return fields
}

func assertEqualSlices[T comparable](t *testing.T, what string, got, want []T) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

func TestQueryBuildsExpectedRequest(t *testing.T) {
	db, fake := newTestDB(t)

	if _, err := Query[testUser](db, "User").
		WithFilter("Status", OpEqual, "active").
		WithFilter("Age", OpGreaterEqual, 18).
		WithFilter("Tier", OpIn, []string{"gold", "silver"}).
		WithOrder("Status").
		WithOrderDesc("CreatedAt").
		WithLimit(50).
		WithOffset(10).
		Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}

	query := fake.lastQuery(t).GetQuery()

	if kinds := query.GetKind(); len(kinds) != 1 || kinds[0].GetName() != "User" {
		t.Errorf("kind = %v, want [User]", kinds)
	}
	assertEqualSlices(t, "filters", propertyFilters(query), []string{
		"Status EQUAL active",
		"Age GREATER_THAN_OR_EQUAL 18",
		"Tier IN [gold silver]",
	})
	assertEqualSlices(t, "order", orderFields(query), []string{"Status", "-CreatedAt"})
	if got := query.GetLimit().GetValue(); got != 50 {
		t.Errorf("limit = %d, want 50", got)
	}
	if got := query.GetOffset(); got != 10 {
		t.Errorf("offset = %d, want 10", got)
	}
}

func TestQueryProjectionAndDistinct(t *testing.T) {
	db, fake := newTestDB(t)

	if _, err := Query[testUser](db, "User").
		WithProject("Name", "Status").
		WithDistinct().
		Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}

	query := fake.lastQuery(t).GetQuery()
	assertEqualSlices(t, "projection", projectedFields(query), []string{"Name", "Status"})
	if len(query.GetDistinctOn()) == 0 {
		t.Error("distinct_on is empty, want the projected fields")
	}
}

func TestSelectDecodesEntities(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return queryResponse([]*pb.EntityResult{
			userResult("", "alice", "active"),
			userResult("", "bob", "inactive"),
		}), nil
	}

	users, err := Query[testUser](db, "User").Select(context.Background())
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(users) != 2 || users[0].Name != "alice" || users[1].Status != "inactive" {
		t.Errorf("users = %+v, want alice/active and bob/inactive", users)
	}
}

func TestSelectWrapsServerError(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "nope")
	}

	_, err := Query[testUser](db, "User").Select(context.Background())
	if err == nil {
		t.Fatal("Select succeeded, want an error")
	}
	if !strings.HasPrefix(err.Error(), "dsx: select User:") {
		t.Errorf("error = %q, want a dsx: select User: prefix", err)
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("wrapped error lost its gRPC status: %v", err)
	}
}

func TestGetReturnsErrNotFoundWhenEmpty(t *testing.T) {
	db, _ := newTestDB(t)

	user, err := Query[testUser](db, "User").WithFilter("Status", OpEqual, "ghost").Get(context.Background())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if user != nil {
		t.Errorf("user = %+v, want nil", user)
	}
}

func TestGetLimitsToOneAndReturnsFirst(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return queryResponse([]*pb.EntityResult{userResult("", "alice", "active")}), nil
	}

	user, err := Query[testUser](db, "User").WithLimit(100).Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if user.Name != "alice" {
		t.Errorf("user.Name = %q, want alice", user.Name)
	}
	if got := fake.lastQuery(t).GetQuery().GetLimit().GetValue(); got != 1 {
		t.Errorf("limit = %d, want 1 (Get must not fetch more than it returns)", got)
	}
}

func TestGetDoesNotMutateBuilderLimit(t *testing.T) {
	db, fake := newTestDB(t)
	builder := Query[testUser](db, "User").WithLimit(25)

	if _, err := builder.Get(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get: %v", err)
	}
	if _, err := builder.Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got := fake.lastQuery(t).GetQuery().GetLimit().GetValue(); got != 25 {
		t.Errorf("limit after Get = %d, want the builder's 25", got)
	}
}

func TestGetByKeyReturnsErrNotFound(t *testing.T) {
	db, _ := newTestDB(t)

	user, err := GetByKey[testUser](context.Background(), db, "User", "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if user != nil {
		t.Errorf("user = %+v, want nil", user)
	}
}

func TestGetByKeyReturnsEntity(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onLookup = func(req *pb.LookupRequest) (*pb.LookupResponse, error) {
		return &pb.LookupResponse{Found: []*pb.EntityResult{userResult("", "alice", "active")}}, nil
	}

	user, err := GetByKey[testUser](context.Background(), db, "User", "alice")
	if err != nil {
		t.Fatalf("GetByKey: %v", err)
	}
	if user.Status != "active" {
		t.Errorf("user = %+v, want status active", user)
	}
}

func TestKeyFilterResolvesToKeyValue(t *testing.T) {
	db, fake := newTestDB(t)

	if _, err := Query[testUser](db, "User").
		WithFilter(FieldKey, OpEqual, "user-123").
		Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}

	assertEqualSlices(t, "filters", propertyFilters(fake.lastQuery(t).GetQuery()),
		[]string{`__key__ EQUAL key(ns="" User/user-123)`})
}

func TestKeyFilterRejectsWrongValueType(t *testing.T) {
	db, _ := newTestDB(t)

	builder := Query[testUser](db, "User").WithFilter(FieldKey, OpEqual, 42)
	if builder.Err() == nil {
		t.Fatal("Err() = nil, want a filter error recorded on the builder")
	}

	_, err := builder.Select(context.Background())
	if err == nil || !strings.Contains(err.Error(), "value must be a string or *datastore.Key") {
		t.Fatalf("Select err = %v, want the recorded filter error", err)
	}
}

func TestBuilderKeepsFirstError(t *testing.T) {
	db, _ := newTestDB(t)

	builder := Query[testUser](db, "User").
		WithFilter(FieldKey, OpEqual, 42).
		WithCursor("not-a-valid-cursor")

	if !strings.Contains(builder.Err().Error(), "value must be a string or *datastore.Key") {
		t.Errorf("Err() = %v, want the first error (the bad filter)", builder.Err())
	}
}

func TestBadCursorIsReportedByTerminalCall(t *testing.T) {
	db, _ := newTestDB(t)

	_, _, err := Query[testUser](db, "User").
		WithCursor("!!! not base64 !!!").
		SelectWithCursor(context.Background())
	if err == nil || !strings.Contains(err.Error(), "decode cursor") {
		t.Fatalf("err = %v, want a decode cursor error", err)
	}
}

func TestOffsetAndCursorConflict(t *testing.T) {
	db, _ := newTestDB(t)
	cursor := encodeCursor("page-2")

	t.Run("cursor then offset", func(t *testing.T) {
		builder := Query[testUser](db, "User").WithCursor(cursor).WithOffset(10)
		if !errors.Is(builder.Err(), ErrPaginationConflict) {
			t.Errorf("Err() = %v, want ErrPaginationConflict", builder.Err())
		}
	})

	t.Run("offset then cursor", func(t *testing.T) {
		builder := Query[testUser](db, "User").WithOffset(10).WithCursor(cursor)
		if !errors.Is(builder.Err(), ErrPaginationConflict) {
			t.Errorf("Err() = %v, want ErrPaginationConflict", builder.Err())
		}
	})

	t.Run("cursor with Select", func(t *testing.T) {
		_, err := Query[testUser](db, "User").WithCursor(cursor).Select(context.Background())
		if !errors.Is(err, ErrPaginationConflict) {
			t.Errorf("err = %v, want ErrPaginationConflict", err)
		}
	})

	t.Run("offset with SelectWithCursor", func(t *testing.T) {
		_, _, err := Query[testUser](db, "User").WithOffset(10).SelectWithCursor(context.Background())
		if !errors.Is(err, ErrPaginationConflict) {
			t.Errorf("err = %v, want ErrPaginationConflict", err)
		}
	})
}

func TestCountReadsAggregation(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onAggregation = func(*pb.RunAggregationQueryRequest) (*pb.RunAggregationQueryResponse, error) {
		return aggregationResponse(42), nil
	}

	count, err := Query[testUser](db, "User").WithFilter("Status", OpEqual, "active").Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 42 {
		t.Errorf("count = %d, want 42", count)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.aggregations) != 1 {
		t.Fatalf("aggregation requests = %d, want 1", len(fake.aggregations))
	}
	nested := fake.aggregations[0].GetAggregationQuery().GetNestedQuery()
	assertEqualSlices(t, "filters", propertyFilters(nested), []string{"Status EQUAL active"})
}

func TestCountReportsMissingAggregationResult(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onAggregation = func(*pb.RunAggregationQueryRequest) (*pb.RunAggregationQueryResponse, error) {
		return &pb.RunAggregationQueryResponse{Batch: &pb.AggregationResultBatch{
			AggregationResults: []*pb.AggregationResult{{AggregateProperties: map[string]*pb.Value{}}},
		}}, nil
	}

	if _, err := Query[testUser](db, "User").Count(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "aggregation result missing") {
		t.Fatalf("err = %v, want an aggregation result missing error", err)
	}
}
