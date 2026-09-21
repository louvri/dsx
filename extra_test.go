package dsx

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/datastore"
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSelectWithCursorReturnsEntitiesAndCursor(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()

	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		first := userResult("", "alice", "active")
		first.Cursor = []byte("after-alice")
		second := userResult("", "bob", "active")
		second.Cursor = []byte("after-bob")
		return queryResponse([]*pb.EntityResult{first, second}), nil
	}

	users, cursor, err := Query[testUser](db, "User").WithLimit(2).SelectWithCursor(ctx)
	if err != nil {
		t.Fatalf("SelectWithCursor: %v", err)
	}
	if len(users) != 2 || users[0].Name != "alice" || users[1].Name != "bob" {
		t.Fatalf("users = %+v, want alice and bob", users)
	}
	if cursor == "" {
		t.Fatal("cursor is empty, want a resumable cursor")
	}

	// The cursor must be usable as the start of the next page.
	if _, _, err := Query[testUser](db, "User").WithLimit(2).WithCursor(cursor).SelectWithCursor(ctx); err != nil {
		t.Fatalf("second page: %v", err)
	}
	if got := fake.lastQuery(t).GetQuery().GetStartCursor(); len(got) == 0 {
		t.Error("second page sent no start cursor")
	}
}

func TestSelectWithCursorWrapsServerError(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "nope")
	}

	_, _, err := Query[testUser](db, "User").SelectWithCursor(context.Background())
	if err == nil || !strings.HasPrefix(err.Error(), "dsx: select-with-cursor User:") {
		t.Fatalf("err = %v, want a dsx: select-with-cursor User: prefix", err)
	}
}

func TestRunInTransactionCommits(t *testing.T) {
	db, fake := newTestDB(t)

	err := RunInTransaction(context.Background(), db, func(tx *datastore.Transaction) error {
		key := datastore.NameKey("User", "alice", nil)
		key.Namespace = db.Namespace()
		_, err := tx.Put(key, &testUser{Name: "alice"})
		return err
	})
	if err != nil {
		t.Fatalf("RunInTransaction: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.begins) != 1 {
		t.Errorf("begin transactions = %d, want 1", len(fake.begins))
	}
	if len(fake.commits) != 1 {
		t.Errorf("commits = %d, want 1", len(fake.commits))
	}
	if len(fake.rollbacks) != 0 {
		t.Errorf("rollbacks = %d, want 0", len(fake.rollbacks))
	}
}

func TestRunInTransactionRollsBackAndWrapsError(t *testing.T) {
	db, fake := newTestDB(t)
	sentinel := errors.New("business rule violated")

	err := RunInTransaction(context.Background(), db, func(*datastore.Transaction) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want it to wrap the callback error", err)
	}
	if !strings.HasPrefix(err.Error(), "dsx: transaction:") {
		t.Errorf("error = %q, want a dsx: transaction: prefix", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.commits) != 0 {
		t.Errorf("commits = %d, want 0 when the callback fails", len(fake.commits))
	}
	if len(fake.rollbacks) != 1 {
		t.Errorf("rollbacks = %d, want 1", len(fake.rollbacks))
	}
}

func TestWriteErrorsAreWrapped(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()
	fake.onCommit = func(*pb.CommitRequest) (*pb.CommitResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "nope")
	}

	tests := map[string]struct {
		run    func() error
		prefix string
	}{
		"upsert": {
			run:    func() error { return Query[testUser](db, "User").Upsert(ctx, "alice", &testUser{}) },
			prefix: "dsx: upsert User/alice:",
		},
		"insert-with-auto-key": {
			run: func() error {
				_, err := Query[testUser](db, "User").InsertWithAutoKey(ctx, &testUser{})
				return err
			},
			prefix: "dsx: insert-with-auto-key User:",
		},
		"upsert-multi": {
			run: func() error {
				return Query[testUser](db, "User").UpsertMulti(ctx, map[string]*testUser{"a": {}})
			},
			prefix: "dsx: upsert-multi User",
		},
		"insert-multi-with-auto-key": {
			run: func() error {
				_, err := Query[testUser](db, "User").InsertMultiWithAutoKey(ctx, []*testUser{{}})
				return err
			},
			prefix: "dsx: insert-multi-with-auto-key User",
		},
		"delete-by-key": {
			run:    func() error { return DeleteByKey(ctx, db, "User", "alice") },
			prefix: "dsx: delete-by-key User/alice:",
		},
		"delete-multi-by-key": {
			run:    func() error { return DeleteMultiByKey(ctx, db, "User", []string{"alice"}) },
			prefix: "dsx: delete-multi-by-key User",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.run()
			if err == nil {
				t.Fatal("succeeded, want an error")
			}
			if !strings.HasPrefix(err.Error(), test.prefix) {
				t.Errorf("error = %q, want prefix %q", err, test.prefix)
			}
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("wrapped error lost its gRPC status: %v", err)
			}
		})
	}
}

func TestDeleteWrapsQueryError(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "nope")
	}

	err := Query[testUser](db, "User").Delete(context.Background())
	if err == nil || !strings.HasPrefix(err.Error(), "dsx: delete User: scan keys:") {
		t.Fatalf("err = %v, want a dsx: delete User: scan keys: prefix", err)
	}
}

func TestBuilderAccessors(t *testing.T) {
	db, fake := newTestDB(t)

	ancestor := datastore.NameKey("Company", "acme", nil)
	builder := Query[testUser](db, "Employee").
		WithAncestorKey(ancestor).
		WithAncestorKey(nil) // ignored

	if builder.DB() != db {
		t.Error("DB() did not return the connection the query was built from")
	}
	if builder.Kind() != "Employee" {
		t.Errorf("Kind() = %q, want Employee", builder.Kind())
	}
	if builder.Err() != nil {
		t.Errorf("Err() = %v, want nil", builder.Err())
	}

	if _, err := builder.Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}
	query := fake.lastQuery(t).GetQuery()
	if got := propertyFilters(query); len(got) != 1 || !strings.Contains(got[0], "Company/acme") {
		t.Errorf("filters = %v, want an ancestor filter on Company/acme", got)
	}
}

func TestBuilderErrorShortCircuitsEveryTerminal(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()

	// A bad key filter value is recorded once and must stop every terminal call.
	newBuilder := func() *QueryBuilder[testUser] {
		return Query[testUser](db, "User").WithFilter(FieldKey, OpEqual, 42)
	}

	if _, err := newBuilder().Select(ctx); err == nil {
		t.Error("Select succeeded, want the builder error")
	}
	if _, err := newBuilder().Get(ctx); err == nil {
		t.Error("Get succeeded, want the builder error")
	}
	if _, _, err := newBuilder().SelectWithCursor(ctx); err == nil {
		t.Error("SelectWithCursor succeeded, want the builder error")
	}
	if _, err := newBuilder().Count(ctx); err == nil {
		t.Error("Count succeeded, want the builder error")
	}
	if err := newBuilder().Delete(ctx); err == nil {
		t.Error("Delete succeeded, want the builder error")
	}
	if err := newBuilder().Upsert(ctx, "a", &testUser{}); err == nil {
		t.Error("Upsert succeeded, want the builder error")
	}
	if err := newBuilder().UpsertMulti(ctx, map[string]*testUser{"a": {}}); err == nil {
		t.Error("UpsertMulti succeeded, want the builder error")
	}
	if _, err := newBuilder().InsertWithAutoKey(ctx, &testUser{}); err == nil {
		t.Error("InsertWithAutoKey succeeded, want the builder error")
	}
	if _, err := newBuilder().InsertMultiWithAutoKey(ctx, []*testUser{{}}); err == nil {
		t.Error("InsertMultiWithAutoKey succeeded, want the builder error")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.queries)+len(fake.commits)+len(fake.aggregations) != 0 {
		t.Error("a builder error still let a request reach the server")
	}
}

func TestWithCredentialsJSONIsRejectedWhenInvalid(t *testing.T) {
	_, err := Connect(context.Background(), testProject, testDatabase, WithCredentialsJSON("{not json"))
	if err == nil {
		t.Fatal("Connect succeeded with malformed credentials, want an error")
	}
	if !strings.HasPrefix(err.Error(), "dsx: connect project=") {
		t.Errorf("error = %q, want a dsx: connect prefix", err)
	}
}

func TestZeroLimitAndOffsetAreIgnored(t *testing.T) {
	db, fake := newTestDB(t)

	if _, err := Query[testUser](db, "User").WithLimit(0).WithOffset(-1).Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}
	query := fake.lastQuery(t).GetQuery()
	if query.GetLimit() != nil {
		t.Errorf("limit = %v, want unset", query.GetLimit())
	}
	if query.GetOffset() != 0 {
		t.Errorf("offset = %d, want 0", query.GetOffset())
	}
}
