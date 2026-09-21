package dsx

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// mutatedNamespaces returns the namespace of every mutated key, in order.
func (f *fakeDatastore) mutatedNamespaces() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var namespaces []string
	for _, commit := range f.commits {
		for _, mutation := range commit.GetMutations() {
			switch {
			case mutation.GetUpsert() != nil:
				namespaces = append(namespaces, mutation.GetUpsert().GetKey().GetPartitionId().GetNamespaceId())
			case mutation.GetInsert() != nil:
				namespaces = append(namespaces, mutation.GetInsert().GetKey().GetPartitionId().GetNamespaceId())
			case mutation.GetDelete() != nil:
				namespaces = append(namespaces, mutation.GetDelete().GetPartitionId().GetNamespaceId())
			}
		}
	}
	return namespaces
}

// lookupNamespaces returns the namespace of every looked-up key, in order.
func (f *fakeDatastore) lookupNamespaces() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var namespaces []string
	for _, lookup := range f.lookups {
		for _, key := range lookup.GetKeys() {
			namespaces = append(namespaces, key.GetPartitionId().GetNamespaceId())
		}
	}
	return namespaces
}

func TestConnectWithoutNamespaceUsesDefault(t *testing.T) {
	db, fake := newTestDB(t)

	if db.Namespace() != "" {
		t.Errorf("Namespace() = %q, want empty", db.Namespace())
	}
	if _, err := Query[testUser](db, "User").Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got := fake.lastQuery(t).GetPartitionId().GetNamespaceId(); got != "" {
		t.Errorf("namespace = %q, want empty", got)
	}
}

func TestConnectNamespaceAppliesToQueriesAndKeys(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("tenant-42"))
	ctx := context.Background()

	if db.Namespace() != "tenant-42" {
		t.Errorf("Namespace() = %q, want tenant-42", db.Namespace())
	}

	if _, err := Query[testUser](db, "User").Select(ctx); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got := fake.lastQuery(t).GetPartitionId().GetNamespaceId(); got != "tenant-42" {
		t.Errorf("query namespace = %q, want tenant-42", got)
	}

	if err := Query[testUser](db, "User").Upsert(ctx, "alice", &testUser{Name: "alice"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	assertEqualSlices(t, "written key namespaces", fake.mutatedNamespaces(), []string{"tenant-42"})

	if _, err := GetByKey[testUser](ctx, db, "User", "alice"); err == nil {
		t.Fatal("GetByKey succeeded against an empty fake, want ErrNotFound")
	}
	assertEqualSlices(t, "looked-up key namespaces", fake.lookupNamespaces(), []string{"tenant-42"})
}

func TestConnectNamespaceAppliesToPackageHelpers(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("tenant-7"))
	ctx := context.Background()

	if _, err := GetMulti[testUser](ctx, db, "User", []string{"a", "b"}); err != nil {
		t.Fatalf("GetMulti: %v", err)
	}
	assertEqualSlices(t, "get-multi namespaces", fake.lookupNamespaces(), []string{"tenant-7", "tenant-7"})

	if err := DeleteMultiByKey(ctx, db, "User", []string{"a", "b"}); err != nil {
		t.Fatalf("DeleteMultiByKey: %v", err)
	}
	if err := DeleteByKey(ctx, db, "User", "c"); err != nil {
		t.Fatalf("DeleteByKey: %v", err)
	}
	assertEqualSlices(t, "delete namespaces", fake.mutatedNamespaces(),
		[]string{"tenant-7", "tenant-7", "tenant-7"})
}

func TestDBWithNamespaceIsADerivedCopy(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("base"))
	tenant := db.WithNamespace("tenant-9")

	if db.Namespace() != "base" {
		t.Errorf("original Namespace() = %q, want base (WithNamespace must not mutate)", db.Namespace())
	}
	if tenant.Namespace() != "tenant-9" {
		t.Errorf("derived Namespace() = %q, want tenant-9", tenant.Namespace())
	}
	if tenant.Client() != db.Client() {
		t.Error("derived DB does not share the underlying client")
	}
	if tenant.ProjectID() != db.ProjectID() || tenant.DatabaseID() != db.DatabaseID() {
		t.Error("derived DB lost its project or database")
	}

	if _, err := Query[testUser](tenant, "User").Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got := fake.lastQuery(t).GetPartitionId().GetNamespaceId(); got != "tenant-9" {
		t.Errorf("query namespace = %q, want tenant-9", got)
	}
}

func TestQueryWithNamespaceOverridesConnectionDefault(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("base"))
	ctx := context.Background()

	builder := Query[testUser](db, "User").WithNamespace("override")
	if builder.Namespace() != "override" {
		t.Errorf("Namespace() = %q, want override", builder.Namespace())
	}
	if _, err := builder.Select(ctx); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got := fake.lastQuery(t).GetPartitionId().GetNamespaceId(); got != "override" {
		t.Errorf("query namespace = %q, want override", got)
	}

	// An empty namespace selects the default namespace, overriding the connection.
	if _, err := Query[testUser](db, "User").WithNamespace("").Select(ctx); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got := fake.lastQuery(t).GetPartitionId().GetNamespaceId(); got != "" {
		t.Errorf("query namespace = %q, want the default namespace", got)
	}
}

func TestWithNamespaceAppliesToWritesThroughTheBuilder(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("base"))

	if err := Query[testUser](db, "User").
		WithNamespace("override").
		Upsert(context.Background(), "alice", &testUser{Name: "alice"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	assertEqualSlices(t, "written key namespaces", fake.mutatedNamespaces(), []string{"override"})
}

// WithNamespace may appear anywhere in the chain, so a key filter added before
// it must still resolve in the namespace the query ends up running in.
func TestWithNamespaceAfterKeyFilterStillApplies(t *testing.T) {
	db, fake := newTestDB(t)

	if _, err := Query[testUser](db, "User").
		WithFilter(FieldKey, OpEqual, "user-123").
		WithNamespace("tenant-3").
		Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}

	assertEqualSlices(t, "filters", propertyFilters(fake.lastQuery(t).GetQuery()),
		[]string{`__key__ EQUAL key(ns="tenant-3" User/user-123)`})
}

func TestKeyFilterAcceptsCallerSuppliedKeyUnchanged(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("connection-ns"))

	explicit := datastore.NameKey("User", "user-9", nil)
	explicit.Namespace = "explicit-ns"

	if _, err := Query[testUser](db, "User").
		WithFilter(FieldKey, OpEqual, explicit).
		Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}

	assertEqualSlices(t, "filters", propertyFilters(fake.lastQuery(t).GetQuery()),
		[]string{`__key__ EQUAL key(ns="explicit-ns" User/user-9)`})
}

func TestKeyFilterAcceptsSliceForMembership(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("tenant-1"))

	if _, err := Query[testUser](db, "User").
		WithFilter(FieldKey, OpIn, []string{"user-1", "user-2"}).
		Select(context.Background()); err != nil {
		t.Fatalf("Select: %v", err)
	}

	assertEqualSlices(t, "filters", propertyFilters(fake.lastQuery(t).GetQuery()),
		[]string{`__key__ IN [key(ns="tenant-1" User/user-1) key(ns="tenant-1" User/user-2)]`})
}

func TestMembershipFilterRejectsNonSlice(t *testing.T) {
	db, _ := newTestDB(t)

	builder := Query[testUser](db, "User").WithFilter("Status", OpIn, "active")
	if builder.Err() == nil {
		t.Fatal("Err() = nil, want an error for a non-slice membership value")
	}
}

func TestInsertWithAutoKeyUsesNamespace(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("tenant-5"))

	key, err := Query[testUser](db, "Order").InsertWithAutoKey(context.Background(), &testUser{Name: "o1"})
	if err != nil {
		t.Fatalf("InsertWithAutoKey: %v", err)
	}
	if key.Incomplete() {
		t.Error("returned key is still incomplete")
	}
	assertEqualSlices(t, "written key namespaces", fake.mutatedNamespaces(), []string{"tenant-5"})
}

func TestDeleteUsesNamespaceForQueryAndKeys(t *testing.T) {
	db, fake := newTestDB(t, WithNamespace("tenant-8"))

	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return keysOnlyResponse([]*pb.EntityResult{
			keyResult("tenant-8", "User", "a"),
			keyResult("tenant-8", "User", "b"),
		}), nil
	}

	if err := Query[testUser](db, "User").Delete(context.Background()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := fake.lastQuery(t).GetPartitionId().GetNamespaceId(); got != "tenant-8" {
		t.Errorf("query namespace = %q, want tenant-8", got)
	}
	assertEqualSlices(t, "deleted key namespaces", fake.mutatedNamespaces(), []string{"tenant-8", "tenant-8"})
}
