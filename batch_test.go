package dsx

import (
	"context"
	"fmt"
	"strings"
	"testing"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// keyNames generates n predictable key names.
func keyNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("key-%04d", i)
	}
	return names
}

func TestUpsertMultiChunksAtCommitLimit(t *testing.T) {
	db, fake := newTestDB(t)

	items := make(map[string]*testUser, 1200)
	for _, name := range keyNames(1200) {
		items[name] = &testUser{Name: name}
	}

	if err := Query[testUser](db, "User").UpsertMulti(context.Background(), items); err != nil {
		t.Fatalf("UpsertMulti: %v", err)
	}
	assertEqualSlices(t, "commit sizes", fake.commitSizes(), []int{500, 500, 200})
}

func TestInsertMultiWithAutoKeyChunksAndKeepsOrder(t *testing.T) {
	db, fake := newTestDB(t)

	entities := make([]*testUser, 1200)
	for i := range entities {
		entities[i] = &testUser{Name: fmt.Sprintf("user-%04d", i)}
	}

	keys, err := Query[testUser](db, "Order").InsertMultiWithAutoKey(context.Background(), entities)
	if err != nil {
		t.Fatalf("InsertMultiWithAutoKey: %v", err)
	}
	assertEqualSlices(t, "commit sizes", fake.commitSizes(), []int{500, 500, 200})
	if len(keys) != len(entities) {
		t.Fatalf("returned %d keys, want %d", len(keys), len(entities))
	}
	for i, key := range keys {
		if key.Incomplete() {
			t.Fatalf("key %d is still incomplete", i)
		}
		if key.Kind != "Order" {
			t.Fatalf("key %d kind = %q, want Order", i, key.Kind)
		}
	}
}

func TestDeleteStreamsKeysAndChunks(t *testing.T) {
	db, fake := newTestDB(t)

	results := make([]*pb.EntityResult, 1200)
	for i, name := range keyNames(1200) {
		results[i] = keyResult("", "User", name)
	}
	fake.onQuery = func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
		return keysOnlyResponse(results), nil
	}

	if err := Query[testUser](db, "User").
		WithFilter("Status", OpEqual, "inactive").
		Delete(context.Background()); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	assertEqualSlices(t, "commit sizes", fake.commitSizes(), []int{500, 500, 200})
	assertEqualSlices(t, "deleted keys", fake.mutatedKeyNames(), keyNames(1200))

	query := fake.lastQuery(t).GetQuery()
	assertEqualSlices(t, "projection", projectedFields(query), []string{"__key__"})
	assertEqualSlices(t, "filters", propertyFilters(query), []string{"Status EQUAL inactive"})
}

func TestDeleteMultiByKeyChunks(t *testing.T) {
	db, fake := newTestDB(t)

	names := keyNames(1200)
	if err := DeleteMultiByKey(context.Background(), db, "User", names); err != nil {
		t.Fatalf("DeleteMultiByKey: %v", err)
	}
	assertEqualSlices(t, "commit sizes", fake.commitSizes(), []int{500, 500, 200})
	assertEqualSlices(t, "deleted keys", fake.mutatedKeyNames(), names)
}

func TestGetMultiChunksAtLookupLimit(t *testing.T) {
	db, fake := newTestDB(t)

	if _, err := GetMulti[testUser](context.Background(), db, "User", keyNames(2500)); err != nil {
		t.Fatalf("GetMulti: %v", err)
	}
	assertEqualSlices(t, "lookup sizes", fake.lookupSizes(), []int{1000, 1000, 500})
}

func TestGetMultiKeepsOrderAndZeroesMissing(t *testing.T) {
	db, fake := newTestDB(t)

	// Every third key is absent; the rest come back with Name set to the key name.
	fake.onLookup = func(req *pb.LookupRequest) (*pb.LookupResponse, error) {
		response := &pb.LookupResponse{}
		for _, key := range req.GetKeys() {
			name := keyName(key)
			if strings.HasSuffix(name, "2") || strings.HasSuffix(name, "5") {
				response.Missing = append(response.Missing, &pb.EntityResult{Entity: &pb.Entity{Key: key}})
				continue
			}
			response.Found = append(response.Found, &pb.EntityResult{Entity: &pb.Entity{
				Key:        key,
				Properties: map[string]*pb.Value{"Name": {ValueType: &pb.Value_StringValue{StringValue: name}}},
			}})
		}
		return response, nil
	}

	names := keyNames(6) // key-0000 .. key-0005
	users, err := GetMulti[testUser](context.Background(), db, "User", names)
	if err != nil {
		t.Fatalf("GetMulti: %v", err)
	}
	if len(users) != len(names) {
		t.Fatalf("got %d users, want %d", len(users), len(names))
	}
	for i, name := range names {
		want := name
		if strings.HasSuffix(name, "2") || strings.HasSuffix(name, "5") {
			want = "" // absent entities stay zero-valued
		}
		if users[i].Name != want {
			t.Errorf("users[%d].Name = %q, want %q", i, users[i].Name, want)
		}
	}
}

func TestGetMultiWrapsRealError(t *testing.T) {
	db, fake := newTestDB(t)
	fake.onLookup = func(*pb.LookupRequest) (*pb.LookupResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "backend refused")
	}

	_, err := GetMulti[testUser](context.Background(), db, "User", keyNames(3))
	if err == nil {
		t.Fatal("GetMulti succeeded, want an error")
	}
	if !strings.HasPrefix(err.Error(), "dsx: get-multi User") {
		t.Errorf("error = %q, want a dsx: get-multi User prefix", err)
	}
}

func TestEmptyBatchesMakeNoRequests(t *testing.T) {
	db, fake := newTestDB(t)
	ctx := context.Background()

	users, err := GetMulti[testUser](ctx, db, "User", nil)
	if err != nil || len(users) != 0 {
		t.Errorf("GetMulti(nil) = %v, %v; want empty, nil", users, err)
	}
	if err := DeleteMultiByKey(ctx, db, "User", nil); err != nil {
		t.Errorf("DeleteMultiByKey(nil): %v", err)
	}
	if err := Query[testUser](db, "User").UpsertMulti(ctx, nil); err != nil {
		t.Errorf("UpsertMulti(nil): %v", err)
	}
	keys, err := Query[testUser](db, "User").InsertMultiWithAutoKey(ctx, nil)
	if err != nil || len(keys) != 0 {
		t.Errorf("InsertMultiWithAutoKey(nil) = %v, %v; want empty, nil", keys, err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.commits) != 0 || len(fake.lookups) != 0 {
		t.Errorf("made %d commits and %d lookups, want none", len(fake.commits), len(fake.lookups))
	}
}

func TestBatchErrorNamesTheFailingChunk(t *testing.T) {
	db, fake := newTestDB(t)

	calls := 0
	fake.onCommit = func(req *pb.CommitRequest) (*pb.CommitResponse, error) {
		calls++
		if calls == 2 {
			return nil, status.Error(codes.InvalidArgument, "boom")
		}
		return commitResponse(req), nil
	}

	err := DeleteMultiByKey(context.Background(), db, "User", keyNames(1200))
	if err == nil {
		t.Fatal("DeleteMultiByKey succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "[500:1000]") {
		t.Errorf("error = %q, want it to name the failing chunk [500:1000]", err)
	}
}
