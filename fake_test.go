package dsx

import (
	"context"
	"encoding/base64"
	"net"
	"strings"
	"sync"
	"testing"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const (
	testProject  = "test-project"
	testDatabase = "test-database"
)

// fakeDatastore is an in-process implementation of the Datastore gRPC service.
// It records every request it receives so that tests can assert on the shape of
// the request dsx builds - filters, ordering, namespaces, batch sizes - and it
// lets each test program the responses it needs.
type fakeDatastore struct {
	pb.UnimplementedDatastoreServer

	mu           sync.Mutex
	lookups      []*pb.LookupRequest
	queries      []*pb.RunQueryRequest
	aggregations []*pb.RunAggregationQueryRequest
	commits      []*pb.CommitRequest
	begins       []*pb.BeginTransactionRequest
	rollbacks    []*pb.RollbackRequest

	// Response hooks. A nil hook falls back to an empty, successful response.
	onLookup      func(*pb.LookupRequest) (*pb.LookupResponse, error)
	onQuery       func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error)
	onAggregation func(*pb.RunAggregationQueryRequest) (*pb.RunAggregationQueryResponse, error)
	onCommit      func(*pb.CommitRequest) (*pb.CommitResponse, error)
}

func (f *fakeDatastore) Lookup(_ context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	f.mu.Lock()
	f.lookups = append(f.lookups, req)
	hook := f.onLookup
	f.mu.Unlock()

	if hook != nil {
		return hook(req)
	}
	// Default: nothing exists.
	missing := make([]*pb.EntityResult, 0, len(req.GetKeys()))
	for _, key := range req.GetKeys() {
		missing = append(missing, &pb.EntityResult{Entity: &pb.Entity{Key: key}})
	}
	return &pb.LookupResponse{Missing: missing}, nil
}

func (f *fakeDatastore) RunQuery(_ context.Context, req *pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
	f.mu.Lock()
	f.queries = append(f.queries, req)
	hook := f.onQuery
	f.mu.Unlock()

	if hook != nil {
		return hook(req)
	}
	return queryResponse(nil), nil
}

func (f *fakeDatastore) RunAggregationQuery(_ context.Context, req *pb.RunAggregationQueryRequest) (*pb.RunAggregationQueryResponse, error) {
	f.mu.Lock()
	f.aggregations = append(f.aggregations, req)
	hook := f.onAggregation
	f.mu.Unlock()

	if hook != nil {
		return hook(req)
	}
	return aggregationResponse(0), nil
}

func (f *fakeDatastore) Commit(_ context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	f.mu.Lock()
	f.commits = append(f.commits, req)
	hook := f.onCommit
	f.mu.Unlock()

	if hook != nil {
		return hook(req)
	}
	return commitResponse(req), nil
}

func (f *fakeDatastore) BeginTransaction(_ context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	f.mu.Lock()
	f.begins = append(f.begins, req)
	f.mu.Unlock()
	return &pb.BeginTransactionResponse{Transaction: []byte("fake-transaction")}, nil
}

func (f *fakeDatastore) Rollback(_ context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	f.mu.Lock()
	f.rollbacks = append(f.rollbacks, req)
	f.mu.Unlock()
	return &pb.RollbackResponse{}, nil
}

// commitResponse acknowledges every mutation in req, allocating a key for the
// incomplete ones the way the real service does for auto-ID inserts.
func commitResponse(req *pb.CommitRequest) *pb.CommitResponse {
	results := make([]*pb.MutationResult, 0, len(req.GetMutations()))
	for i, mutation := range req.GetMutations() {
		result := &pb.MutationResult{}
		if upsert := mutation.GetUpsert(); upsert != nil && isIncomplete(upsert.GetKey()) {
			result.Key = allocatedKey(upsert.GetKey(), int64(i+1))
		}
		if insert := mutation.GetInsert(); insert != nil && isIncomplete(insert.GetKey()) {
			result.Key = allocatedKey(insert.GetKey(), int64(i+1))
		}
		results = append(results, result)
	}
	return &pb.CommitResponse{MutationResults: results}
}

func isIncomplete(key *pb.Key) bool {
	path := key.GetPath()
	if len(path) == 0 {
		return false
	}
	last := path[len(path)-1]
	return last.GetName() == "" && last.GetId() == 0
}

// allocatedKey returns key with an ID assigned to its final path element.
func allocatedKey(key *pb.Key, id int64) *pb.Key {
	path := make([]*pb.Key_PathElement, len(key.GetPath()))
	copy(path, key.GetPath())
	last := path[len(path)-1]
	path[len(path)-1] = &pb.Key_PathElement{
		Kind:   last.GetKind(),
		IdType: &pb.Key_PathElement_Id{Id: id},
	}
	return &pb.Key{PartitionId: key.GetPartitionId(), Path: path}
}

// queryResponse wraps results in a single exhausted batch.
func queryResponse(results []*pb.EntityResult) *pb.RunQueryResponse {
	return &pb.RunQueryResponse{Batch: &pb.QueryResultBatch{
		EntityResultType: pb.EntityResult_FULL,
		EntityResults:    results,
		EndCursor:        []byte("end-cursor"),
		MoreResults:      pb.QueryResultBatch_NO_MORE_RESULTS,
	}}
}

// keysOnlyResponse wraps key-only results in a single exhausted batch.
func keysOnlyResponse(results []*pb.EntityResult) *pb.RunQueryResponse {
	return &pb.RunQueryResponse{Batch: &pb.QueryResultBatch{
		EntityResultType: pb.EntityResult_KEY_ONLY,
		EntityResults:    results,
		EndCursor:        []byte("end-cursor"),
		MoreResults:      pb.QueryResultBatch_NO_MORE_RESULTS,
	}}
}

func aggregationResponse(count int64) *pb.RunAggregationQueryResponse {
	return &pb.RunAggregationQueryResponse{Batch: &pb.AggregationResultBatch{
		AggregationResults: []*pb.AggregationResult{{
			AggregateProperties: map[string]*pb.Value{
				"total": {ValueType: &pb.Value_IntegerValue{IntegerValue: count}},
			},
		}},
	}}
}

// testUser is the entity type used throughout the tests.
type testUser struct {
	Name   string
	Status string
}

// protoKey builds a named key proto in the given namespace.
func protoKey(namespace, kind, name string) *pb.Key {
	return &pb.Key{
		PartitionId: &pb.PartitionId{ProjectId: testProject, DatabaseId: testDatabase, NamespaceId: namespace},
		Path:        []*pb.Key_PathElement{{Kind: kind, IdType: &pb.Key_PathElement_Name{Name: name}}},
	}
}

// userResult builds a full entity result for a testUser.
func userResult(namespace, name, status string) *pb.EntityResult {
	return &pb.EntityResult{Entity: &pb.Entity{
		Key: protoKey(namespace, "User", name),
		Properties: map[string]*pb.Value{
			"Name":   {ValueType: &pb.Value_StringValue{StringValue: name}},
			"Status": {ValueType: &pb.Value_StringValue{StringValue: status}},
		},
	}}
}

// keyResult builds a key-only entity result.
func keyResult(namespace, kind, name string) *pb.EntityResult {
	return &pb.EntityResult{Entity: &pb.Entity{Key: protoKey(namespace, kind, name)}}
}

// newTestDB starts a fake Datastore on an in-process listener and connects a DB
// to it through the real Connect path. Both are torn down with the test.
func newTestDB(t *testing.T, opts ...Option) (*DB, *fakeDatastore) {
	t.Helper()

	fake := &fakeDatastore{}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterDatastoreServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial fake datastore: %v", err)
	}

	opts = append(opts, WithClientOptions(option.WithGRPCConn(conn)))
	db, err := Connect(context.Background(), testProject, testDatabase, opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db, fake
}

// lastQuery returns the most recent RunQuery request, failing if there is none.
func (f *fakeDatastore) lastQuery(t *testing.T) *pb.RunQueryRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queries) == 0 {
		t.Fatal("no RunQuery request was made")
	}
	return f.queries[len(f.queries)-1]
}

// commitSizes returns the number of mutations in each commit, in order.
func (f *fakeDatastore) commitSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	sizes := make([]int, 0, len(f.commits))
	for _, commit := range f.commits {
		sizes = append(sizes, len(commit.GetMutations()))
	}
	return sizes
}

// lookupSizes returns the number of keys in each lookup, in order.
func (f *fakeDatastore) lookupSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	sizes := make([]int, 0, len(f.lookups))
	for _, lookup := range f.lookups {
		sizes = append(sizes, len(lookup.GetKeys()))
	}
	return sizes
}

// keyNames extracts the key names of every mutation across all commits, in order.
func (f *fakeDatastore) mutatedKeyNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for _, commit := range f.commits {
		for _, mutation := range commit.GetMutations() {
			switch {
			case mutation.GetUpsert() != nil:
				names = append(names, keyName(mutation.GetUpsert().GetKey()))
			case mutation.GetInsert() != nil:
				names = append(names, keyName(mutation.GetInsert().GetKey()))
			case mutation.GetDelete() != nil:
				names = append(names, keyName(mutation.GetDelete()))
			}
		}
	}
	return names
}

func keyName(key *pb.Key) string {
	path := key.GetPath()
	if len(path) == 0 {
		return ""
	}
	return path[len(path)-1].GetName()
}

// encodeCursor renders raw cursor bytes the way datastore.Cursor.String does,
// so that datastore.DecodeCursor accepts the result.
func encodeCursor(raw string) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString([]byte(raw)), "=")
}
