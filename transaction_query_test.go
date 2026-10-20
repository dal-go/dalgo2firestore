package dalgo2firestore

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type transactionQueryServer struct {
	pb.UnimplementedFirestoreServer
	requests chan *pb.RunQueryRequest
	begins   chan *pb.BeginTransactionRequest
	hold     bool
	stopped  chan struct{}
	failure  codes.Code
}

func (s *transactionQueryServer) BeginTransaction(_ context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	s.begins <- req
	return &pb.BeginTransactionResponse{Transaction: []byte("actual-transaction")}, nil
}
func (*transactionQueryServer) Commit(context.Context, *pb.CommitRequest) (*pb.CommitResponse, error) {
	return &pb.CommitResponse{CommitTime: timestamppb.Now()}, nil
}
func (*transactionQueryServer) Rollback(context.Context, *pb.RollbackRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (s *transactionQueryServer) RunQuery(req *pb.RunQueryRequest, stream pb.Firestore_RunQueryServer) error {
	s.requests <- req
	if s.failure != codes.OK {
		return status.Error(s.failure, "query refused")
	}
	name := req.Parent + "/items/result"
	if req.GetStructuredQuery().From[0].AllDescendants {
		name = req.Parent + "/spaces/a/items/result"
	}
	doc := &pb.Document{Name: name, Fields: map[string]*pb.Value{}, CreateTime: timestamppb.Now(), UpdateTime: timestamppb.Now()}
	if err := stream.Send(&pb.RunQueryResponse{Document: doc, ReadTime: timestamppb.Now()}); err != nil {
		return err
	}
	if s.hold {
		<-stream.Context().Done()
		close(s.stopped)
		return stream.Context().Err()
	}
	return nil
}
func transactionQueryDB(t *testing.T, server *transactionQueryServer) (dal.DB, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	server.requests = make(chan *pb.RunQueryRequest, 4)
	server.begins = make(chan *pb.BeginTransactionRequest, 4)
	server.stopped = make(chan struct{})
	listener := bufconn.Listen(1 << 20)
	rpc := grpc.NewServer()
	pb.RegisterFirestoreServer(rpc, server)
	go func() { _ = rpc.Serve(listener) }()
	t.Cleanup(func() { rpc.Stop(); _ = listener.Close() })
	client, err := firestore.NewClient(ctx, "transaction-query-test", option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())), option.WithGRPCDialOption(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return NewDatabase("(default)", client), ctx, cancel
}
func transactionTestQuery(group bool) dal.Query {
	var source dal.RecordsetSource = dal.NewCollectionRef("items", "", record.NewKeyWithID("spaces", "a"))
	cursor := "before"
	if group {
		source = dal.NewCollectionGroupRef("items", "")
		cursor = "spaces/a/items/before"
	}
	return dal.From(source).NewQuery().OrderBy(dal.Ascending(dal.DocumentID())).StartAfter(dal.Cursor(cursor)).Limit(2).SelectIntoRecord(func() record.Record { return record.NewRecordWithIncompleteKey("items", reflect.String, nil) })
}

func TestTransactionQueriesCarrySDKSelectorAndPreserveTranslation(t *testing.T) {
	for _, mode := range []string{"ordinary", "readonly", "readwrite"} {
		for _, group := range []bool{false, true} {
			name := mode + "/collection"
			if group {
				name = mode + "/group"
			}
			t.Run(name, func(t *testing.T) {
				server := new(transactionQueryServer)
				db, ctx, _ := transactionQueryDB(t, server)
				read := func(ctx context.Context, session dal.ReadSession) error {
					rows, err := dal.ExecuteQueryAndReadAllToRecords(ctx, transactionTestQuery(group), session)
					if err == nil && (len(rows) != 1 || rows[0].Key().ID != "result") {
						t.Errorf("returned rows = %v", rows)
					}
					return err
				}
				var err error
				switch mode {
				case "ordinary":
					err = read(ctx, db)
				case "readonly":
					err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error { return read(ctx, tx) })
				case "readwrite":
					err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error { return read(ctx, tx) })
				}
				if err != nil {
					t.Fatal(err)
				}
				req := <-server.requests
				if mode == "ordinary" {
					if req.ConsistencySelector != nil {
						t.Fatalf("ordinary query selector = %v", req.ConsistencySelector)
					}
					if len(server.begins) != 0 {
						t.Fatal("ordinary query began a transaction")
					}
				} else {
					if string(req.GetTransaction()) != "actual-transaction" {
						t.Fatalf("transaction selector = %v", req.ConsistencySelector)
					}
					begin := <-server.begins
					if (begin.GetOptions().GetReadOnly() != nil) != (mode == "readonly") {
						t.Fatalf("transaction options = %v", begin.Options)
					}
				}
				q := req.GetStructuredQuery()
				parent := "projects/transaction-query-test/databases/(default)/documents"
				cursor := parent + "/spaces/a/items/before"
				if !group {
					parent += "/spaces/a"
				}
				if req.Parent != parent || len(q.From) != 1 || q.From[0].CollectionId != "items" || q.From[0].AllDescendants != group {
					t.Fatalf("translated scope = %v", req)
				}
				if q.Limit.GetValue() != 2 || len(q.OrderBy) != 1 || q.OrderBy[0].Field.GetFieldPath() != firestore.DocumentID || q.StartAt == nil || q.StartAt.Before || len(q.StartAt.Values) != 1 || q.StartAt.Values[0].GetReferenceValue() != cursor {
					t.Fatalf("translated cursor/window = %v", q)
				}
			})
		}
	}
}
func TestTransactionQueryReadAfterWriteIsRefused(t *testing.T) {
	server := new(transactionQueryServer)
	db, ctx, _ := transactionQueryDB(t, server)
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		if err := tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("items", "written"), map[string]any{"value": 1})); err != nil {
			return err
		}
		reader, err := tx.ExecuteQueryToRecordsReader(ctx, transactionTestQuery(false))
		if err != nil {
			return err
		}
		defer func() { _ = reader.Close() }()
		_, err = reader.Next()
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "read after write") {
		t.Fatalf("expected SDK read-after-write refusal, got %v", err)
	}
	if len(server.requests) != 0 {
		t.Fatal("read after write escaped to a nontransactional RunQuery")
	}
}
func TestTransactionQueryErrorsCancellationAndCleanup(t *testing.T) {
	t.Run("provider-error", func(t *testing.T) {
		server := &transactionQueryServer{failure: codes.PermissionDenied}
		db, ctx, _ := transactionQueryDB(t, server)
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			_, err := dal.ExecuteQueryAndReadAllToRecords(ctx, transactionTestQuery(false), tx)
			return err
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("query failure = %v", err)
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		server := new(transactionQueryServer)
		db, ctx, cancel := transactionQueryDB(t, server)
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			cancel()
			_, err := dal.ExecuteQueryAndReadAllToRecords(ctx, transactionTestQuery(false), tx)
			return err
		})
		if !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
			t.Fatalf("query cancellation = %v", err)
		}
	})
	for _, mode := range []string{"ordinary", "readonly"} {
		t.Run(mode+"-close", func(t *testing.T) {
			server := &transactionQueryServer{hold: true}
			db, ctx, _ := transactionQueryDB(t, server)
			read := func(ctx context.Context, session dal.ReadSession) error {
				reader, err := session.ExecuteQueryToRecordsReader(ctx, transactionTestQuery(false))
				if err != nil {
					return err
				}
				if _, err = reader.Next(); err != nil {
					_ = reader.Close()
					return err
				}
				if err = reader.Close(); err != nil {
					return err
				}
				if err = reader.Close(); err != nil {
					return err
				}
				select {
				case <-server.stopped:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			var err error
			if mode == "ordinary" {
				err = read(ctx, db)
			} else {
				err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error { return read(ctx, tx) })
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
