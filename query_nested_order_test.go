package dalgo2firestore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type nestedOrderServer struct {
	pb.UnimplementedFirestoreServer
	eligible  string
	direction pb.StructuredQuery_Direction
	cutoff    time.Time
	requests  int
}

func (s *nestedOrderServer) RunQuery(req *pb.RunQueryRequest, stream pb.Firestore_RunQueryServer) error {
	q := req.GetStructuredQuery()
	if len(q.GetFrom()) != 1 || q.From[0].CollectionId != "tasks" || !q.From[0].AllDescendants || q.GetLimit().GetValue() != 2 {
		return fmt.Errorf("unexpected query source or limit: %v", q)
	}
	filters := q.GetWhere().GetCompositeFilter().GetFilters()
	if len(filters) != 3 {
		return fmt.Errorf("unexpected filters: %v", filters)
	}
	for i, want := range []string{"spec.owner", "state", s.eligible} {
		f := filters[i].GetFieldFilter()
		if f.GetField().GetFieldPath() != want {
			return fmt.Errorf("filter path = %q, want %q", f.GetField().GetFieldPath(), want)
		}
	}
	if filters[0].GetFieldFilter().GetValue().GetStringValue() != "example" || filters[1].GetFieldFilter().GetValue().GetStringValue() != "pending" || filters[2].GetFieldFilter().GetOp() != pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL || !filters[2].GetFieldFilter().GetValue().GetTimestampValue().AsTime().Equal(s.cutoff) {
		return fmt.Errorf("filter meaning changed: %v", filters)
	}
	if len(q.OrderBy) != 2 || q.OrderBy[0].Field.GetFieldPath() != s.eligible || q.OrderBy[0].Direction != s.direction || q.OrderBy[1].Field.GetFieldPath() != firestore.DocumentID || q.OrderBy[1].Direction != pb.StructuredQuery_ASCENDING {
		return fmt.Errorf("native order = %v", q.OrderBy)
	}
	s.requests++
	return stream.Send(&pb.RunQueryResponse{ReadTime: timestamppb.New(s.cutoff)})
}

// Exercise native SDK serialization and the actual adapter reader, rather than
// asserting only the display string of a DALgo expression.
func TestNestedOrderReachesFirestoreWithRawFieldPath(t *testing.T) {
	for _, field := range []string{"nextAttemptAt", "claim.until"} {
		for _, descending := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/descending=%v", field, descending), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				listener := bufconn.Listen(1 << 20)
				server := grpc.NewServer()
				fake := &nestedOrderServer{eligible: field, direction: pb.StructuredQuery_ASCENDING, cutoff: time.Unix(100, 0).UTC()}
				order := dal.AscendingField(field)
				if descending {
					order = dal.DescendingField(field)
					fake.direction = pb.StructuredQuery_DESCENDING
				}
				pb.RegisterFirestoreServer(server, fake)
				go func() { _ = server.Serve(listener) }()
				t.Cleanup(func() { server.Stop(); _ = listener.Close() })
				client, err := firestore.NewClient(ctx, "nested-order-test", option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())), option.WithGRPCDialOption(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				q := dal.From(dal.NewCollectionGroupRef("tasks", "")).NewQuery().WhereField("spec.owner", dal.Equal, "example").WhereField("state", dal.Equal, "pending").WhereField(field, dal.LessOrEqual, fake.cutoff).OrderBy(order, dal.Ascending(dal.DocumentID())).Limit(2).SelectKeysOnly(reflect.String)
				reader, err := NewDatabase("(default)", client).ExecuteQueryToRecordsReader(ctx, q)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := reader.Close(); err != nil {
						t.Errorf("close query reader: %v", err)
					}
				})
				if _, err := reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
					t.Fatalf("native query rejected valid field path: %v", err)
				}
				if fake.requests != 1 {
					t.Fatalf("native query requests=%d", fake.requests)
				}
			})
		}
	}
}

func TestFirestoreOrderExpression(t *testing.T) {
	tests := []struct {
		name string
		expr dal.Expression
		want string
	}{
		{
			name: "ordinary field",
			expr: dal.Field("title"),
			want: "title",
		},
		{
			name: "dotted field",
			expr: dal.Field("claim.until"),
			want: "claim.until",
		},
		{
			name: "typed document ID",
			expr: dal.DocumentID(),
			want: firestore.DocumentID,
		},
		{
			name: "other expression fallback",
			expr: dal.String("value"),
			want: dal.String("value").String(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := firestoreOrderExpression(tt.expr); got != tt.want {
				t.Errorf("firestoreOrderExpression(%v) = %q, want %q", tt.expr, got, tt.want)
			}
		})
	}
}
