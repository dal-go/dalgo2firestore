package dalgo2firestore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type opaquePageServer struct {
	pb.UnimplementedFirestoreServer
	ids        []string
	at         time.Time
	fieldValue *pb.Value
	requests   []*pb.RunQueryRequest
}

func (s *opaquePageServer) RunQuery(req *pb.RunQueryRequest, stream pb.Firestore_RunQueryServer) error {
	s.requests = append(s.requests, proto.Clone(req).(*pb.RunQueryRequest))
	q := req.GetStructuredQuery()
	if len(q.OrderBy) != 2 || q.OrderBy[0].Field.FieldPath != "due" || q.OrderBy[1].Field.FieldPath != firestore.DocumentID || q.Offset != 0 || q.Limit.GetValue() != 2 {
		return fmt.Errorf("unexpected page query: %v", q)
	}
	boundary := ""
	if cursor := q.StartAt; cursor != nil {
		if cursor.Before || len(cursor.Values) != 2 || !cursor.Values[0].GetTimestampValue().AsTime().Equal(s.at) {
			return fmt.Errorf("invalid composite continuation: %v", cursor)
		}
		boundary = cursor.Values[1].GetReferenceValue()
	}
	n := 0
	value := s.fieldValue
	if value == nil {
		value = &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(s.at)}}
	}
	for _, id := range s.ids {
		name := req.Parent + "/" + id
		if name <= boundary {
			continue
		}
		if err := stream.Send(&pb.RunQueryResponse{Document: &pb.Document{Name: name, CreateTime: timestamppb.New(s.at), UpdateTime: timestamppb.New(s.at), Fields: map[string]*pb.Value{
			"due": value,
		}}, ReadTime: timestamppb.New(s.at)}); err != nil {
			return err
		}
		n++
		if n == int(q.Limit.GetValue()) {
			break
		}
	}
	return nil
}

func opaqueCursorFixture(t *testing.T) (*firestore.Client, *opaquePageServer) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	fake := &opaquePageServer{ids: []string{"spaces/a/tasks/a", "spaces/a/tasks/b", "spaces/b/tasks/c", "spaces/c/tasks/d"}, at: time.Unix(100, 0).UTC()}
	pb.RegisterFirestoreServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	client, err := firestore.NewClient(context.Background(), "opaque-cursor-test", option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())), option.WithGRPCDialOption(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, fake
}

func opaquePageQuery(at time.Time, after string) dal.StructuredQuery {
	b := dal.From(dal.NewCollectionGroupRef("tasks", "")).NewQuery().WhereField("due", dal.LessOrEqual, at).
		OrderBy(dal.AscendingField("due"), dal.Ascending(dal.DocumentID())).Limit(2)
	if after != "" {
		b = b.StartAfter(dal.Cursor(after))
	}
	return b.SelectIntoRecord(func() record.Record {
		fields := make(map[string]any)
		return record.NewRecordWithIncompleteKey("tasks", reflect.String, &fields)
	})
}

func TestOpaqueCursorContinuesEqualTimestampsAcrossSpacesAndDeletedBoundary(t *testing.T) {
	client, fake := opaqueCursorFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	page := func(after string) ([]string, string) {
		t.Helper()
		r, err := NewDatabase("(default)", client).ExecuteQueryToRecordsReader(ctx, opaquePageQuery(fake.at, after))
		if err != nil {
			t.Fatal(err)
		}
		if c, err := r.Cursor(); err != nil || c != "" {
			t.Fatal("reader started at a position without a consumed row", c, err)
		}
		var ids []string
		for {
			row, err := r.Next()
			if errors.Is(err, dal.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, row.Key().ID.(string))
		}
		cursor, err := r.Cursor()
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if closed, err := r.Cursor(); err != nil || closed != cursor {
			t.Fatal("closing changed the last consumed position", err)
		}
		return ids, cursor
	}
	ids, cursor := page("")
	if !reflect.DeepEqual(ids, []string{"a", "b"}) || !strings.HasPrefix(cursor, opaqueCursorPrefix) {
		t.Fatal(ids, cursor)
	}
	// A native values cursor needs no lookup of the deleted boundary document.
	fake.ids = []string{"spaces/b/tasks/c", "spaces/c/tasks/d"}
	ids, next := page(cursor)
	if !reflect.DeepEqual(ids, []string{"c", "d"}) || next == cursor {
		t.Fatal("tied values duplicated/skipped a page", ids)
	}
	ids, end := page(next)
	if len(ids) != 0 || end != "" || len(fake.requests) != 3 {
		t.Fatal("end of cycle should restart explicitly", ids, len(fake.requests))
	}
}

func rewriteOpaqueCursor(t *testing.T, token string, mutate func(*pb.RunQueryRequest)) string {
	t.Helper()
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, opaqueCursorPrefix))
	if err != nil {
		t.Fatal(err)
	}
	request := new(pb.RunQueryRequest)
	if err := proto.Unmarshal(data, request); err != nil {
		t.Fatal(err)
	}
	mutate(request)
	data, err = proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return opaqueCursorPrefix + base64.RawURLEncoding.EncodeToString(data)
}

func TestOpaqueCursorRejectsQueryChangesAndMalformedPositionsBeforeReading(t *testing.T) {
	client, fake := opaqueCursorFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := newFirestoreReader(ctx, client, opaquePageQuery(fake.at, ""))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	token, err := r.Cursor()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		alter func(*pb.RunQueryRequest)
	}{
		{"project", func(r *pb.RunQueryRequest) { r.Parent = "projects/other/databases/(default)/documents" }},
		{"database", func(r *pb.RunQueryRequest) { r.Parent = "projects/opaque-cursor-test/databases/other/documents" }},
		{"cutoff", func(r *pb.RunQueryRequest) {
			r.GetStructuredQuery().Where.GetFieldFilter().Value = &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(fake.at.Add(time.Hour))}}
		}},
		{"limit", func(r *pb.RunQueryRequest) { r.GetStructuredQuery().Limit.Value++ }},
		{"offset", func(r *pb.RunQueryRequest) { r.GetStructuredQuery().Offset = 1 }},
		{"no cursor", func(r *pb.RunQueryRequest) { r.GetStructuredQuery().StartAt = nil }},
		{"inclusive", func(r *pb.RunQueryRequest) { r.GetStructuredQuery().StartAt.Before = true }},
		{"partial", func(r *pb.RunQueryRequest) {
			r.GetStructuredQuery().StartAt.Values = r.GetStructuredQuery().StartAt.Values[:1]
		}},
		{"nil value", func(r *pb.RunQueryRequest) { r.GetStructuredQuery().StartAt.Values[0] = &pb.Value{} }},
		{"unknown cursor field", func(r *pb.RunQueryRequest) { r.GetStructuredQuery().StartAt.ProtoReflect().SetUnknown([]byte{0x18, 1}) }},
		{"bad timestamp", func(r *pb.RunQueryRequest) {
			r.GetStructuredQuery().StartAt.Values[0] = &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: &timestamppb.Timestamp{Seconds: 253402300800}}}
		}},
		{"ordinary reference", func(r *pb.RunQueryRequest) {
			r.GetStructuredQuery().StartAt.Values[0] = &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: r.Parent + "/tasks/a"}}
		}},
		{"composite ordinary value", func(r *pb.RunQueryRequest) {
			r.GetStructuredQuery().StartAt.Values[0] = &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{}}}
		}},
		{"foreign reference", func(r *pb.RunQueryRequest) {
			r.GetStructuredQuery().StartAt.Values[1] = &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: "projects/other/databases/(default)/documents/spaces/a/tasks/a"}}
		}},
		{"wrong collection", func(r *pb.RunQueryRequest) {
			r.GetStructuredQuery().StartAt.Values[1] = &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: r.Parent + "/spaces/a/foreign/a"}}
		}},
		{"empty path segment", func(r *pb.RunQueryRequest) {
			r.GetStructuredQuery().StartAt.Values[1] = &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: r.Parent + "/spaces//tasks/a"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := rewriteOpaqueCursor(t, token, tc.alter)
			if _, err := newFirestoreReader(ctx, client, opaquePageQuery(fake.at, bad)); err == nil {
				t.Fatal("accepted a changed/malformed continuation")
			}
		})
	}
	for _, bad := range []string{opaqueCursorPrefix + "!", opaqueCursorPrefix + "AA", "//dalgo-firestore/v2/unknown", opaqueCursorPrefix + strings.Repeat("A", maxOpaqueCursorBytes)} {
		if _, err := newFirestoreReader(ctx, client, opaquePageQuery(fake.at, bad)); err == nil {
			t.Fatal("accepted invalid envelope")
		}
	}
	if len(fake.requests) != 1 {
		t.Fatal("rejected cursor performed a query", len(fake.requests))
	}
	if got, err := firestoreDocumentIDCursor(client, "fsq1:legacy", false); err != nil || got != "fsq1:legacy" {
		t.Fatal("legacy ID was reinterpreted", got, err)
	}
}

func TestOpaqueCursorOwnsExecutedQueryDespiteCallerMutation(t *testing.T) {
	client, fake := opaqueCursorFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := dal.From(dal.NewCollectionGroupRef("tasks", "")).NewQuery().
		WhereField("due", dal.LessOrEqual, fake.at).WhereField("state", dal.Equal, "pending").
		OrderBy(dal.AscendingField("due"), dal.Ascending(dal.DocumentID())).Limit(2).
		SelectIntoRecord(func() record.Record {
			fields := make(map[string]any)
			return record.NewRecordWithIncompleteKey("tasks", reflect.String, &fields)
		})
	r, err := newFirestoreReader(ctx, client, q)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	// Both of these public accessors return mutable backing slices in DALgo.
	q.OrderBy()[0] = dal.DescendingField("other")
	q.Where().(dal.GroupCondition).Conditions()[0] = dal.NewComparison(dal.Field("due"), dal.LessOrEqual, dal.NewConstant(fake.at.Add(time.Hour)))
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	cursor, err := r.Cursor()
	if err != nil {
		t.Fatal(err)
	}
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(cursor, opaqueCursorPrefix))
	if err != nil {
		t.Fatal(err)
	}
	request := new(pb.RunQueryRequest)
	if err := proto.Unmarshal(data, request); err != nil {
		t.Fatal(err)
	}
	query := request.GetStructuredQuery()
	if query.OrderBy[0].Field.FieldPath != "due" || !query.Where.GetCompositeFilter().Filters[0].GetFieldFilter().Value.GetTimestampValue().AsTime().Equal(fake.at) {
		t.Fatal("cursor was minted from mutated caller query", query)
	}
}

func TestOpaqueCursorUnsupportedWindowsAndValueFailuresStayExplicit(t *testing.T) {
	client, fake := opaqueCursorFixture(t)
	base := opaquePageQuery(fake.at, "")
	withoutID := opaquePageQuery(fake.at, "")
	withoutID.OrderBy()[1] = dal.AscendingField("not-an-id")
	expression := opaquePageQuery(fake.at, "")
	expression.OrderBy()[0] = dal.Ascending(dal.String("value"))
	for _, q := range []dal.StructuredQuery{nil, withoutID, expression} {
		if supportsOpaqueCursor(q) {
			t.Fatal("claimed unsupported ordering")
		}
	}
	if _, err := applyOpaqueCursor(base, client.Collection("tasks").Query); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatal("accepted missing continuation", err)
	}
	if _, err := applyOpaqueCursor(opaquePageQuery(fake.at, opaqueCursorPrefix+"AA"), client.Collection("tasks").OrderBy("bad[", firestore.Asc)); err == nil {
		t.Fatal("invalid native query accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := newFirestoreReader(ctx, client, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	valid, err := r.Cursor()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyOpaqueCursor(opaquePageQuery(fake.at, valid), client.Collection("tasks").OrderBy("bad[", firestore.Asc)); err == nil {
		t.Fatal("native serialization failure ignored")
	}
	r.nativeQuery = client.Collection("tasks").OrderBy("bad[", firestore.Asc)
	if token, err := r.Cursor(); err == nil || token != "" {
		t.Fatal("missing document minted a cursor", token, err)
	}
	for _, value := range []*pb.Value{
		{ValueType: &pb.Value_BooleanValue{BooleanValue: true}},
		{ValueType: &pb.Value_IntegerValue{IntegerValue: 4}},
		{ValueType: &pb.Value_DoubleValue{DoubleValue: 1.5}},
		{ValueType: &pb.Value_StringValue{StringValue: "x"}},
		{ValueType: &pb.Value_BytesValue{BytesValue: []byte("x")}},
		{ValueType: &pb.Value_NullValue{}},
	} {
		if err := validateOpaqueValues(&pb.Cursor{Values: []*pb.Value{value}}); err != nil {
			t.Fatal("supported scalar rejected", value, err)
		}
	}
}

func TestOpaqueCursorConstructionFailsBeforeReadOnInvalidQueryOrSDKFailure(t *testing.T) {
	client, fake := opaqueCursorFixture(t)
	ctx := context.Background()
	bad := opaquePageQuery(fake.at, "")
	bad.OrderBy()[0] = dal.AscendingField("bad[")
	if _, err := newFirestoreReader(ctx, client, bad); err == nil {
		t.Fatal("invalid native query accepted")
	}
	saved := deserializeCursorQuery
	t.Cleanup(func() { deserializeCursorQuery = saved })
	failure := errors.New("SDK query round-trip failure")
	deserializeCursorQuery = func(q firestore.Query, _ []byte) (firestore.Query, error) { return q, failure }
	if _, err := newFirestoreReader(ctx, client, opaquePageQuery(fake.at, "")); !errors.Is(err, failure) {
		t.Fatal("SDK round-trip error swallowed", err)
	}
	if len(fake.requests) != 0 {
		t.Fatal("failed construction performed a query")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("missing required-client panic")
		}
	}()
	_, _ = newFirestoreReader(ctx, nil, opaquePageQuery(fake.at, ""))
}

func TestOpaqueCursorNeverReturnsUnusableOrOversizedTokens(t *testing.T) {
	for _, kind := range []string{"map", "array", "reference", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			client, fake := opaqueCursorFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			switch kind {
			case "map":
				fake.fieldValue = &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{}}}
			case "array":
				fake.fieldValue = &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{}}}
			case "reference":
				fake.fieldValue = &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: "projects/opaque-cursor-test/databases/(default)/documents/tasks/a"}}
			}
			b := dal.From(dal.NewCollectionGroupRef("tasks", "")).NewQuery().OrderBy(dal.AscendingField("due"), dal.Ascending(dal.DocumentID())).Limit(2)
			if kind == "oversized" {
				b = b.WhereField("largeFilter", dal.Equal, strings.Repeat("x", maxOpaqueCursorBytes))
			}
			q := b.SelectIntoRecord(func() record.Record {
				fields := make(map[string]any)
				return record.NewRecordWithIncompleteKey("tasks", reflect.String, &fields)
			})
			r, err := newFirestoreReader(ctx, client, q)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			if _, err := r.Next(); err != nil {
				t.Fatal(err)
			}
			if token, err := r.Cursor(); err == nil || token != "" {
				t.Fatal("returned an unusable/oversized continuation", len(token), err)
			}
		})
	}
}

func TestOpaqueCursorRetainsLastSuccessfulRowAfterConversionFailure(t *testing.T) {
	client, fake := opaqueCursorFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	badTarget := false
	q := dal.From(dal.NewCollectionGroupRef("tasks", "")).NewQuery().WhereField("due", dal.LessOrEqual, fake.at).
		OrderBy(dal.AscendingField("due"), dal.Ascending(dal.DocumentID())).Limit(2).
		SelectIntoRecord(func() record.Record {
			if badTarget {
				n := 0
				return record.NewRecordWithIncompleteKey("tasks", reflect.String, &n)
			}
			fields := make(map[string]any)
			return record.NewRecordWithIncompleteKey("tasks", reflect.String, &fields)
		})
	r, err := newFirestoreReader(ctx, client, q)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	before, err := r.Cursor()
	if err != nil {
		t.Fatal(err)
	}
	badTarget = true
	if _, err := r.Next(); err == nil {
		t.Fatal("expected conversion error")
	}
	if after, err := r.Cursor(); err != nil || after != before {
		t.Fatal("failed conversion advanced continuation", err)
	}
}
