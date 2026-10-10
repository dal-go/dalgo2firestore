package dalgo2firestore

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFirestoreDocumentIDExpressionIsTyped(t *testing.T) {
	if got := firestoreOrderExpression(dal.DocumentID()); got != firestore.DocumentID {
		t.Fatalf("typed ID = %q", got)
	}
	if got := firestoreOrderExpression(dal.Field("__name__")); got != "__name__" {
		t.Fatalf("ordinary field = %q", got)
	}
}

type cursorTestDocument struct{}

type cursorFirestoreServer struct {
	firestorepb.UnimplementedFirestoreServer
	t             *testing.T
	parent        string
	collection    string
	wantCursor    string
	page          int
	pageDocuments [][]string
}

func (s *cursorFirestoreServer) RunQuery(req *firestorepb.RunQueryRequest, stream firestorepb.Firestore_RunQueryServer) error {
	s.t.Helper()
	query := req.GetStructuredQuery()
	if req.Parent != s.parent {
		return fmt.Errorf("query parent = %q, want %q", req.Parent, s.parent)
	}
	if len(query.From) != 1 || query.From[0].CollectionId != s.collection {
		return fmt.Errorf("query collection = %+v, want %q", query.From, s.collection)
	}
	if len(query.OrderBy) != 1 || query.OrderBy[0].Field.GetFieldPath() != firestore.DocumentID || query.Limit.GetValue() != 3 {
		return fmt.Errorf("query order/limit = %+v/%v, want document ID/3", query.OrderBy, query.Limit)
	}
	switch s.page {
	case 0:
		if query.StartAt != nil {
			return fmt.Errorf("first page unexpectedly has cursor: %+v", query.StartAt)
		}
	case 1:
		wantCursor := s.wantCursor
		if wantCursor == "" {
			wantCursor = s.parent + "/" + s.collection + "/delivered-c"
		}
		if query.StartAt == nil || query.StartAt.GetBefore() || len(query.StartAt.GetValues()) != 1 || query.StartAt.GetValues()[0].GetReferenceValue() != wantCursor {
			return fmt.Errorf("continuation cursor = %+v, want exclusive document reference %q", query.StartAt, wantCursor)
		}
	default:
		return fmt.Errorf("unexpected RunQuery call %d", s.page+1)
	}
	page := s.page
	s.page++
	for _, id := range s.pageDocuments[page] {
		name := req.Parent + "/" + s.collection + "/" + id
		if len(query.From) == 1 && query.From[0].AllDescendants {
			name = req.Parent + "/" + id
		}
		doc := &firestorepb.Document{Name: name,
			Fields: map[string]*firestorepb.Value{}, CreateTime: timestamppb.New(time.Unix(1, 0)), UpdateTime: timestamppb.New(time.Unix(1, 0))}
		if err := stream.Send(&firestorepb.RunQueryResponse{Document: doc, ReadTime: timestamppb.New(time.Unix(2, 0))}); err != nil {
			return err
		}
	}
	return nil
}

func TestExecuteCollectionGroupQueryUsesFullDocumentReferenceCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	parent := "projects/cursor-group-test/databases/(default)/documents"
	cursor := "spaces/space-a/ext/datatug/queryActivityPending/delivered-c"
	fake := &cursorFirestoreServer{t: t, parent: parent, collection: "queryActivityPending", wantCursor: parent + "/" + cursor,
		pageDocuments: [][]string{{"spaces/space-a/ext/datatug/queryActivityPending/delivered-c"}, {"spaces/space-b/ext/datatug/queryActivityPending/pending-d"}}}
	firestorepb.RegisterFirestoreServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	client, err := firestore.NewClient(ctx, "cursor-group-test", option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithGRPCDialOption(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	db := NewDatabase("(default)", client)
	queryPage := func(after dal.Cursor) {
		t.Helper()
		builder := dal.From(dal.NewCollectionGroupRef(fake.collection, "")).NewQuery().OrderBy(dal.Ascending(dal.DocumentID())).Limit(3)
		if after != "" {
			builder = builder.StartAfter(after)
		}
		query := builder.SelectKeysOnly(reflect.String)
		reader, err := db.ExecuteQueryToRecordsReader(ctx, query)
		if err != nil {
			t.Fatalf("execute collection-group page after %q: %v", after, err)
		}
		defer reader.Close()
		if _, err := reader.Next(); err != nil {
			t.Fatalf("read collection-group page after %q: %v", after, err)
		}
	}
	queryPage("")
	queryPage(dal.Cursor(cursor))
	if fake.page != 2 {
		t.Fatalf("RunQuery calls = %d, want 2", fake.page)
	}
}

func TestCollectionGroupDocumentIDCursorRejectsInvalidPath(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), "query-window-test", option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	for _, cursor := range []dal.Cursor{"", "one", "spaces//queries/doc", "spaces/space-a/queries"} {
		if cursor == "" {
			continue // An empty DAL cursor means there is no cursor.
		}
		if _, err := firestoreDocumentIDCursor(client, cursor, true); err == nil {
			t.Errorf("cursor %q unexpectedly accepted", cursor)
		}
	}
}

func TestCollectionGroupDocumentIDCursorsUseDocumentReferences(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), "query-window-test", option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	base := client.CollectionGroup("queryActivityPending").Query
	cursor := dal.Cursor("spaces/space-a/ext/datatug/queryActivityPending/receipt-a")
	for _, test := range []struct {
		name  string
		start func(dal.IQueryBuilder) dal.IQueryBuilder
		want  func(firestore.Query, *firestore.DocumentRef) firestore.Query
	}{
		{name: "inclusive", start: func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.StartFrom(cursor) }, want: func(q firestore.Query, ref *firestore.DocumentRef) firestore.Query { return q.StartAt(ref) }},
		{name: "exclusive", start: func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.StartAfter(cursor) }, want: func(q firestore.Query, ref *firestore.DocumentRef) firestore.Query { return q.StartAfter(ref) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := dal.From(dal.NewCollectionGroupRef("queryActivityPending", "")).NewQuery().OrderBy(dal.Ascending(dal.DocumentID()))
			structured := test.start(builder).SelectKeysOnly(reflect.String)
			got, err := applyQueryWindow(structured, base, client, true)
			if err != nil {
				t.Fatal(err)
			}
			want := test.want(base, client.Doc(string(cursor)))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("collection-group window mismatch\ngot:  %#v\nwant: %#v", got, want)
			}
		})
	}
}

func TestExecuteQueryWithDocumentIDCursorContinuesAcrossSpacePages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	parentKey := record.NewKeyWithParentAndID(record.NewKeyWithID("spaces", "space-a"), "ext", "datatug")
	wantParent := "projects/cursor-test/databases/(default)/documents/" + parentKey.String()
	fake := &cursorFirestoreServer{t: t, parent: wantParent, collection: "queryActivityPending",
		pageDocuments: [][]string{{"delivered-a", "delivered-b", "delivered-c"}, {"pending-d", "pending-e"}}}
	firestorepb.RegisterFirestoreServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	client, err := firestore.NewClient(ctx, "cursor-test", option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithGRPCDialOption(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	db := NewDatabase("(default)", client)
	collection := dal.NewCollectionRef(fake.collection, "", parentKey)
	queryPage := func(after dal.Cursor) []record.Record {
		t.Helper()
		builder := dal.From(collection).NewQuery().OrderBy(dal.Ascending(dal.DocumentID())).Limit(3)
		if after != "" {
			builder = builder.StartAfter(after)
		}
		query := builder.SelectIntoRecord(func() record.Record {
			return record.NewRecordWithIncompleteKey(fake.collection, reflect.String, new(cursorTestDocument))
		})
		rows, err := dal.ExecuteQueryAndReadAllToRecords(ctx, query, db)
		if err != nil {
			t.Fatalf("query page after %q: %v", after, err)
		}
		return rows
	}
	assertIDsAndParents := func(rows []record.Record, ids []string) {
		t.Helper()
		if len(rows) != len(ids) {
			t.Fatalf("page has %d rows, want %d", len(rows), len(ids))
		}
		for i, row := range rows {
			if got := row.Key().ID; got != ids[i] {
				t.Fatalf("row %d ID = %v, want %q", i, got, ids[i])
			}
			if got := row.Key().Parent().String(); got != parentKey.String() {
				t.Fatalf("row %d parent = %q, want %q", i, got, parentKey.String())
			}
		}
	}
	assertIDsAndParents(queryPage(""), []string{"delivered-a", "delivered-b", "delivered-c"})
	assertIDsAndParents(queryPage(dal.Cursor("delivered-c")), []string{"pending-d", "pending-e"})
	if fake.page != 2 {
		t.Fatalf("RunQuery calls = %d, want 2", fake.page)
	}
}

func TestApplyQueryWindowPreservesImmutableFirestoreClauses(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), "query-window-test", option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	base := client.Collection("states").Query
	query := func(from, after dal.Cursor) dal.StructuredQuery {
		builder := dal.From(dal.NewRootCollectionRef("states", "")).NewQuery().Offset(2).Limit(3).OrderBy(dal.Ascending(dal.DocumentID()))
		if from != "" {
			builder = builder.StartFrom(from)
		}
		if after != "" {
			builder = builder.StartAfter(after)
		}
		return builder.SelectKeysOnly(reflect.String)
	}
	for _, test := range []struct {
		name string
		got  firestore.Query
		want firestore.Query
	}{
		{name: "inclusive", got: mustApplyQueryWindow(t, query("state-2", ""), base, client, false), want: base.Limit(3).Offset(2).StartAt("state-2")},
		{name: "exclusive", got: mustApplyQueryWindow(t, query("", "state-2"), base, client, false), want: base.Limit(3).Offset(2).StartAfter("state-2")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if !reflect.DeepEqual(test.got, test.want) {
				t.Fatalf("Firestore window was not preserved\ngot:  %#v\nwant: %#v", test.got, test.want)
			}
		})
	}
}

func mustApplyQueryWindow(t *testing.T, q dal.StructuredQuery, base firestore.Query, client *firestore.Client, collectionGroup bool) firestore.Query {
	t.Helper()
	got, err := applyQueryWindow(q, base, client, collectionGroup)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
