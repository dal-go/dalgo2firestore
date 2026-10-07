package dalgo2firestore

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"google.golang.org/api/option"
)

func newReferenceOnlyFirestoreClient(t *testing.T) *firestore.Client {
	t.Helper()
	client, err := firestore.NewClient(context.Background(), "test-project", option.WithoutAuthentication(), option.WithEndpoint("127.0.0.1:8080"))
	if err != nil {
		t.Fatalf("create reference-only Firestore client: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Firestore client: %v", err)
		}
	})
	return client
}

func TestFirestoreReaderNextUsesActualCollectionGroupDocumentPath(t *testing.T) {
	client := newReferenceOnlyFirestoreClient(t)
	refs := []*firestore.DocumentRef{
		client.Doc("spaces/space-a/projects/project-a/queries/query-a"),
		client.Doc("spaces/space-b/projects/project-b/queries/query-b"),
		// Creating a child reference does not require its parent document to exist.
		client.Doc("spaces/space-c/projects/deleted-project/archives/deleted-archive/queries/query-c"),
	}
	index := 0
	originalNext := docIteratorNext
	docIteratorNext = func(_ *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
		ref := refs[index]
		index++
		return &firestore.DocumentSnapshot{Ref: ref}, nil
	}
	t.Cleanup(func() { docIteratorNext = originalNext })

	query := dal.NewQueryBuilder(dal.From(dal.NewCollectionGroupRef("queries", ""))).SelectKeysOnly(reflect.String)
	reader := &firestoreReader{query: query}
	wantDepths := []int{2, 2, 3}
	for i, want := range refs {
		got, err := reader.Next()
		if err != nil {
			t.Fatalf("Next() #%d: %v", i, err)
		}
		if got.Key().String() != want.Path[strings.Index(want.Path, "/documents/")+len("/documents/"):] {
			t.Fatalf("Next() #%d key = %q, want Firestore path %q", i, got.Key(), want.Path)
		}
		if got.Key().Level() != wantDepths[i] {
			t.Fatalf("Next() #%d parent depth = %d, want %d", i, got.Key().Level(), wantDepths[i])
		}
	}
}

func TestKeyFromFirestoreDocumentRefPreservesMatchingAncestorKinds(t *testing.T) {
	client := newReferenceOnlyFirestoreClient(t)
	ref := client.Doc("spaces/42/projects/project-a/queries/query-a")
	prototype := dalrecord.NewKeyWithParentAndID(
		dalrecord.NewKeyWithParentAndID(
			dalrecord.NewKeyWithID("spaces", int64(42)),
			"projects",
			"project-a",
		),
		"queries",
		"",
	)
	prototype.IDKind = reflect.String

	got, err := keyFromFirestoreDocumentRef(ref, prototype)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "spaces/42/projects/project-a/queries/query-a" {
		t.Fatalf("key = %q", got)
	}
	if reflect.TypeOf(got.Parent().Parent().ID).Kind() != reflect.Int64 {
		t.Fatalf("space ID kind = %v, want int64", reflect.TypeOf(got.Parent().Parent().ID).Kind())
	}
	if got.Parent().Parent().ID != int64(42) {
		t.Fatalf("space ID = %#v, want int64(42)", got.Parent().Parent().ID)
	}
}

func TestKeyFromFirestoreDocumentRefRejectsIncompleteOrMismatchedInputs(t *testing.T) {
	client := newReferenceOnlyFirestoreClient(t)
	validRef := client.Doc("spaces/space-a/queries/query-a")
	validPrototype := dalrecord.NewRecordWithIncompleteKey("queries", reflect.String, nil).Key()

	tests := []struct {
		name      string
		ref       *firestore.DocumentRef
		prototype *dalrecord.Key
	}{{
		name:      "nil reference",
		prototype: validPrototype,
	}, {
		name: "nil prototype",
		ref:  validRef,
	}, {
		name:      "missing reference parent",
		ref:       &firestore.DocumentRef{ID: "query-a"},
		prototype: validPrototype,
	}, {
		name:      "wrong collection",
		ref:       client.Doc("spaces/space-a/other/query-a"),
		prototype: validPrototype,
	}, {
		name:      "invalid leaf ID kind",
		ref:       validRef,
		prototype: dalrecord.NewKeyWithID("queries", ""),
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := keyFromFirestoreDocumentRef(test.ref, test.prototype); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	if _, err := keyFromFirestoreDocumentRef(validRef, validPrototype); err != nil {
		t.Fatalf("valid input unexpectedly failed: %v", err)
	}
}

func TestFirestoreReaderNextReturnsKeyConstructionError(t *testing.T) {
	originalNext := docIteratorNext
	docIteratorNext = func(_ *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
		return &firestore.DocumentSnapshot{Ref: &firestore.DocumentRef{ID: "query-a"}}, nil
	}
	t.Cleanup(func() { docIteratorNext = originalNext })

	query := dal.NewQueryBuilder(dal.From(dal.NewCollectionGroupRef("queries", ""))).SelectKeysOnly(reflect.String)
	reader := &firestoreReader{query: query}
	if _, err := reader.Next(); err == nil {
		t.Fatal("expected key construction error")
	}
}

func TestFirestoreReaderNextPreservesDataWrapper(t *testing.T) {
	client := newReferenceOnlyFirestoreClient(t)
	ref := client.Doc("spaces/space-a/projects/project-a/queries/query-a")
	fixture := fakeDocumentSnapshot{
		Ref: ref,
		c:   client,
		proto: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
			"title": {ValueType: &firestorepb.Value_StringValue{StringValue: "loaded"}},
		}},
	}
	originalNext := docIteratorNext
	docIteratorNext = func(_ *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
		return (*firestore.DocumentSnapshot)(unsafe.Pointer(&fixture)), nil
	}
	t.Cleanup(func() { docIteratorNext = originalNext })

	data := map[string]any{}
	query := dal.NewQueryBuilder(dal.From(dal.NewCollectionGroupRef("queries", ""))).
		SelectIntoRecord(func() dalrecord.Record {
			record := dalrecord.NewRecordWithIncompleteKey("queries", reflect.String, mockDataWrapper{data: data})
			record.MarkAsChanged()
			return record
		})
	reader := &firestoreReader{query: query}
	got, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got.Key().String() != "spaces/space-a/projects/project-a/queries/query-a" {
		t.Fatalf("key = %q", got.Key())
	}
	if !got.HasChanged() {
		t.Fatal("record change state was not preserved")
	}
	wrapper, ok := got.Data().(mockDataWrapper)
	if !ok {
		t.Fatalf("data type = %T, want mockDataWrapper", got.Data())
	}
	if wrapper.Data().(map[string]any)["title"] != "loaded" {
		t.Fatalf("wrapped data = %#v", wrapper.Data())
	}
}
