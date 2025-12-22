package dalgo2firestore

import (
	"context"
	"testing"

	"cloud.google.com/go/firestore"

	"github.com/dal-go/dalgo/dal"
)

func Test_database_Delete_calls_deleteByDocRef(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origDelete := deleteByDocRef
	defer func() { keyToDocRef = origKeyToDocRef; deleteByDocRef = origDelete }()

	var gotDocRef *firestore.DocumentRef
	keyToDocRef = func(_ *dal.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "abc"}
	}
	deleteByDocRef = func(ctx context.Context, docRef *firestore.DocumentRef) (*firestore.WriteResult, error) {
		gotDocRef = docRef
		return &firestore.WriteResult{}, nil
	}

	db := database{id: "t", client: &firestore.Client{}}
	key := dal.NewKeyWithID("coll", "1")
	if err := db.Delete(context.Background(), key); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotDocRef == nil || gotDocRef.ID != "abc" {
		t.Fatalf("deleteByDocRef was not called with expected docRef, got: %#v", gotDocRef)
	}
}
