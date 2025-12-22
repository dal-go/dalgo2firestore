package dalgo2firestore

import (
	"context"
	"testing"

	"cloud.google.com/go/firestore"

	"github.com/dal-go/dalgo/dal"
)

func Test_DB_Insert_uses_createNonTransactional(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origCreate := createNonTransactional
	defer func() { keyToDocRef = origKeyToDocRef; createNonTransactional = origCreate }()

	keyToDocRef = func(_ *dal.Key, _ *firestore.Client) *firestore.DocumentRef { return &firestore.DocumentRef{ID: "x"} }
	called := 0
	createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
		called++
		return &firestore.WriteResult{}, nil
	}

	db := database{id: "db", client: &firestore.Client{}}
	rec := dal.NewRecordWithData(dal.NewKeyWithID("c", "1"), struct{}{})
	if err := db.Insert(context.Background(), rec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected createNonTransactional to be called once, got %d", called)
	}
}

func Test_DB_InsertMulti_uses_createNonTransactional(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origCreate := createNonTransactional
	defer func() { keyToDocRef = origKeyToDocRef; createNonTransactional = origCreate }()

	keyToDocRef = func(_ *dal.Key, _ *firestore.Client) *firestore.DocumentRef { return &firestore.DocumentRef{ID: "x"} }
	called := 0
	createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
		called++
		return &firestore.WriteResult{}, nil
	}

	db := database{id: "db", client: &firestore.Client{}}
	records := []dal.Record{
		dal.NewRecordWithData(dal.NewKeyWithID("c", "1"), struct{}{}),
		dal.NewRecordWithData(dal.NewKeyWithID("c", "2"), struct{}{}),
	}
	if err := db.InsertMulti(context.Background(), records); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called != len(records) {
		t.Fatalf("expected %d calls, got %d", len(records), called)
	}
}
