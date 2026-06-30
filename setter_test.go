package dalgo2firestore

import (
	"context"
	"testing"

	"cloud.google.com/go/firestore"

	"github.com/dal-go/dalgo/dal"
)

// Test_DB_Set_does_not_panic_on_fresh_record is the regression for the bug where
// database.Set called record.Data() on a freshly built NewRecordWithData record
// (whose error is still nil), and record.Data() panics with "an attempt to
// access record data before it was retrieved from database and SetError(error)
// called". Set must mark the record error first (like the insert() path does),
// so writing a brand-new record succeeds. This broke every eventus store write
// (e.g. create-event) against Firestore.
func Test_DB_Set_does_not_panic_on_fresh_record(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origSet := setFirestore
	defer func() { keyToDocRef = origKeyToDocRef; setFirestore = origSet }()

	keyToDocRef = func(_ *dal.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "x"}
	}
	called := 0
	setFirestore = func(_ context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
		called++
		return &firestore.WriteResult{}, nil
	}

	db := database{id: "db", client: &firestore.Client{}}
	// A freshly built record: NewRecordWithData leaves the record error nil.
	rec := dal.NewRecordWithData(dal.NewKeyWithID("c", "1"), &struct{ V string }{V: "v"})
	if err := db.Set(context.Background(), rec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected setFirestore to be called once, got %d", called)
	}
}
