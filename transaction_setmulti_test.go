package dalgo2firestore

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/firestore"
	dalrecord "github.com/dal-go/record"
)

func TestTransactionSetMulti_QueuesWritesAndPropagatesError(t *testing.T) {
	originalKeyToDocRef := keyToDocRef
	originalSet := setInFirestoreTransaction
	defer func() {
		keyToDocRef = originalKeyToDocRef
		setInFirestoreTransaction = originalSet
	}()

	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "contact"}
	}
	writeErr := errors.New("transaction write failed")
	calls := 0
	setInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef, _ interface{}) error {
		calls++
		return writeErr
	}

	record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("contacts", "contact"), &struct{ Name string }{Name: "Andrey"})
	tx := transaction{db: database{client: &firestore.Client{}}}
	err := tx.SetMulti(context.Background(), []dalrecord.Record{record})

	if !errors.Is(err, writeErr) {
		t.Fatalf("SetMulti() error = %v, want %v", err, writeErr)
	}
	if calls != 1 {
		t.Fatalf("transaction write calls = %d, want 1", calls)
	}
}
