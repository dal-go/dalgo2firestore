package dalgo2firestore

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/record"
)

// testData is a plain payload with no validation of its own: record
// invariants are now the framework's job (dal.BeforeSave, run by the pipeline
// dal.NewDB returns), not this adapter's. See TestConformance for the
// behavioural proof that Insert/Set/UpdateRecord are all covered.
type testData struct{}

func withStubbedDocRef(t *testing.T, fn func()) {
	t.Helper()
	origKeyToDocRef := keyToDocRef
	keyToDocRef = func(_ *record.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "test"}
	}
	defer func() { keyToDocRef = origKeyToDocRef }()
	fn()
}

func Test_insert_success(t *testing.T) {
	withStubbedDocRef(t, func() {
		origCreate := createNonTransactional
		createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
			return &firestore.WriteResult{}, nil
		}
		defer func() { createNonTransactional = origCreate }()

		db := database{id: "t", client: &firestore.Client{}}
		key := record.NewKeyWithID("c", "1")
		rec := record.NewRecordWithData(key, testData{})
		if _, err := insert(context.Background(), db, rec, createNonTransactional); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func Test_insert_create_error(t *testing.T) {
	withStubbedDocRef(t, func() {
		origCreate := createNonTransactional
		createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
			return nil, errors.New("create failed")
		}
		defer func() { createNonTransactional = origCreate }()

		db := database{id: "t", client: &firestore.Client{}}
		key := record.NewKeyWithID("c", "1")
		rec := record.NewRecordWithData(key, testData{})
		if _, err := insert(context.Background(), db, rec, createNonTransactional); err == nil {
			t.Fatalf("expected create error")
		}
	})
}

func Test_insertMulti_basic(t *testing.T) {
	withStubbedDocRef(t, func() {
		origCreate := createNonTransactional
		count := 0
		createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
			count++
			return &firestore.WriteResult{}, nil
		}
		defer func() { createNonTransactional = origCreate }()

		db := database{id: "t", client: &firestore.Client{}}
		records := []record.Record{
			record.NewRecordWithData(record.NewKeyWithID("c", "1"), testData{}),
			record.NewRecordWithData(record.NewKeyWithID("c", "2"), testData{}),
		}
		if err := insertMulti(context.Background(), db, records, createNonTransactional); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != len(records) {
			t.Fatalf("expected %d creates, got %d", len(records), count)
		}
	})
}
