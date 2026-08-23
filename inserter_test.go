package dalgo2firestore

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/record"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// Test_insert_create_already_exists proves that a gRPC codes.AlreadyExists
// status from Create — what both docRef.Create and firestore.Transaction.Create
// return for a duplicate document — is classified as record.ErrRecordExists,
// so callers can identify it with record.IsAlreadyExists instead of treating
// every insert failure as a duplicate key.
func Test_insert_create_already_exists(t *testing.T) {
	withStubbedDocRef(t, func() {
		origCreate := createNonTransactional
		createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
			return nil, status.Error(codes.AlreadyExists, "document already exists")
		}
		defer func() { createNonTransactional = origCreate }()

		db := database{id: "t", client: &firestore.Client{}}
		key := record.NewKeyWithID("c", "1")
		rec := record.NewRecordWithData(key, testData{})
		_, err := insert(context.Background(), db, rec, createNonTransactional)
		if err == nil {
			t.Fatal("expected an error for a duplicate key")
		}
		if !record.IsAlreadyExists(err) {
			t.Fatalf("insert over an existing key: err = %v, want record.IsAlreadyExists(err) == true", err)
		}
	})
}

// Test_insert_create_error_is_not_misclassified_as_already_exists is the other
// direction: an ordinary create failure (not codes.AlreadyExists) must not
// satisfy record.IsAlreadyExists.
func Test_insert_create_error_is_not_misclassified_as_already_exists(t *testing.T) {
	withStubbedDocRef(t, func() {
		origCreate := createNonTransactional
		createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
			return nil, errors.New("create failed")
		}
		defer func() { createNonTransactional = origCreate }()

		db := database{id: "t", client: &firestore.Client{}}
		key := record.NewKeyWithID("c", "1")
		rec := record.NewRecordWithData(key, testData{})
		_, err := insert(context.Background(), db, rec, createNonTransactional)
		if err == nil {
			t.Fatal("expected an error")
		}
		if record.IsAlreadyExists(err) {
			t.Fatalf("an unrelated create failure incorrectly satisfies record.IsAlreadyExists: %v", err)
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
