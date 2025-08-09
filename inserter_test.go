package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

type testData struct {
	ValidErr   error
	WithKeyErr error
}

func (t testData) Validate() error                  { return t.ValidErr }
func (t testData) ValidateWithKey(_ *dal.Key) error { return t.WithKeyErr }

func withStubbedDocRef(t *testing.T, fn func()) {
	t.Helper()
	origKeyToDocRef := keyToDocRef
	keyToDocRef = func(_ *dal.Key, _ *firestore.Client) *firestore.DocumentRef {
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
		key := dal.NewKeyWithID("c", "1")
		rec := dal.NewRecordWithData(key, testData{})
		if _, err := insert(context.Background(), db, rec, createNonTransactional); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func Test_insert_validate_error(t *testing.T) {
	withStubbedDocRef(t, func() {
		origCreate := createNonTransactional
		createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
			return &firestore.WriteResult{}, nil
		}
		defer func() { createNonTransactional = origCreate }()

		db := database{id: "t", client: &firestore.Client{}}
		key := dal.NewKeyWithID("c", "1")
		rec := dal.NewRecordWithData(key, testData{ValidErr: errors.New("bad")})
		if _, err := insert(context.Background(), db, rec, createNonTransactional); err == nil {
			t.Fatalf("expected validation error")
		}
	})
}

func Test_insert_validate_with_key_error(t *testing.T) {
	withStubbedDocRef(t, func() {
		origCreate := createNonTransactional
		createNonTransactional = func(ctx context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
			return &firestore.WriteResult{}, nil
		}
		defer func() { createNonTransactional = origCreate }()

		db := database{id: "t", client: &firestore.Client{}}
		key := dal.NewKeyWithID("c", "1")
		rec := dal.NewRecordWithData(key, testData{WithKeyErr: errors.New("bad-with-key")})
		if _, err := insert(context.Background(), db, rec, createNonTransactional); err == nil {
			t.Fatalf("expected validate with key error")
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
		key := dal.NewKeyWithID("c", "1")
		rec := dal.NewRecordWithData(key, testData{})
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
		records := []dal.Record{
			dal.NewRecordWithData(dal.NewKeyWithID("c", "1"), testData{}),
			dal.NewRecordWithData(dal.NewKeyWithID("c", "2"), testData{}),
		}
		if _, err := insertMulti(context.Background(), db, records, createNonTransactional); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != len(records) {
			t.Fatalf("expected %d creates, got %d", len(records), count)
		}
	})
}
