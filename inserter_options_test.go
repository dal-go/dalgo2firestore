package dalgo2firestore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errStubNotFound = status.Error(codes.NotFound, "stub: document not found")

// stubGetByDocRef replaces getByDocRef so that each call returns the next error
// from results (nil means "document exists"). The last result is repeated.
func stubGetByDocRef(t *testing.T, results []error) (calls *int) {
	t.Helper()
	origGetByDocRef := getByDocRef
	calls = new(int)
	getByDocRef = func(_ context.Context, _ *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		i := *calls
		if i >= len(results) {
			i = len(results) - 1
		}
		*calls++
		return nil, results[i]
	}
	t.Cleanup(func() { getByDocRef = origGetByDocRef })
	return calls
}

func stubCreateNonTransactional(t *testing.T, err error) (calls *int) {
	t.Helper()
	origCreate := createNonTransactional
	calls = new(int)
	createNonTransactional = func(_ context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
		*calls++
		return &firestore.WriteResult{}, err
	}
	t.Cleanup(func() { createNonTransactional = origCreate })
	return calls
}

func newIncompleteRecord() dal.Record {
	return dal.NewRecordWithData(dal.NewIncompleteKey("c", reflect.String, nil), &testData{})
}

func Test_insertWithOptions_with_id_generator_success_first_attempt(t *testing.T) {
	getCalls := stubGetByDocRef(t, []error{errStubNotFound})
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	record := newIncompleteRecord()
	options := dal.NewInsertOptions(dal.WithRandomStringKey(10, 5))

	if err := insertWithOptions(context.Background(), db, record, createNonTransactional, options); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id, ok := record.Key().ID.(string)
	if !ok || len(id) != 10 {
		t.Fatalf("expected generated string ID of length 10, got: %v", record.Key().ID)
	}
	if *getCalls != 1 {
		t.Fatalf("expected 1 existence check, got %d", *getCalls)
	}
	if *createCalls != 1 {
		t.Fatalf("expected 1 create call, got %d", *createCalls)
	}
}

func Test_insertWithOptions_with_id_generator_retries_on_taken_id(t *testing.T) {
	// First 2 generated IDs are already taken, 3rd is free.
	getCalls := stubGetByDocRef(t, []error{nil, nil, errStubNotFound})
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	record := newIncompleteRecord()
	options := dal.NewInsertOptions(dal.WithRandomStringKey(10, 100))

	if err := insertWithOptions(context.Background(), db, record, createNonTransactional, options); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *getCalls != 3 {
		t.Fatalf("expected 3 existence checks, got %d", *getCalls)
	}
	if *createCalls != 1 {
		t.Fatalf("expected 1 create call, got %d", *createCalls)
	}
}

func Test_insertWithOptions_with_id_generator_exhausts_attempts(t *testing.T) {
	getCalls := stubGetByDocRef(t, []error{nil}) // every generated ID is already taken
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	record := newIncompleteRecord()
	options := dal.NewInsertOptions(dal.WithRandomStringKey(10, 100))

	err := insertWithOptions(context.Background(), db, record, createNonTransactional, options)
	if !errors.Is(err, dal.ErrExceedsMaxNumberOfAttempts) {
		t.Fatalf("expected ErrExceedsMaxNumberOfAttempts, got: %v", err)
	}
	if *getCalls != maxIDGenerationAttempts {
		t.Fatalf("expected %d existence checks, got %d", maxIDGenerationAttempts, *getCalls)
	}
	if *createCalls != 0 {
		t.Fatalf("expected no create calls, got %d", *createCalls)
	}
}

func Test_insertWithOptions_with_id_generator_existence_check_error(t *testing.T) {
	existsErr := errors.New("stub: failed to check existence")
	stubGetByDocRef(t, []error{existsErr})
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	record := newIncompleteRecord()
	options := dal.NewInsertOptions(dal.WithRandomStringKey(10, 5))

	err := insertWithOptions(context.Background(), db, record, createNonTransactional, options)
	if !errors.Is(err, existsErr) {
		t.Fatalf("expected error wrapping existence check error, got: %v", err)
	}
	if *createCalls != 0 {
		t.Fatalf("expected no create calls, got %d", *createCalls)
	}
}

func Test_insertWithOptions_with_adapter_generated_id_for_incomplete_key(t *testing.T) {
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	record := newIncompleteRecord()
	options := dal.NewInsertOptions(dal.WithAdapterGeneratedID())

	if err := insertWithOptions(context.Background(), db, record, createNonTransactional, options); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id, ok := record.Key().ID.(string)
	if !ok || len(id) != 20 {
		t.Fatalf("expected Firestore native auto-generated 20-char ID, got: %v", record.Key().ID)
	}
	if *createCalls != 1 {
		t.Fatalf("expected 1 create call, got %d", *createCalls)
	}
}

func Test_insertWithOptions_with_adapter_generated_id_keeps_complete_key(t *testing.T) {
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	record := dal.NewRecordWithData(dal.NewKeyWithID("c", "fixed-id"), &testData{})
	options := dal.NewInsertOptions(dal.WithAdapterGeneratedID())

	if err := insertWithOptions(context.Background(), db, record, createNonTransactional, options); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record.Key().ID != "fixed-id" {
		t.Fatalf("expected key ID to stay unchanged, got: %v", record.Key().ID)
	}
	if *createCalls != 1 {
		t.Fatalf("expected 1 create call, got %d", *createCalls)
	}
}

func Test_insertWithOptions_without_options_keeps_key_as_is(t *testing.T) {
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	record := dal.NewRecordWithData(dal.NewKeyWithID("c", "explicit-id"), &testData{})

	if err := insertWithOptions(context.Background(), db, record, createNonTransactional, dal.NewInsertOptions()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record.Key().ID != "explicit-id" {
		t.Fatalf("expected key ID to stay unchanged, got: %v", record.Key().ID)
	}
	if *createCalls != 1 {
		t.Fatalf("expected 1 create call, got %d", *createCalls)
	}
}

func Test_insertMulti_with_adapter_generated_id(t *testing.T) {
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	records := []dal.Record{newIncompleteRecord(), newIncompleteRecord()}

	if err := insertMulti(context.Background(), db, records, createNonTransactional, dal.WithAdapterGeneratedID()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	id1, _ := records[0].Key().ID.(string)
	id2, _ := records[1].Key().ID.(string)
	if len(id1) != 20 || len(id2) != 20 {
		t.Fatalf("expected both records to get 20-char IDs, got: %q, %q", id1, id2)
	}
	if id1 == id2 {
		t.Fatalf("expected unique IDs, got the same: %q", id1)
	}
	if *createCalls != len(records) {
		t.Fatalf("expected %d create calls, got %d", len(records), *createCalls)
	}
}

func Test_insertMulti_with_id_generator(t *testing.T) {
	stubGetByDocRef(t, []error{errStubNotFound})
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "t", client: &firestore.Client{}}
	records := []dal.Record{newIncompleteRecord(), newIncompleteRecord()}

	if err := insertMulti(context.Background(), db, records, createNonTransactional, dal.WithRandomStringKey(8, 5)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, record := range records {
		if id, ok := record.Key().ID.(string); !ok || len(id) != 8 {
			t.Fatalf("expected record %d to get generated 8-char ID, got: %v", i, record.Key().ID)
		}
	}
	if *createCalls != len(records) {
		t.Fatalf("expected %d create calls, got %d", len(records), *createCalls)
	}
}

func Test_insertMulti_returns_error_with_record_index(t *testing.T) {
	createErr := errors.New("stub: create failed")
	stubCreateNonTransactional(t, createErr)

	db := database{id: "t", client: &firestore.Client{}}
	records := []dal.Record{
		dal.NewRecordWithData(dal.NewKeyWithID("c", "1"), &testData{}),
	}

	err := insertMulti(context.Background(), db, records, createNonTransactional)
	if !errors.Is(err, createErr) {
		t.Fatalf("expected error wrapping create error, got: %v", err)
	}
}
