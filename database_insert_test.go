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

func Test_DB_Insert_honors_WithAdapterGeneratedID(t *testing.T) {
	createCalls := stubCreateNonTransactional(t, nil)

	origDebugf := Debugf
	Debugf = func(_ context.Context, _ string, _ ...interface{}) {}
	defer func() { Debugf = origDebugf }()

	db := database{id: "db", client: &firestore.Client{}}
	rec := newIncompleteRecord()
	if err := db.Insert(context.Background(), rec, dal.WithAdapterGeneratedID()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id, ok := rec.Key().ID.(string); !ok || len(id) != 20 {
		t.Fatalf("expected adapter generated 20-char ID, got: %v", rec.Key().ID)
	}
	if *createCalls != 1 {
		t.Fatalf("expected 1 create call, got %d", *createCalls)
	}
}

func Test_DB_Insert_honors_WithRandomStringKey(t *testing.T) {
	stubGetByDocRef(t, []error{errStubNotFound})
	createCalls := stubCreateNonTransactional(t, nil)

	db := database{id: "db", client: &firestore.Client{}}
	rec := newIncompleteRecord()
	if err := db.Insert(context.Background(), rec, dal.WithRandomStringKey(12, 5)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id, ok := rec.Key().ID.(string); !ok || len(id) != 12 {
		t.Fatalf("expected generated 12-char ID, got: %v", rec.Key().ID)
	}
	if *createCalls != 1 {
		t.Fatalf("expected 1 create call, got %d", *createCalls)
	}
}

func Test_DB_InsertMulti_honors_WithAdapterGeneratedID(t *testing.T) {
	createCalls := stubCreateNonTransactional(t, nil)

	origDebugf := Debugf
	Debugf = func(_ context.Context, _ string, _ ...interface{}) {}
	defer func() { Debugf = origDebugf }()

	db := database{id: "db", client: &firestore.Client{}}
	records := []dal.Record{newIncompleteRecord(), newIncompleteRecord()}
	if err := db.InsertMulti(context.Background(), records, dal.WithAdapterGeneratedID()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, rec := range records {
		if id, ok := rec.Key().ID.(string); !ok || len(id) != 20 {
			t.Fatalf("expected record %d to get adapter generated 20-char ID, got: %v", i, rec.Key().ID)
		}
	}
	if *createCalls != len(records) {
		t.Fatalf("expected %d create calls, got %d", len(records), *createCalls)
	}
}
