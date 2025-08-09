package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"testing"
)

func TestNewDatabase_panics(t *testing.T) {
	defer func() { _ = recover() }()
	// Panics on empty id
	func() { NewDatabase("", &firestore.Client{}) }()
	// Panics on nil client
	func() { NewDatabase("id", nil) }()
}

func TestDatabase_ID_Adapter_Schema(t *testing.T) {
	db := NewDatabase("db1", &firestore.Client{})
	if db.ID() != "db1" {
		t.Fatalf("unexpected ID: %v", db.ID())
	}
	ad := db.Adapter()
	if ad == nil || ad.Name() == "" {
		t.Fatalf("unexpected adapter: %#v", ad)
	}
	if db.Schema() != nil {
		t.Fatalf("expected nil schema")
	}
}
