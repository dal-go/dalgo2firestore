package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"testing"
)

func TestNewDatabase(t *testing.T) {
	db := NewDatabase("test-db", &firestore.Client{})
	if db == nil {
		t.Fatalf("NewDatabase() returned nil")
	}
}
