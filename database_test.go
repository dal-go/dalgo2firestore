package dalgo2firestore

import (
	"testing"

	"cloud.google.com/go/firestore"
)

func TestNewDatabase(t *testing.T) {
	db := NewDatabase("test-db", &firestore.Client{})
	if db == nil {
		t.Fatalf("NewDatabase() returned nil")
	}
}
