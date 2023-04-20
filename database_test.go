package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"testing"
)

func TestNewDatabase(t *testing.T) {
	_ = NewDatabase("test-db", &firestore.Client{})
}
