package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"testing"
)

func TestNewDatabase(t *testing.T) {
	var dtb = NewDatabase("test-db", &firestore.Client{})
	if dtb == nil {
		t.Error("NewDatabase returned nil")
	}
}
