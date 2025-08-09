package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"reflect"
	"testing"
)

func Test_idFromFirestoreDocRef_unsupported_kind(t *testing.T) {
	ref := &firestore.DocumentRef{ID: "123"}
	if _, err := idFromFirestoreDocRef(ref, reflect.Bool); err == nil {
		t.Fatalf("expected error for unsupported kind")
	}
}
