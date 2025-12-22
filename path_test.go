package dalgo2firestore

import (
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestPathFromKey(t *testing.T) {
	k := dal.NewKeyWithID("users", "u1")
	got := PathFromKey(k)
	want := k.String()
	if got != want {
		t.Fatalf("PathFromKey() = %v, want %v", got, want)
	}
	// Sanity: ensure format is collection/id
	if want != "users/u1" {
		// In case dalgo.Key.String format changes, this expectation may need to be updated
		t.Fatalf("unexpected key.String() format: %v (idKind=%v)", want, reflect.Invalid)
	}
}
