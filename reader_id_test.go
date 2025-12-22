package dalgo2firestore

import (
	"reflect"
	"testing"

	"cloud.google.com/go/firestore"
)

func Test_idFromFirestoreDocRef(t *testing.T) {
	refWith := func(id string) *firestore.DocumentRef { return &firestore.DocumentRef{ID: id} }

	t.Run("invalid_kind", func(t *testing.T) {
		_, err := idFromFirestoreDocRef(refWith("any"), reflect.Invalid)
		if err == nil {
			t.Fatalf("expected error for reflect.Invalid kind")
		}
	})

	t.Run("string_kind", func(t *testing.T) {
		got, err := idFromFirestoreDocRef(refWith("abc"), reflect.String)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "abc" {
			t.Fatalf("got %v, want %v", got, "abc")
		}
	})

	intKinds := []reflect.Kind{reflect.Int, reflect.Int32, reflect.Int64, reflect.Int16, reflect.Int8}
	for _, k := range intKinds {
		t.Run("int_kind_"+k.String(), func(t *testing.T) {
			got, err := idFromFirestoreDocRef(refWith("123"), k)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Note: implementation returns int for all integer kinds
			if _, ok := got.(int); !ok {
				t.Fatalf("expected int for kind %v, got %T", k, got)
			}
			if got != 123 {
				t.Fatalf("got %v, want %v", got, 123)
			}
		})
	}

	t.Run("non_numeric_for_int_kind", func(t *testing.T) {
		_, err := idFromFirestoreDocRef(refWith("abc"), reflect.Int)
		if err == nil {
			t.Fatalf("expected error when converting non-numeric to int")
		}
	})
}
