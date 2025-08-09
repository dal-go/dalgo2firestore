package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"fmt"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
)

type mockUpdate struct {
	name string
	path []string
	val  any
}

func (m mockUpdate) FieldName() string           { return m.name }
func (m mockUpdate) FieldPath() update.FieldPath { return update.FieldPath(m.path) }
func (m mockUpdate) Value() any                  { return m.val }

func Test_getFirestoreUpdate_Delete(t *testing.T) {
	u := update.DeleteByFieldName("toDelete")
	fsu, err := getFirestoreUpdate(u)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fsu.Path != "toDelete" {
		t.Fatalf("unexpected Path: %q", fsu.Path)
	}
	if fsu.Value != firestore.Delete {
		t.Fatalf("expected firestore.Delete, got: %v", fsu.Value)
	}
}

func Test_getFirestoreUpdate_TransformIncrement(t *testing.T) {
	u := update.ByFieldName("count", dal.Increment(2))
	fsu, err := getFirestoreUpdate(u)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fsu.Path != "count" {
		t.Fatalf("unexpected Path: %q", fsu.Path)
	}
	// Value is an internal firestore.transform type. Check type string for safety.
	if typeName := fmt.Sprintf("%T", fsu.Value); typeName != "firestore.transform" {
		// In recent versions the concrete type string format is "firestore.transform".
		// We allow prefix match to be resilient across minor versions.
		if typeName != "cloud.google.com/go/firestore.transform" {
			t.Fatalf("expected firestore.transform, got: %s", typeName)
		}
	}
}

func Test_getFirestoreUpdate_InvalidFieldNameWithDot(t *testing.T) {
	u := mockUpdate{name: "a.b", val: 1}
	_, err := getFirestoreUpdate(u)
	if err == nil {
		t.Fatalf("expected error for dotted field name")
	}
}

func Test_getFirestoreUpdate_EmptyPathAndFieldPath(t *testing.T) {
	u := mockUpdate{name: "", path: nil, val: 1}
	_, err := getFirestoreUpdate(u)
	if err == nil {
		t.Fatalf("expected error for empty Path and FieldPath")
	}
}

func Test_getFirestoreUpdate_FieldPathWithEmptyComponent(t *testing.T) {
	u := mockUpdate{path: []string{"a", ""}, val: 1}
	_, err := getFirestoreUpdate(u)
	if err == nil {
		t.Fatalf("expected error for empty component in FieldPath")
	}
}

func Test_getFirestoreUpdates_EmptySlice(t *testing.T) {
	_, err := getFirestoreUpdates(nil)
	if err == nil {
		t.Fatalf("expected error for empty updates slice")
	}
}

func Test_getUpdatePreconditions_Exists(t *testing.T) {
	fs := getUpdatePreconditions([]dal.Precondition{dal.WithExistsPrecondition()})
	if len(fs) != 1 || fs[0] != firestore.Exists {
		t.Fatalf("expected single firestore.Exists precondition, got: %+v", fs)
	}
}
