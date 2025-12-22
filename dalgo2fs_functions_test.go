package dalgo2firestore

import (
	"reflect"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
)

func TestGetFirestoreCollectionRef(t *testing.T) {
	type args struct {
		colRef *dal.CollectionRef
		client *firestore.Client
	}
	tests := []struct {
		name                string
		args                args
		wantFsCollectionRef *firestore.CollectionRef
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotFsCollectionRef := GetFirestoreCollectionRef(tt.args.colRef, tt.args.client); !reflect.DeepEqual(gotFsCollectionRef, tt.wantFsCollectionRef) {
				t.Errorf("GetFirestoreCollectionRef() = %v, want %v", gotFsCollectionRef, tt.wantFsCollectionRef)
			}
		})
	}
}
