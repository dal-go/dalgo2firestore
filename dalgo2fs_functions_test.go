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
	client := &firestore.Client{}
	rootColRef := dal.NewRootCollectionRef("c1", "id1")
	subColRef := dal.NewCollectionRef("c2", "id2", dal.NewKeyWithID("c1", "id1"))
	tests := []struct {
		name                string
		args                args
		wantFsCollectionRef *firestore.CollectionRef
		shouldPanic         bool
	}{
		{
			name: "nil_colRef",
			args: args{
				colRef: nil,
				client: client,
			},
			shouldPanic: true,
		},
		{
			name: "nil_client",
			args: args{
				colRef: &rootColRef,
				client: nil,
			},
			shouldPanic: true,
		},
		{
			name: "root_collection",
			args: args{
				colRef: &rootColRef,
				client: client,
			},
			wantFsCollectionRef: client.Collection("c1"),
		},
		{
			name: "sub_collection",
			args: args{
				colRef: &subColRef,
				client: client,
			},
			wantFsCollectionRef: client.Collection("c1/id1/c2"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tt.shouldPanic && r == nil {
					t.Errorf("GetFirestoreCollectionRef() should have panicked")
				} else if !tt.shouldPanic && r != nil {
					t.Errorf("GetFirestoreCollectionRef() panicked unexpectedly: %v", r)
				}
			}()
			if gotFsCollectionRef := GetFirestoreCollectionRef(tt.args.colRef, tt.args.client); !reflect.DeepEqual(gotFsCollectionRef, tt.wantFsCollectionRef) {
				t.Errorf("GetFirestoreCollectionRef() = %v, want %v", gotFsCollectionRef, tt.wantFsCollectionRef)
			}
		})
	}
}
