package dalgo_firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/strongo/dalgo"
	"testing"
)

type deleterMock struct {
	deleteCalled int
	deleter      deleter
}

func (dm *deleterMock) delete(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.WriteResult, err error) {
	dm.deleteCalled++
	return nil, nil
}

func newDeleterMock() *deleterMock {
	var dm deleterMock
	dm.deleter = deleter{
		doc: func(key dalgo.RecordKey) *firestore.DocumentRef {
			return nil
		},
		delete: dm.delete,
	}
	return &dm
}

func TestDeleter_Delete(t *testing.T) {
	deleterMock := newDeleterMock()
	ctx := context.Background()
	key := dalgo.NewRecordKey(dalgo.RecordRef{Kind: "TestKind", ID: "test-id"})
	err := deleterMock.deleter.Delete(ctx, key)
	if err != nil {
		t.Errorf("expected to be successful, got error: %v", err)
	}
	if deleterMock.deleteCalled != 1 {
		t.Errorf("expected a single call to delete, got %v", deleterMock.deleteCalled)
	}
}