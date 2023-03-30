package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/strongo/dalgo/dal"
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
		doc: func(key *dal.Key) *firestore.DocumentRef {
			return nil
		},
		delete: dm.delete,
	}
	return &dm
}

func TestDeleter_Delete(t *testing.T) {
	deleterMock := newDeleterMock()
	ctx := context.Background()
	key := dal.NewKeyWithID("TestKind", "test-id")
	err := deleterMock.deleter.Delete(ctx, key)
	if err != nil {
		t.Errorf("expected to be successful, got error: %v", err)
	}
	if deleterMock.deleteCalled != 1 {
		t.Errorf("expected a single call to delete, got %v", deleterMock.deleteCalled)
	}
}
