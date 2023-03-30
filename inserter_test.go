package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/strongo/dalgo/dal"
	"testing"
)

type inserterMock struct {
	createCalled int
	inserter     inserter
}

func newInserterMock() *inserterMock {
	im := inserterMock{}
	im.inserter = inserter{
		doc: func(key *dal.Key) *firestore.DocumentRef {
			return nil
		},
		create: func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (_ *firestore.WriteResult, err error) {
			im.createCalled++
			return nil, nil
		},
	}
	return &im
}
func TestInserter_Insert(t *testing.T) {
	inserterMock := newInserterMock()
	ctx := context.Background()
	key := dal.NewKeyWithID("TestKind", "test-id")
	data := new(testKind)
	record := dal.NewRecordWithData(key, data)
	err := inserterMock.inserter.Insert(ctx, record)
	if err != nil {
		t.Errorf("expected to be successful, got error: %v", err)
	}
	if inserterMock.createCalled != 1 {
		t.Errorf("expected a single call to create(), got %v", inserterMock.createCalled)
	}
}
