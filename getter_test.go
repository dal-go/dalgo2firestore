package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/strongo/dalgo/dal"
	"testing"
)

type getterMock struct {
	getCalled int
	getter    getter
}

func newGetterMock() *getterMock {
	var gm getterMock
	gm.getter = getter{
		doc: func(key *dal.Key) *firestore.DocumentRef {
			return nil
		},
		get: func(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.DocumentSnapshot, err error) {
			gm.getCalled++
			return nil, err
		},
		dataTo: func(ds *firestore.DocumentSnapshot, p interface{}) error {
			return nil
		},
	}
	return &gm
}

type testKind struct {
	Str string
	Int int
}

func TestGetter_Get(t *testing.T) {
	gm := newGetterMock()
	ctx := context.Background()
	key := dal.NewKeyWithID("TestKind", "TestID")
	data := new(testKind)
	record := dal.NewRecordWithData(key, data)
	err := gm.getter.Get(ctx, record)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
