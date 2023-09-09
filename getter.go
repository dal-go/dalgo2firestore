package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//type getter struct {
//	client      *firestore.Client
//	keyToDocRef keyToDocRefFunc
//	dataTo      func(ds *firestore.DocumentSnapshot, p interface{}) error
//	get         func(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.DocumentSnapshot, err error)
//	getAll      func(ctx context.Context, docRefs []*firestore.DocumentRef) (_ []*firestore.DocumentSnapshot, err error)
//}

//func newGetter(db database) getter {
//	return getter{
//		client:      db.client,
//		keyToDocRef: keyToDocRef,
//		get:         get,
//		getAll:      db.client.GetAll,
//		dataTo: func(ds *firestore.DocumentSnapshot, p interface{}) error {
//			return ds.DataTo(p)
//		},
//	}
//}

var dataTo = func(ds *firestore.DocumentSnapshot, p interface{}) error {
	return ds.DataTo(p)
}

func (db database) Get(ctx context.Context, record dal.Record) error {
	key := record.Key()
	docRef := db.keyToDocRef(key)
	docSnapshot, err := get(ctx, docRef)
	return docSnapshotToRecord(err, docSnapshot, record, dataTo)
}

func docSnapshotToRecord(
	err error,
	docSnapshot *firestore.DocumentSnapshot,
	record dal.Record,
	dataTo func(ds *firestore.DocumentSnapshot, p interface{}) error,
) error {
	if err != nil {
		if status.Code(err) == codes.NotFound {
			err = dal.NewErrNotFoundByKey(record.Key(), err)
		}
		record.SetError(err)
		return err
	}
	record.SetError(nil)
	recData := record.Data()
	err = dataTo(docSnapshot, recData)
	if status.Code(err) == codes.NotFound {
		err = dal.NewErrNotFoundByKey(record.Key(), err)
		record.SetError(err)
		return err
	}
	if err != nil {
		err = errors.Wrapf(err, "failed to marshal record data into a target of type %T", recData)
	}
	record.SetError(err)
	return nil
}

func (db database) GetMulti(ctx context.Context, records []dal.Record) error {
	return db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return tx.GetMulti(ctx, records)
	})
}
