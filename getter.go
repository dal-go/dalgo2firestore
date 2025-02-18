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
//	getFirestore         func(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.DocumentSnapshot, err error)
//	getAll      func(ctx context.Context, docRefs []*firestore.DocumentRef) (_ []*firestore.DocumentSnapshot, err error)
//}

//func newGetter(db database) getter {
//	return getter{
//		client:      db.client,
//		keyToDocRef: keyToDocRef,
//		getFirestore:         getFirestore,
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
	return get(ctx, record, db.client, getFirestore)
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
	if !docSnapshot.Exists() {
		key := record.Key()
		err = dal.NewErrNotFoundByKey(key, err)
		record.SetError(err)
		return nil // This is for GetMulti() to continue processing other records
	}
	record.SetError(nil) // !Important - we need to setFirestore error to nil before accessing record.Data()
	recData := record.Data()
	if err = dataTo(docSnapshot, recData); err != nil {
		if status.Code(err) == codes.NotFound {
			key := record.Key()
			err = dal.NewErrNotFoundByKey(key, err)
		}
		err = errors.Wrapf(err, "failed to marshal record data into a target of type %T", recData)
		record.SetError(err)
		return err
	}
	return nil
}

func (db database) GetMulti(ctx context.Context, records []dal.Record) error {
	logMultiRecords(ctx, "db.GetMulti", records)
	return db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return tx.GetMulti(ctx, records)
	})
}
