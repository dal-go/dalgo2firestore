package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
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

func getByKey(
	ctx context.Context,
	key *dal.Key,
	client *firestore.Client,
	getByDocRef func(ctx context.Context, dr *firestore.DocumentRef) (*firestore.DocumentSnapshot, error),
) (
	docSnapshot *firestore.DocumentSnapshot, err error,
) {
	docRef := keyToDocRef(key, client)
	if docSnapshot, err = getByDocRef(ctx, docRef); err != nil {
		err = handleGetByKeyError(key, err)
	}
	return
}

func existsByKey(
	ctx context.Context,
	key *dal.Key,
	client *firestore.Client,
	getByDocRef func(ctx context.Context, dr *firestore.DocumentRef) (*firestore.DocumentSnapshot, error),
) (
	exists bool, err error,
) {
	_, err = getByKey(ctx, key, client, getByDocRef)
	exists = err == nil
	return
}

func getAndUnmarshal(
	ctx context.Context,
	record dal.Record,
	client *firestore.Client,
	getByDocRef func(ctx context.Context, dr *firestore.DocumentRef) (*firestore.DocumentSnapshot, error),
) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	key := record.Key()
	var docSnapshot *firestore.DocumentSnapshot

	if docSnapshot, err = getByKey(ctx, key, client, getByDocRef); err != nil {
		if err = handleGetByKeyError(key, err); err != nil {
			record.SetError(err)
		}
	} else {
		if err = docSnapshotToRecord(docSnapshot, record, dataTo); err != nil {
			//Do not set error on record to prevent accidental access to data?
			record.SetError(err)
		}
	}
	if Debugf != nil {
		Debugf(ctx, "getAndUnmarshal(%v) completed in %v, err: %v", key, time.Since(started), err)
	}
	return
}

func (db database) Get(ctx context.Context, record dal.Record) error {
	return getAndUnmarshal(ctx, record, db.client, getByDocRef)
}

func (db database) Exists(ctx context.Context, key *dal.Key) (exists bool, err error) {
	return existsByKey(ctx, key, db.client, getByDocRef)
}

func handleGetByKeyError(key *dal.Key, err error) error {
	if err != nil {
		if status.Code(err) == codes.NotFound {
			err = dal.NewErrNotFoundByKey(key, err)
		}
		return err
	}
	return nil
}

func docSnapshotToRecord(
	docSnapshot *firestore.DocumentSnapshot,
	record dal.Record,
	dataTo func(ds *firestore.DocumentSnapshot, p interface{}) error,
) error {
	if !docSnapshot.Exists() {
		key := record.Key()
		err := dal.NewErrNotFoundByKey(key, nil)
		record.SetError(err)
		return nil // This is for GetMulti() to continue processing other records
	}
	record.SetError(nil) // !Important - we need to setFirestore error to nil before accessing record.Data()
	recData := record.Data()
	if err := dataTo(docSnapshot, recData); err != nil {
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
	return getMulti(ctx, records, "db", db.client, db.client.GetAll)
}
