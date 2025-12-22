package dalgo2firestore

import (
	"context"
	"time"

	"github.com/dal-go/dalgo/dal"
)

//type deleter struct {
//	client      *firestore.Client
//	keyToDocRef keyToDocRefFunc
//	deleteByDocRef      func(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.WriteResult, err error)
//	bulkWriter       func(ctx context.Context) *firestore.BulkWriter
//}

//func newDeleter(dtb database) deleter {
//	return deleter{
//		client:      dtb.client,
//		keyToDocRef: keyToDocRef,
//		deleteByDocRef:      deleteByDocRef,
//		bulkWriter: func(c context.Context) *firestore.BulkWriter {
//
//		},
//	}
//}

// Delete deletes a record from the database.
func (db database) Delete(ctx context.Context, key *dal.Key) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	docRef := keyToDocRef(key, db.client)
	_, err = deleteByDocRef(ctx, docRef)
	if Debugf != nil {
		Debugf(ctx, "db.Delete(%v) completed in %v, err: %v", key, time.Since(started), err)
	}
	return err
}

// DeleteMulti deletes multiple records from the database.
func (db database) DeleteMulti(ctx context.Context, keys []*dal.Key) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	batch := db.bulkWriter(ctx)
	for _, key := range keys {
		docRef := keyToDocRef(key, db.client)
		if _, err = batch.Delete(docRef); err != nil {
			break
		}
	}
	batch.End()
	logMultiKeys(ctx, "db.DeleteMulti", keys, started, err)
	return err
}
