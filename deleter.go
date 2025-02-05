package dalgo2firestore

import (
	"context"
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
func (db database) Delete(ctx context.Context, key *dal.Key) error {
	docRef := db.keyToDocRef(key)
	_, err := deleteByDocRef(ctx, docRef)
	return err
}

// DeleteMulti deletes multiple records from the database.
func (db database) DeleteMulti(ctx context.Context, keys []*dal.Key) error {
	logMultiKeys(ctx, "db.DeleteMulti", keys)
	batch := db.bulkWriter(ctx)
	for _, key := range keys {
		docRef := db.keyToDocRef(key)
		if _, err := batch.Delete(docRef); err != nil {
			return err
		}
	}
	batch.End()
	return nil
}
