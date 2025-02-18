package dalgo2firestore

import (
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

//type setter struct {
//	client      *firestore.Client
//	keyToDocRef keyToDocRefFunc
//	setFirestore         func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (_ *firestore.WriteResult, err error)
//	bulkWriter       func(ctx context.Context) *firestore.BulkWriter
//}

//func newSetter(dtb database) setter {
//	return setter{
//		keyToDocRef: keyToDocRef,
//		setFirestore:         setFirestore,
//		bulkWriter: func(ctx context.Context) *firestore.BulkWriter {
//			return dtb.client.BulkWriter(ctx)
//		},
//	}
//}

func (db database) Set(ctx context.Context, record dal.Record) (err error) {
	if record == nil {
		panic("record is a required parameter, got nil")
	}
	if Debugf != nil {
		Debugf(ctx, "db.Set(key=%s)", record.Key().String())
	}
	key := record.Key()
	docRef := keyToDocRef(key, db.client)
	if docRef == nil {
		return fmt.Errorf("keyToDocRef is nil for key=%v", key)
	}
	data := record.Data()
	_, err = setFirestore(ctx, docRef, data)
	return err
}

func (db database) SetMulti(ctx context.Context, records []dal.Record) error {
	logMultiRecords(ctx, "db.SetMulti", records)
	batch := db.bulkWriter(ctx)
	for _, record := range records {
		key := record.Key()
		docRef := keyToDocRef(key, db.client)
		data := record.Data()
		if _, err := batch.Set(docRef, data); err != nil {
			return err
		}
	}
	batch.End()
	return nil
}
