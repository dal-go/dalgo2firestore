package dalgo2firestore

import (
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

//type setter struct {
//	client      *firestore.Client
//	keyToDocRef keyToDocRefFunc
//	set         func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (_ *firestore.WriteResult, err error)
//	bulkWriter       func(ctx context.Context) *firestore.BulkWriter
//}

//func newSetter(dtb Database) setter {
//	return setter{
//		keyToDocRef: keyToDocRef,
//		set:         set,
//		bulkWriter: func(ctx context.Context) *firestore.BulkWriter {
//			return dtb.client.BulkWriter(ctx)
//		},
//	}
//}

func (db Database) Set(ctx context.Context, record dal.Record) (err error) {
	if record == nil {
		panic("record is a required parameter, got nil")
	}
	key := record.Key()
	docRef := db.keyToDocRef(key)
	if docRef == nil {
		return fmt.Errorf("keyToDocRef is nil for key=%v", key)
	}
	data := record.Data()
	_, err = set(ctx, docRef, data)
	return err
}

func (db Database) SetMulti(ctx context.Context, records []dal.Record) error {
	batch := db.bulkWriter(ctx)
	for _, record := range records {
		key := record.Key()
		docRef := db.keyToDocRef(key)
		data := record.Data()
		if _, err := batch.Set(docRef, data); err != nil {
			return err
		}
	}
	batch.End()
	return nil
}
