package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

type setter struct {
	doc   func(key *dal.Key) *firestore.DocumentRef
	set   func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (_ *firestore.WriteResult, err error)
	batch func(ctx context.Context) *firestore.BulkWriter
}

func newSetter(dtb database) setter {
	return setter{
		doc: dtb.doc,
		set: set,
		batch: func(ctx context.Context) *firestore.BulkWriter {
			return dtb.client.BulkWriter(ctx)
		},
	}
}

func (s setter) Set(ctx context.Context, record dal.Record) (err error) {
	if record == nil {
		panic("record is a required parameter, got nil")
	}
	key := record.Key()
	if key == nil {
		panic("record.Key() returned nil")
	}
	docRef := s.doc(key)
	if docRef == nil {
		return fmt.Errorf("docRef is nil for key=%v", key)
	}
	data := record.Data()
	_, err = s.set(ctx, docRef, data)
	return err
}

func (s setter) SetMulti(ctx context.Context, records []dal.Record) error {
	batch := s.batch(ctx)
	for _, record := range records {
		key := record.Key()
		docRef := s.doc(key)
		data := record.Data()
		if _, err := batch.Set(docRef, data); err != nil {
			return err
		}
	}
	batch.End()
	return nil
}
