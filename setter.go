package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
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

func (s setter) Set(ctx context.Context, record dal.Record) error {
	key := record.Key()
	docRef := s.doc(key)
	data := record.Data()
	_, err := s.set(ctx, docRef, data)
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
