package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
)

type deleter struct {
	doc    func(key *dal.Key) *firestore.DocumentRef
	delete func(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.WriteResult, err error)
	batch  func(ctx context.Context) *firestore.BulkWriter
}

func newDeleter(dtb database) deleter {
	return deleter{
		doc:    dtb.doc,
		delete: delete,
		batch: func(c context.Context) *firestore.BulkWriter {
			return dtb.client.BulkWriter(c)
		},
	}
}

func (d deleter) Delete(ctx context.Context, key *dal.Key) error {
	docRef := d.doc(key)
	_, err := d.delete(ctx, docRef)
	return err
}

func (d deleter) DeleteMulti(ctx context.Context, keys []*dal.Key) error {
	batch := d.batch(ctx)
	for _, key := range keys {
		docRef := d.doc(key)
		if _, err := batch.Delete(docRef); err != nil {
			return err
		}
	}
	batch.End()
	return nil
}
