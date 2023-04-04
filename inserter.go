package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
	"log"
)

type inserter struct {
	doc    func(key *dal.Key) *firestore.DocumentRef
	create func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (_ *firestore.WriteResult, err error)
}

func newInserter(dtb database) inserter {
	return inserter{
		doc:    dtb.doc,
		create: create,
	}
}

func (i inserter) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	options := dal.NewInsertOptions(opts...)
	generateID := options.IDGenerator()
	if generateID != nil {
		if err := generateID(ctx, record); err != nil {
			return err
		}
	}
	_, err := i.insert(ctx, record)
	return err
}

func (i inserter) insert(ctx context.Context, record dal.Record) (*firestore.WriteResult, error) {
	key := record.Key()
	docRef := i.doc(key)
	if docRef != nil {
		log.Println("inserting document:", docRef.Path)
	}
	data := record.Data()
	return i.create(ctx, docRef, data)
}
