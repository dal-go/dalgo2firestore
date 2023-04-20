package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
	"log"
)

//type inserter struct {
//	client      *firestore.Client
//	keyToDocRef keyToDocRefFunc
//	create      func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (_ *firestore.WriteResult, err error)
//}
//
//func newInserter(db Database) inserter {
//	return inserter{
//		client:      db.client,
//		create:      create,
//		keyToDocRef: keyToDocRef,
//	}
//}

func (db Database) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	options := dal.NewInsertOptions(opts...)
	generateID := options.IDGenerator()
	if generateID != nil {
		if err := generateID(ctx, record); err != nil {
			return err
		}
	}
	_, err := db.insert(ctx, record)
	return err
}

func (db Database) insert(ctx context.Context, record dal.Record) (*firestore.WriteResult, error) {
	key := record.Key()
	docRef := keyToDocRef(key, db.client)
	if docRef != nil {
		log.Println("inserting document:", docRef.Path)
	}
	data := record.Data()
	return create(ctx, docRef, data)
}
