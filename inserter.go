package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"log"
)

//type inserter struct {
//	client      *firestore.Client
//	keyToDocRef keyToDocRefFunc
//	create      func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (_ *firestore.WriteResult, err error)
//}
//
//func newInserter(db database) inserter {
//	return inserter{
//		client:      db.client,
//		create:      create,
//		keyToDocRef: keyToDocRef,
//	}
//}

func (db database) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	options := dal.NewInsertOptions(opts...)
	generateID := options.IDGenerator()
	if generateID != nil {
		if err := generateID(ctx, record); err != nil {
			return err
		}
	}
	_, err := insert(ctx, db, record, createNonTransactional)
	return err
}

func insert(ctx context.Context, db database, record dal.Record, create createFunc) (*firestore.WriteResult, error) {
	key := record.Key()
	docRef := keyToDocRef(key, db.client)
	if docRef != nil {
		log.Println("inserting document:", docRef.Path)
	}
	data := record.Data()
	return create(ctx, docRef, data)
}

func (db database) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) (err error) {
	options := dal.NewInsertOptions(opts...)
	generateID := options.IDGenerator()
	for i, record := range records {
		if generateID != nil {
			if err = generateID(ctx, record); err != nil {
				return fmt.Errorf("failed to generate ID for record %d out of %d: %w", i, len(records), err)
			}
		}
	}
	_, err = insertMulti(ctx, db, records, createNonTransactional)
	return err
}

// insertMulti inserts multiple records into the database using create() function that uses either transaction or not.
func insertMulti(ctx context.Context, db database, records []dal.Record, create createFunc) (results []*firestore.WriteResult, err error) {
	for i, record := range records {
		if _, err = insert(ctx, db, record, create); err != nil {
			return results, fmt.Errorf("failed to insert record %d out of %d: %w", i, len(records), err)
		}
	}
	return
}
