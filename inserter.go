package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"log"
)

func insert(ctx context.Context, db database, record dal.Record, create createFunc) (result *firestore.WriteResult, err error) {
	key := record.Key()
	docRef := keyToDocRef(key, db.client)
	if docRef != nil {
		log.Println("inserting document:", docRef.Path)
	} else {
		log.Println("inserting document: docRef=nil")
	}
	record.SetError(dal.NoError)
	data := record.Data()
	if validatable, ok := data.(interface{ Validate() error }); ok {
		if err = validatable.Validate(); err != nil {
			record.SetError(err)
			return
		}
	}
	record.SetError(dal.NoError)
	if result, err = create(ctx, docRef, data); err != nil {
		record.SetError(fmt.Errorf("failed to insert record: %w", err))
		return
	}
	return
}

// insertMulti inserts multiple records into the database using create() function that uses either transaction or not.
func insertMulti(ctx context.Context, db database, records []dal.Record, create createFunc, opts ...dal.InsertOption) (results []*firestore.WriteResult, err error) {
	options := dal.NewInsertOptions(opts...)
	generateID := options.IDGenerator()
	for i, record := range records {
		if generateID != nil {
			if err = generateID(ctx, record); err != nil {
				return results, fmt.Errorf("failed to generate ID for record %d out of %d: %w", i, len(records), err)
			}
		}
		if _, err = insert(ctx, db, record, create); err != nil {
			return results, fmt.Errorf("failed to insert record %d out of %d: %w", i, len(records), err)
		}
	}
	return
}
