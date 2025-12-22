package dalgo2firestore

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
)

func insert(ctx context.Context, db database, record dal.Record, create createFunc) (result *firestore.WriteResult, err error) {
	key := record.Key()
	docRef := keyToDocRef(key, db.client)
	if docRef != nil {
		log.Println("inserting document:", docRef.Path)
	} else {
		log.Println("inserting document: docRef=nil")
	}
	record.SetError(dal.ErrNoError)
	data := record.Data()

	{ // TODO: Validations should be called by dalgo core
		if validatable, ok := data.(interface{ Validate() error }); ok {
			if err = validatable.Validate(); err != nil {
				record.SetError(err)
				return
			}
		}
		if validatable, ok := data.(interface{ ValidateWithKey(key *dal.Key) error }); ok {
			if err = validatable.ValidateWithKey(record.Key()); err != nil {
				record.SetError(fmt.Errorf("validate with record key returned error: %w", err))
				return
			}
		}
	}

	record.SetError(dal.ErrNoError)
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
