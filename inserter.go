package dalgo2firestore

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxIDGenerationAttempts bounds retries when an explicit dal.IDGenerator is supplied
// and the generated ID is already taken by an existing document.
const maxIDGenerationAttempts = 10

func insert(ctx context.Context, db database, record dalrecord.Record, create createFunc) (result *firestore.WriteResult, err error) {
	key := record.Key()
	docRef := keyToDocRef(key, db.client)
	if docRef != nil {
		log.Println("inserting document:", docRef.Path)
	} else {
		log.Println("inserting document: docRef=nil")
	}
	record.SetError(dalrecord.ErrNoError)
	data := record.Data()

	{ // TODO: Validations should be called by dalgo core
		if validatable, ok := data.(interface{ Validate() error }); ok {
			if err = validatable.Validate(); err != nil {
				record.SetError(err)
				return
			}
		}
		if validatable, ok := data.(interface {
			ValidateWithKey(key *dalrecord.Key) error
		}); ok {
			if err = validatable.ValidateWithKey(record.Key()); err != nil {
				record.SetError(fmt.Errorf("validate with record key returned error: %w", err))
				return
			}
		}
	}

	record.SetError(dalrecord.ErrNoError)
	if result, err = create(ctx, docRef, data); err != nil {
		record.SetError(fmt.Errorf("failed to insert record: %w", err))
		return
	}
	return
}

// recordExists returns nil if a document for the given key exists,
// an error matched by record.IsNotFound if it does not, or any other error as is.
func recordExists(ctx context.Context, key *dalrecord.Key, client *firestore.Client) error {
	docRef := keyToDocRef(key, client)
	_, err := getByDocRef(ctx, docRef)
	if status.Code(err) == codes.NotFound {
		err = dal.NewErrNotFoundByKey(key, err)
	}
	return err
}

// insertWithOptions inserts a single record honoring dal.InsertOptions:
//   - an explicit ID generator (e.g. dal.WithRandomStringKey) is run with bounded
//     retries while the generated ID is already taken by an existing document;
//   - dal.WithAdapterGeneratedID assigns Firestore's native client-side auto-ID
//     (generated locally, no round-trip, no collision check needed) to an incomplete key;
//   - otherwise the record is inserted as is.
func insertWithOptions(ctx context.Context, db database, record dalrecord.Record, create createFunc, options dal.InsertOptions) (err error) {
	if generateID := options.IDGenerator(); generateID != nil {
		return dal.InsertWithIdGenerator(ctx, record, generateID, maxIDGenerationAttempts,
			func(key *dalrecord.Key) error {
				return recordExists(ctx, key, db.client)
			},
			func(r dalrecord.Record) error {
				_, err := insert(ctx, db, r, create)
				return err
			},
		)
	}
	if options.PreferAdapterGeneratedID() {
		if key := record.Key(); key.ID == nil || key.ID == "" {
			key.ID = keyToCollectionRef(key, db.client).NewDoc().ID
		}
	}
	_, err = insert(ctx, db, record, create)
	return err
}

// insertMulti inserts multiple records into the database using create() function that uses either transaction or not.
func insertMulti(ctx context.Context, db database, records []dalrecord.Record, create createFunc, opts ...dal.InsertOption) (err error) {
	options := dal.NewInsertOptions(opts...)
	for i, record := range records {
		if err = insertWithOptions(ctx, db, record, create, options); err != nil {
			return fmt.Errorf("failed to insert record %d out of %d: %w", i, len(records), err)
		}
	}
	return
}
