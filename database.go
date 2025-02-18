package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
)

// NewDatabase creates new instance of dalgo interface to Firestore
func NewDatabase(id string, client *firestore.Client) (db dal.DB) {
	if id == "" {
		panic("id is a required field, got empty string")
	}
	if client == nil {
		panic("client is a required field, got nil")
	}
	var getReader = func(c context.Context, query dal.Query) (reader dal.Reader, err error) {
		return newFirestoreReader(c, client, query)
	}
	return &database{
		id:            id,
		client:        client,
		QueryExecutor: dal.NewQueryExecutor(getReader),
	}
}

var _ dal.DB = database{}
var _ dal.DB = (*database)(nil)

// database implements dal.Database
type database struct {
	id     string
	client *firestore.Client
	dal.QueryExecutor
}

func (db database) ID() string {
	return db.id
}

func (db database) Adapter() dal.Adapter {
	return dal.NewAdapter("firestore", "v1.9.0")
}

var _ dal.DB = (*database)(nil)

func (db database) Upsert(_ context.Context, _ dal.Record) error {
	panic("implement me")
}

func (db database) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	if Debugf != nil {
		Debugf(ctx, "db.Insert(key=%v)", record.Key())
	}
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

func (db database) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) (err error) {
	logMultiRecords(ctx, "db.InsertMulti", records)
	_, err = insertMulti(ctx, db, records, createNonTransactional, opts...)
	return err
}
