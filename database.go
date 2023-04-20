package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
)

// NewDatabase creates new instance of dalgo interface to Firestore
func NewDatabase(id string, client *firestore.Client) (db dal.Database) {
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

var _ dal.Database = database{}
var _ dal.Database = (*database)(nil)

// database implements dal.Database
type database struct {
	id     string
	client *firestore.Client
	dal.QueryExecutor
}

func (db database) ID() string {
	return db.id
}

func (db database) Client() dal.ClientInfo {
	return dal.NewClientInfo("firestore", "v1.9.0")
}

var _ dal.Database = (*database)(nil)

func (db database) Upsert(ctx context.Context, record dal.Record) error {
	panic("implement me")
}

func (db database) keyToDocRef(key *dal.Key) *firestore.DocumentRef {
	return keyToDocRef(key, db.client)
}
