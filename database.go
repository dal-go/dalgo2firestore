package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
)

// NewDatabase creates new instance of dalgo interface to Firestore
func NewDatabase(id string, client *firestore.Client) (db Database) {
	if id == "" {
		panic("id is a required field, got empty string")
	}
	if client == nil {
		panic("client is a required field, got nil")
	}
	var getReader = func(c context.Context, query dal.Query) (reader dal.Reader, err error) {
		return newFirestoreReader(c, client, query)
	}
	db = Database{
		id:            id,
		client:        client,
		QueryExecutor: dal.NewQueryExecutor(getReader),
	}
	return db
}

var _ dal.Database = Database{}
var _ dal.Database = (*Database)(nil)

// Database implements dal.Database
type Database struct {
	id     string
	client *firestore.Client
	dal.QueryExecutor
}

func (db Database) ID() string {
	return db.id
}

func (db Database) Client() dal.ClientInfo {
	return dal.NewClientInfo("firestore", "v1.9.0")
}

var _ dal.Database = (*Database)(nil)

func (db Database) Upsert(ctx context.Context, record dal.Record) error {
	panic("implement me")
}

func (db Database) keyToDocRef(key *dal.Key) *firestore.DocumentRef {
	return keyToDocRef(key, db.client)
}
