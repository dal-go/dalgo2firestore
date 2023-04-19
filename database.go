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
	dtb := database{
		id:            id,
		client:        client,
		QueryExecutor: dal.NewQueryExecutor(getReader),
	}
	dtb.inserter = newInserter(dtb)
	dtb.deleter = newDeleter(dtb)
	dtb.getter = newGetter(dtb)
	dtb.setter = newSetter(dtb)
	dtb.updater = newUpdater(&dtb)
	return dtb
}

// database implements dal.Database
type database struct {
	id string
	inserter
	deleter
	getter
	setter
	updater
	client *firestore.Client
	dal.QueryExecutor
}

func (dtb database) ID() string {
	return dtb.id
}

func (dtb database) Client() dal.ClientInfo {
	return dal.NewClientInfo("firestore", "v1.9.0")
}

var _ dal.Database = (*database)(nil)

func (dtb database) doc(key *dal.Key) *firestore.DocumentRef {
	path := PathFromKey(key)
	return dtb.client.Doc(path)
}

func (dtb database) Upsert(ctx context.Context, record dal.Record) error {
	panic("implement me")
}
