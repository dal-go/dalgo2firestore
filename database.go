package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
)

// NewDatabase creates new instance of dalgo interface to Firestore
func NewDatabase(client *firestore.Client) dal.Database {
	if client == nil {
		panic("client is a required field, got nil")
	}
	dtb := database{
		client: client,
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
	inserter
	deleter
	getter
	setter
	updater
	client *firestore.Client
}

// Select implement respetive method from dal.ReadonlySession
func (dtb database) Select(ctx context.Context, query dal.Select) (dal.Reader, error) {
	panic("implement me")
}

var _ dal.Database = (*database)(nil)

func (dtb database) doc(key *dal.Key) *firestore.DocumentRef {
	path := PathFromKey(key)
	return dtb.client.Doc(path)
}

func (dtb database) Upsert(ctx context.Context, record dal.Record) error {
	panic("implement me")
}
