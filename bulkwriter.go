package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
)

func (db Database) bulkWriter(ctx context.Context) *firestore.BulkWriter {
	return db.client.BulkWriter(ctx)
}
