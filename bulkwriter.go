package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
)

func (db database) bulkWriter(ctx context.Context) *firestore.BulkWriter {
	return db.client.BulkWriter(ctx)
}
