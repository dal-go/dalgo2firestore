package dalgo2firestore

import (
	"context"

	"cloud.google.com/go/firestore"
)

func (db database) bulkWriter(ctx context.Context) *firestore.BulkWriter {
	return db.client.BulkWriter(ctx)
}
