package dalgo2firestore

import (
	"context"

	"cloud.google.com/go/firestore"
)

type bulkWriter interface {
	Delete(doc *firestore.DocumentRef, fps ...firestore.Precondition) (*firestore.BulkWriterJob, error)
	Set(doc *firestore.DocumentRef, in interface{}, fps ...firestore.SetOption) (*firestore.BulkWriterJob, error)
	End()
}

var newBulkWriter = func(ctx context.Context, db database) bulkWriter {
	return db.client.BulkWriter(ctx)
}

func (db database) bulkWriter(ctx context.Context) bulkWriter {
	return newBulkWriter(ctx, db)
}
