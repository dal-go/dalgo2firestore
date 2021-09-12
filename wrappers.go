package dalgo_firestore

import (
	"cloud.google.com/go/firestore"
	"context"
)

var delete = func(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.WriteResult, err error) {
	return docRef.Delete(ctx)
}

var create = func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (_ *firestore.WriteResult, err error) {
	return docRef.Create(ctx, data)
}

var get = func(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.DocumentSnapshot, err error) {
	return docRef.Get(ctx)
}

var getAll = func(ctx context.Context, client *firestore.Client, docRefs []*firestore.DocumentRef) (_ []*firestore.DocumentSnapshot, err error) {
	return client.GetAll(ctx, docRefs)
}
