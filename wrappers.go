package dalgo2firestore

import (
	"context"

	"cloud.google.com/go/firestore"
)

var deleteByDocRef = func(ctx context.Context, docRef *firestore.DocumentRef) (result *firestore.WriteResult, err error) {
	return docRef.Delete(ctx)
}

type createFunc func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (result *firestore.WriteResult, err error)

var createNonTransactional = func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (result *firestore.WriteResult, err error) {
	return docRef.Create(ctx, data)
}

var setFirestore = func(ctx context.Context, docRef *firestore.DocumentRef, data interface{}) (result *firestore.WriteResult, err error) {
	return docRef.Set(ctx, data)
}

var getByDocRef = func(ctx context.Context, docRef *firestore.DocumentRef) (result *firestore.DocumentSnapshot, err error) {
	return docRef.Get(ctx)
}

//var getAll = func(ctx context.Context, client *firestore.Client, docRefs []*firestore.DocumentRef) (_ []*firestore.DocumentSnapshot, err error) {
//	return client.GetAll(ctx, docRefs)
//}
