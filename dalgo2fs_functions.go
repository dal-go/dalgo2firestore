package dalgo2firestore

import (
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

var keyToDocRef = func(key *record.Key, client *firestore.Client) *firestore.DocumentRef {
	if key == nil {
		panic("key is a required parameter, got nil")
	}
	path := PathFromKey(key)
	return client.Doc(path)
}

var keyToCollectionRef = func(key *record.Key, client *firestore.Client) *firestore.CollectionRef {
	if key == nil {
		panic("key is a required parameter, got nil")
	}
	path := PathFromKey(key)
	path = strings.TrimSuffix(path, "/<nil>")
	return client.Collection(path)
}

func GetFirestoreCollectionRef(colRef *dal.CollectionRef, client *firestore.Client) (fsCollectionRef *firestore.CollectionRef) {
	if colRef == nil {
		panic("colRef is a required parameter, got nil")
	}
	if client == nil {
		panic("client is a required parameter, got nil")
	}
	path := colRef.Path()
	return client.Collection(path)
}
