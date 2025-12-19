package dalgo2firestore

import (
	"fmt"
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
)

var keyToDocRef = func(key *dal.Key, client *firestore.Client) *firestore.DocumentRef {
	if key == nil {
		panic("key is a required parameter, got nil")
	}
	path := PathFromKey(key)
	docRef := client.Doc(path)
	if docRef == nil {
		panic(fmt.Sprintf("docRef is nil for path=%s, key: %v", path, key))
	}
	return docRef
}

var keyToCollectionRef = func(key *dal.Key, client *firestore.Client) *firestore.CollectionRef {
	if key == nil {
		panic("key is a required parameter, got nil")
	}
	path := PathFromKey(key)
	path = strings.TrimSuffix(path, "/<nil>")
	collectionRef := client.Collection(path)
	if collectionRef == nil {
		panic(fmt.Sprintf("collectionRef is nil for path=%s, key: %v", path, key))
	}
	return collectionRef
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
