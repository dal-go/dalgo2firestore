package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"strings"
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
