package dalgo2firestore

import "github.com/dal-go/dalgo/dal"

// PathFromKey generates a full path of a key
func PathFromKey(key *dal.Key) string {
	return key.String()
}
