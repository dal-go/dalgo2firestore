package dalgo2firestore

import (
	"github.com/dal-go/record"
)

// PathFromKey generates a full path of a key
func PathFromKey(key *record.Key) string {
	return key.String()
}
