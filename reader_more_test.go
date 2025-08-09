package dalgo2firestore

import (
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func Test_firestoreReader_Next_respects_limit(t *testing.T) {
	q := dal.NewQueryBuilder(dal.NewRootCollectionRef("c", "")).Limit(1).SelectKeysOnly(0) // idKind 0 ok for SelectKeysOnly
	fr := &firestoreReader{query: q, i: 1}
	if _, err := fr.Next(); err == nil || err != dal.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords, got: %v", err)
	}
}
