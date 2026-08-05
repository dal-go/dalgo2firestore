package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
	"testing"
)

func TestFirestoreDocumentIDExpressionIsTyped(t *testing.T) {
	if got := firestoreOrderExpression(dal.DocumentID()); got != firestore.DocumentID {
		t.Fatalf("typed ID = %q", got)
	}
	if got := firestoreOrderExpression(dal.Field("__name__")); got != "__name__" {
		t.Fatalf("ordinary field = %q", got)
	}
}
