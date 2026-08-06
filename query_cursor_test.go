package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/option"
	"reflect"
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

func TestApplyQueryWindowPreservesImmutableFirestoreClauses(t *testing.T) {
	client, err := firestore.NewClient(context.Background(), "query-window-test", option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	base := client.Collection("states").Query
	query := func(from, after dal.Cursor) dal.StructuredQuery {
		builder := dal.From(dal.NewRootCollectionRef("states", "")).NewQuery().Offset(2).Limit(3).OrderBy(dal.Ascending(dal.DocumentID()))
		if from != "" {
			builder = builder.StartFrom(from)
		}
		if after != "" {
			builder = builder.StartAfter(after)
		}
		return builder.SelectKeysOnly(reflect.String)
	}
	for _, test := range []struct {
		name string
		got  firestore.Query
		want firestore.Query
	}{
		{name: "inclusive", got: applyQueryWindow(query("state-2", ""), base), want: base.Limit(3).Offset(2).StartAt(dal.Cursor("state-2"))},
		{name: "exclusive", got: applyQueryWindow(query("", "state-2"), base), want: base.Limit(3).Offset(2).StartAfter(dal.Cursor("state-2"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			if !reflect.DeepEqual(test.got, test.want) {
				t.Fatalf("Firestore window was not preserved\ngot:  %#v\nwant: %#v", test.got, test.want)
			}
		})
	}
}
