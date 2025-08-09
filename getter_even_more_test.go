package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Test_getByKey_not_found(t *testing.T) {
	ctx := context.Background()
	key := dal.NewKeyWithID("c", "1")
	client := &firestore.Client{}
	_, err := getByKey(ctx, key, client, func(context.Context, *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return nil, status.Errorf(codes.NotFound, "no")
	})
	if !dal.IsNotFound(err) {
		t.Fatalf("expected NotFound, got: %v", err)
	}
}

func Test_getAndUnmarshal_error_sets_record_error(t *testing.T) {
	ctx := context.Background()
	key := dal.NewKeyWithID("c", "1")
	rec := dal.NewRecordWithData(key, struct{}{})
	client := &firestore.Client{}
	err := getAndUnmarshal(ctx, rec, client, func(context.Context, *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return nil, status.Errorf(codes.NotFound, "no")
	})
	if rec.Error() != nil {
		t.Fatalf("expected record.Error() to be nil for NotFound, got: %v", rec.Error())
	}
	if rec.Exists() {
		t.Fatalf("expected rec.Exists() to be false after NotFound")
	}
	if !dal.IsNotFound(err) {
		t.Fatalf("expected returned error to be NotFound, got: %v", err)
	}
}
