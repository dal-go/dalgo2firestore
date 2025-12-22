package dalgo2firestore

import (
	"context"
	"testing"

	"cloud.google.com/go/firestore"

	"github.com/dal-go/dalgo/dal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Test_handleGetByKeyError_NotFoundWrap(t *testing.T) {
	key := dal.NewKeyWithID("c", "1")
	err := handleGetByKeyError(key, status.Errorf(codes.NotFound, "no"))
	if !dal.IsNotFound(err) {
		t.Fatalf("expected dal.NotFound, got: %v", err)
	}
}

func Test_existsByKey(t *testing.T) {
	ctx := context.Background()
	key := dal.NewKeyWithID("c", "1")
	client := &firestore.Client{}

	// Case: found
	exists, err := existsByKey(ctx, key, client, func(context.Context, *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return &firestore.DocumentSnapshot{}, nil
	})
	if err != nil || !exists {
		t.Fatalf("expected exists=true, err=nil; got exists=%v err=%v", exists, err)
	}
	// Case: not found
	exists, err = existsByKey(ctx, key, client, func(context.Context, *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return nil, status.Errorf(codes.NotFound, "no")
	})
	if err != nil || exists {
		t.Fatalf("expected exists=false, err=nil on not-found; got exists=%v err=%v", exists, err)
	}
}
