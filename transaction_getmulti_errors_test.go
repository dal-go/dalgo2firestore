package dalgo2firestore

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

type batchErrorCounter struct {
	Owner string
	Count int64
}

func batchErrorSnapshot(client *firestore.Client, id string, malformed bool) *firestore.DocumentSnapshot {
	count := &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 7}}
	if malformed {
		count = &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: "not-an-integer"}}
	}
	// Reuse the existing test-only snapshot layout. The actual SDK DataTo
	// decoder is exercised, with no Firestore service or network client.
	snapshot := &fakeDocumentSnapshot{Ref: client.Doc("c/" + id), proto: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
		"Owner": {ValueType: &firestorepb.Value_StringValue{StringValue: "fixture-owner"}},
		"Count": count,
	}}}
	return (*firestore.DocumentSnapshot)(unsafe.Pointer(snapshot))
}

func TestGetMultiPropagatesSDKDecodeErrors(t *testing.T) {
	client := &firestore.Client{}
	originalGetAll := getAllFirestore
	t.Cleanup(func() { getAllFirestore = originalGetAll })
	for _, malformed := range [][]bool{{true, false}, {false, true}, {true, true}, {false, false}} {
		records := []dalrecord.Record{
			dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "global"), &batchErrorCounter{}),
			dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "key"), &batchErrorCounter{}),
		}
		getAllFirestore = func(context.Context, *firestore.Client, []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
			return []*firestore.DocumentSnapshot{batchErrorSnapshot(client, "global", malformed[0]), batchErrorSnapshot(client, "key", malformed[1])}, nil
		}
		err := NewDatabase("batch-errors", client).GetMulti(t.Context(), records)
		if (err != nil) != (malformed[0] || malformed[1]) {
			t.Fatalf("decode error discarded: malformed=%v err=%v", malformed, err)
		}
		for i, record := range records {
			if malformed[i] {
				if record.Error() == nil || !errors.Is(err, record.Error()) {
					t.Fatalf("aggregate omitted record %d error: %v / %v", i, err, record.Error())
				}
			} else if record.Error() != nil || record.Data().(*batchErrorCounter).Count != 7 {
				t.Fatalf("valid batch record lost: %d %v", i, record.Error())
			}
		}
	}
	getAllFirestore = func(context.Context, *firestore.Client, []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
		return []*firestore.DocumentSnapshot{{}, {}}, nil
	}
	missing := []dalrecord.Record{
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "global"), &batchErrorCounter{}),
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "key"), &batchErrorCounter{}),
	}
	if err := NewDatabase("batch-errors", client).GetMulti(t.Context(), missing); err != nil || missing[0].Exists() || missing[1].Exists() {
		t.Fatalf("missing-document compatibility changed: %v", err)
	}
}

func TestGetMultiDecodeErrorReachesConditionalSecureDB(t *testing.T) {
	client := &firestore.Client{}
	originalGetAll := getAllFirestore
	t.Cleanup(func() { getAllFirestore = originalGetAll })
	policy := access.MustPolicy("batch-errors", access.Root(access.Allow(access.Get).Where(dal.WhereField("Owner", dal.Equal, "fixture-owner"))))
	for _, badPosition := range []int{0, 1, -1} {
		t.Run(map[int]string{0: "first", 1: "last", -1: "valid"}[badPosition], func(t *testing.T) {
			getAllFirestore = func(context.Context, *firestore.Client, []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
				return []*firestore.DocumentSnapshot{batchErrorSnapshot(client, "global", badPosition == 0), batchErrorSnapshot(client, "key", badPosition == 1)}, nil
			}
			secured := access.MustSecureDB(NewDatabase("batch-errors", client), access.WithDatabasePolicies(policy))
			records := []dalrecord.Record{
				dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "global"), &batchErrorCounter{}),
				dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "key"), &batchErrorCounter{}),
			}
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("conditional policy received an unreported decode error and panicked: %v", recovered)
				}
			}()
			err := secured.GetMulti(t.Context(), records)
			if (err != nil) != (badPosition >= 0) {
				t.Fatalf("conditional batch error: position=%d err=%v", badPosition, err)
			}
		})
	}
}

func TestTransactionalGetMultiDecodeErrorAndConditionalPolicy(t *testing.T) {
	client := &firestore.Client{}
	originalRunner, originalGetAll := runFirestoreTransaction, getAllInFirestoreTransaction
	t.Cleanup(func() {
		runFirestoreTransaction, getAllInFirestoreTransaction = originalRunner, originalGetAll
	})
	// The real provider transaction and published SecureDB wrappers run here;
	// only the SDK transaction transport is replaced with owned snapshots.
	runFirestoreTransaction = func(_ *firestore.Client, ctx context.Context, work func(context.Context, *firestore.Transaction) error, _ ...firestore.TransactionOption) error {
		return work(ctx, &firestore.Transaction{})
	}
	for _, readonly := range []bool{true, false} {
		for _, tc := range []struct {
			name  string
			bad   int
			owner string
		}{
			{"first", 0, "fixture-owner"}, {"last", 1, "fixture-owner"},
			{"valid", -1, "fixture-owner"}, {"policy-denied", -1, "foreign-owner"},
		} {
			t.Run(map[bool]string{true: "readonly/", false: "readwrite/"}[readonly]+tc.name, func(t *testing.T) {
				calls := 0
				getAllInFirestoreTransaction = func(_ *firestore.Transaction, refs []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
					calls++
					if len(refs) != 2 || refs[0].ID != "global" || refs[1].ID != "key" {
						t.Fatal("transaction did not batch the original two record identities")
					}
					return []*firestore.DocumentSnapshot{batchErrorSnapshot(client, "global", tc.bad == 0), batchErrorSnapshot(client, "key", tc.bad == 1)}, nil
				}
				policy := access.MustPolicy("transaction-batch", access.Root(access.Allow(access.Get).Where(dal.WhereField("Owner", dal.Equal, tc.owner))))
				secured := access.MustSecureDB(NewDatabase("batch-errors", client), access.WithDatabasePolicies(policy))
				records := []dalrecord.Record{
					dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "global"), &batchErrorCounter{}),
					dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "key"), &batchErrorCounter{}),
				}
				read := func(ctx context.Context, session dal.ReadSession) error { return session.GetMulti(ctx, records) }
				defer func() {
					if recovered := recover(); recovered != nil {
						t.Fatalf("transactional conditional enforcement panicked: %v", recovered)
					}
				}()
				var err error
				if readonly {
					err = secured.RunReadonlyTransaction(t.Context(), func(ctx context.Context, tx dal.ReadTransaction) error { return read(ctx, tx) })
				} else {
					err = secured.RunReadwriteTransaction(t.Context(), func(ctx context.Context, tx dal.ReadwriteTransaction) error { return read(ctx, tx) })
				}
				if calls != 1 {
					t.Fatalf("expected one actual provider batch, got %d", calls)
				}
				switch {
				case tc.bad >= 0:
					if records[tc.bad].Error() == nil || !errors.Is(err, records[tc.bad].Error()) {
						t.Fatalf("transaction lost the original decode error: %v", err)
					}
				case tc.owner != "fixture-owner":
					if !errors.Is(err, access.ErrAccessDenied) {
						t.Fatalf("valid decoded data bypassed its row policy: %v", err)
					}
				default:
					if err != nil || records[0].Data().(*batchErrorCounter).Count != 7 || records[1].Data().(*batchErrorCounter).Count != 7 {
						t.Fatalf("valid transaction batch failed: %v", err)
					}
				}
			})
		}
	}
}
