package dalgo2firestore

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type mockBulkWriter struct {
	deleteErr error
	setErr    error
}

func (m mockBulkWriter) Delete(_ *firestore.DocumentRef, _ ...firestore.Precondition) (*firestore.BulkWriterJob, error) {
	return nil, m.deleteErr
}

func (m mockBulkWriter) Set(_ *firestore.DocumentRef, _ interface{}, _ ...firestore.SetOption) (*firestore.BulkWriterJob, error) {
	return nil, m.setErr
}

func (m mockBulkWriter) End() {}

type dummyQuery struct{}

func (dummyQuery) String() string { return "dummy" }
func (dummyQuery) Offset() int    { return 0 }
func (dummyQuery) Limit() int     { return 0 }
func (dummyQuery) GetRecordsReader(_ context.Context, _ dal.QueryExecutor) (dal.RecordsReader, error) {
	return nil, nil
}
func (dummyQuery) GetRecordsetReader(_ context.Context, _ dal.QueryExecutor) (dal.RecordsetReader, error) {
	return nil, nil
}

func TestDatabase_BasicCoverage(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty ID")
		}
	}()
	_ = NewDatabase("", &firestore.Client{})
}

func TestDatabase_NilClientPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil client")
		}
	}()
	_ = NewDatabase("db1", nil)
}

func TestDatabase_MetadataAndUpsert(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origSet := setFirestore
	defer func() {
		keyToDocRef = origKeyToDocRef
		setFirestore = origSet
	}()

	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "k1"}
	}
	setFirestore = func(_ context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
		return &firestore.WriteResult{}, nil
	}

	db := database{id: "test-db", client: &firestore.Client{}}
	if db.ID() != "test-db" {
		t.Fatalf("expected ID test-db, got %q", db.ID())
	}
	if db.Adapter().Name() != "firestore" {
		t.Fatalf("expected adapter firestore, got %v", db.Adapter())
	}
	if db.Schema() != nil {
		t.Fatal("expected nil schema")
	}

	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "1"), map[string]any{"a": 1})
	if err := db.Upsert(context.Background(), rec); err != nil {
		t.Fatalf("unexpected Upsert error: %v", err)
	}
}

func TestDeleter_Coverage(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origDelete := deleteByDocRef
	origBulk := newBulkWriter
	origDebug := Debugf
	defer func() {
		keyToDocRef = origKeyToDocRef
		deleteByDocRef = origDelete
		newBulkWriter = origBulk
		Debugf = origDebug
	}()

	Debugf = func(_ context.Context, _ string, _ ...any) {}

	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "d1"}
	}
	deleteByDocRef = func(_ context.Context, _ *firestore.DocumentRef) (*firestore.WriteResult, error) {
		return &firestore.WriteResult{}, nil
	}

	db := database{id: "db", client: &firestore.Client{}}
	k := dalrecord.NewKeyWithID("c", "1")

	// Delete with Debugf
	if err := db.Delete(context.Background(), k); err != nil {
		t.Fatalf("unexpected Delete error: %v", err)
	}

	// DeleteMulti success
	newBulkWriter = func(_ context.Context, _ database) bulkWriter {
		return mockBulkWriter{}
	}
	if err := db.DeleteMulti(context.Background(), []*dalrecord.Key{k}); err != nil {
		t.Fatalf("unexpected DeleteMulti error: %v", err)
	}

	// DeleteMulti error
	newBulkWriter = func(_ context.Context, _ database) bulkWriter {
		return mockBulkWriter{deleteErr: errors.New("delete failed")}
	}
	if err := db.DeleteMulti(context.Background(), []*dalrecord.Key{k}); err == nil {
		t.Fatal("expected DeleteMulti error")
	}
}

func TestSetter_Coverage(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origSet := setFirestore
	origBulk := newBulkWriter
	origDebug := Debugf
	defer func() {
		keyToDocRef = origKeyToDocRef
		setFirestore = origSet
		newBulkWriter = origBulk
		Debugf = origDebug
	}()

	Debugf = func(_ context.Context, _ string, _ ...any) {}

	db := database{id: "db", client: &firestore.Client{}}

	// Set nil record panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil record")
			}
		}()
		_ = db.Set(context.Background(), nil)
	}()

	// Set docRef nil
	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return nil
	}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "1"), map[string]any{"x": 1})
	if err := db.Set(context.Background(), rec); err == nil {
		t.Fatal("expected error on nil docRef")
	}

	// SetMulti success
	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "s1"}
	}
	newBulkWriter = func(_ context.Context, _ database) bulkWriter {
		return mockBulkWriter{}
	}
	if err := db.SetMulti(context.Background(), []dalrecord.Record{rec}); err != nil {
		t.Fatalf("unexpected SetMulti error: %v", err)
	}

	// SetMulti error
	newBulkWriter = func(_ context.Context, _ database) bulkWriter {
		return mockBulkWriter{setErr: errors.New("set failed")}
	}
	if err := db.SetMulti(context.Background(), []dalrecord.Record{rec}); err == nil {
		t.Fatal("expected SetMulti error")
	}
}

func TestDalgo2FSFunctions_Panics(t *testing.T) {
	// keyToDocRef nil panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil key")
			}
		}()
		_ = keyToDocRef(nil, &firestore.Client{})
	}()

	// keyToCollectionRef nil panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil key")
			}
		}()
		_ = keyToCollectionRef(nil, &firestore.Client{})
	}()

	// GetFirestoreCollectionRef nil colRef panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil colRef")
			}
		}()
		_ = GetFirestoreCollectionRef(nil, &firestore.Client{})
	}()

	// GetFirestoreCollectionRef nil client panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil client")
			}
		}()
		colRef := dal.NewRootCollectionRef("c", "")
		_ = GetFirestoreCollectionRef(&colRef, nil)
	}()
}

func TestInserter_DocRefNil(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	defer func() { keyToDocRef = origKeyToDocRef }()

	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return nil
	}

	db := database{id: "db", client: &firestore.Client{}}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "1"), map[string]any{"v": 1})
	create := func(_ context.Context, _ *firestore.DocumentRef, _ interface{}) (*firestore.WriteResult, error) {
		return &firestore.WriteResult{}, nil
	}

	if _, err := insert(context.Background(), db, rec, create); err != nil {
		t.Fatalf("unexpected insert error: %v", err)
	}
}

func TestQueryExecutor_Coverage(t *testing.T) {
	qe := queryExecutor{
		getRecordsReader: func(_ context.Context, _ dal.Query) (dal.RecordsReader, error) {
			return nil, errors.New("read err")
		},
	}
	if _, err := qe.ExecuteQueryToRecordsReader(context.Background(), nil); err == nil {
		t.Fatal("expected error from ExecuteQueryToRecordsReader")
	}
	if _, err := qe.ExecuteQueryToRecordsetReader(context.Background(), nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got: %v", err)
	}
}

func TestReader_UnitCoverage(t *testing.T) {
	r := &firestoreReader{}
	if err := r.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
	if _, err := r.Cursor(); !errors.Is(err, dal.ErrNotImplementedYet) {
		t.Fatalf("expected ErrNotImplementedYet, got: %v", err)
	}

	// newFirestoreReader nil query
	if _, err := newFirestoreReader(context.Background(), &firestore.Client{}, nil); err == nil {
		t.Fatal("expected error on nil query")
	}

	// Next() non-structured query
	rBadQuery := &firestoreReader{query: dummyQuery{}}
	if _, err := rBadQuery.Next(); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported on non-structured query, got: %v", err)
	}

	// Next() limit exceeded
	qLimit := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).Limit(2).SelectKeysOnly(reflect.String)
	rLimit := &firestoreReader{query: qLimit, i: 2}
	if _, err := rLimit.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("expected ErrNoMoreRecords when limit reached, got: %v", err)
	}

	// Next() without IntoRecord and IDKind == reflect.Invalid
	qNoID := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).SelectKeysOnly(reflect.Invalid)
	rNoID := &firestoreReader{query: qNoID}
	if _, err := rNoID.Next(); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported on Invalid IDKind, got: %v", err)
	}

	// Next() iterator.Done error
	origNext := docIteratorNext
	defer func() { docIteratorNext = origNext }()

	docIteratorNext = func(_ *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
		return nil, iterator.Done
	}
	qValid := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).SelectKeysOnly(reflect.String)
	rDone := &firestoreReader{query: qValid}
	if _, err := rDone.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("expected ErrNoMoreRecords on iterator.Done, got: %v", err)
	}

	// Next() generic iterator error
	docIteratorNext = func(_ *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
		return nil, errors.New("read failed")
	}
	rErr := &firestoreReader{query: qValid}
	if _, err := rErr.Next(); err == nil {
		t.Fatal("expected error on generic iterator error")
	}

	// idFromFirestoreDocRef tests
	docRefStr := &firestore.DocumentRef{ID: "strID"}
	if id, err := idFromFirestoreDocRef(docRefStr, reflect.Invalid); err == nil || id != nil {
		t.Fatal("expected error on reflect.Invalid")
	}
	if id, err := idFromFirestoreDocRef(docRefStr, reflect.String); err != nil || id != "strID" {
		t.Fatalf("expected strID, got %v, err=%v", id, err)
	}
	// non-numeric ID for Int
	if _, err := idFromFirestoreDocRef(docRefStr, reflect.Int); err == nil {
		t.Fatal("expected error converting string ID to int")
	}
	// numeric ID for various int kinds
	docRefNum := &firestore.DocumentRef{ID: "123"}
	for _, k := range []reflect.Kind{reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8} {
		if id, err := idFromFirestoreDocRef(docRefNum, k); err != nil || id != 123 {
			t.Fatalf("kind %v: expected 123, got %v, err=%v", k, id, err)
		}
	}
	// unsupported kind
	if _, err := idFromFirestoreDocRef(docRefNum, reflect.Float64); err == nil {
		t.Fatal("expected error on unsupported kind Float64")
	}
}

type dotUpdate struct{}

func (dotUpdate) FieldName() string           { return "a.b" }
func (dotUpdate) FieldPath() update.FieldPath { return nil }
func (dotUpdate) Value() any                  { return 1 }

type emptyUpdate struct{}

func (emptyUpdate) FieldName() string           { return "" }
func (emptyUpdate) FieldPath() update.FieldPath { return nil }
func (emptyUpdate) Value() any                  { return 1 }

type emptyPathSegUpdate struct{}

func (emptyPathSegUpdate) FieldName() string           { return "" }
func (emptyPathSegUpdate) FieldPath() update.FieldPath { return update.FieldPath{"a", ""} }
func (emptyPathSegUpdate) Value() any                  { return 1 }

func TestUpdater_UnitCoverage(t *testing.T) {
	// getFirestoreUpdates 0 updates
	if _, err := getFirestoreUpdates(nil); err == nil {
		t.Fatal("expected error on 0 updates")
	}

	// getFirestoreUpdate Path with '.'
	uDot := dotUpdate{}
	if _, err := getFirestoreUpdate(uDot); err == nil {
		t.Fatal("expected error for field name with dot")
	}

	// getFirestoreUpdate no Path nor FieldPath
	uEmpty := emptyUpdate{}
	if _, err := getFirestoreUpdate(uEmpty); err == nil {
		t.Fatal("expected error for empty path and field path")
	}

	// getFirestoreUpdate FieldPath with empty string
	uEmptyPathSeg := emptyPathSegUpdate{}
	if _, err := getFirestoreUpdate(uEmptyPathSeg); err == nil {
		t.Fatal("expected error for empty field path segment")
	}

	// getFirestoreUpdate unsupported transform panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on unsupported transform")
			}
		}()
		type iface struct {
			tab  unsafe.Pointer
			data unsafe.Pointer
		}
		type rawTransform struct {
			name  string
			value any
		}
		tUnsupported := dal.Increment(1)
		rawT := (*rawTransform)((*iface)(unsafe.Pointer(&tUnsupported)).data)
		rawT.name = "unsupported_op"
		_, _ = getFirestoreUpdate(update.ByFieldName("x", tUnsupported))
	}()

	// getUpdatePreconditions with Exists
	pExists := getUpdatePreconditions([]dal.Precondition{dal.WithExistsPrecondition()})
	if len(pExists) != 1 {
		t.Fatalf("expected 1 precondition, got %d", len(pExists))
	}

	// tx.Update and tx.UpdateRecord and tx.UpdateMulti error and success
	origKeyToDocRef := keyToDocRef
	origUpdate := updateInFirestoreTransaction
	defer func() {
		keyToDocRef = origKeyToDocRef
		updateInFirestoreTransaction = origUpdate
	}()

	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "u1"}
	}
	updateInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef, _ []firestore.Update, _ ...firestore.Precondition) error {
		return nil
	}

	tx := transaction{db: database{id: "db", client: &firestore.Client{}}}
	key := dalrecord.NewKeyWithID("c", "1")
	uValid := []update.Update{update.ByFieldName("title", "new")}

	if err := tx.Update(context.Background(), key, uValid); err != nil {
		t.Fatalf("unexpected tx.Update error: %v", err)
	}

	rec := dalrecord.NewRecordWithData(key, map[string]any{"title": "old"})
	if err := tx.UpdateRecord(context.Background(), rec, uValid); err != nil {
		t.Fatalf("unexpected tx.UpdateRecord error: %v", err)
	}

	// Update with invalid updates
	if err := tx.Update(context.Background(), key, []update.Update{uDot}); err == nil {
		t.Fatal("expected error on invalid updates")
	}

	// tx.UpdateMulti success
	if err := tx.UpdateMulti(context.Background(), []*dalrecord.Key{key}, uValid); err != nil {
		t.Fatalf("unexpected tx.UpdateMulti error: %v", err)
	}

	// tx.UpdateMulti invalid updates error
	if err := tx.UpdateMulti(context.Background(), []*dalrecord.Key{key}, []update.Update{uDot}); err == nil {
		t.Fatal("expected error on invalid updates in UpdateMulti")
	}

	// tx.UpdateMulti update error
	updateInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef, _ []firestore.Update, _ ...firestore.Precondition) error {
		return errors.New("update failed")
	}
	if err := tx.UpdateMulti(context.Background(), []*dalrecord.Key{key}, uValid); err == nil {
		t.Fatal("expected error on updateInFirestoreTransaction error")
	}

	// db.Update and db.UpdateMulti
	origRunTx := runFirestoreTransaction
	defer func() { runFirestoreTransaction = origRunTx }()

	runFirestoreTransaction = func(_ *firestore.Client, ctx context.Context, f func(context.Context, *firestore.Transaction) error, _ ...firestore.TransactionOption) error {
		return f(ctx, nil)
	}
	updateInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef, _ []firestore.Update, _ ...firestore.Precondition) error {
		return nil
	}

	db := database{id: "db", client: &firestore.Client{}}
	if err := db.Update(context.Background(), key, uValid); err != nil {
		t.Fatalf("unexpected db.Update error: %v", err)
	}
	if err := db.UpdateMulti(context.Background(), []*dalrecord.Key{key}, uValid); err != nil {
		t.Fatalf("unexpected db.UpdateMulti error: %v", err)
	}
}

func TestTransaction_UnitCoverage(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origKeyToColRef := keyToCollectionRef
	origCreate := createInFirestoreTransaction
	origDelete := deleteInFirestoreTransaction
	origGet := getInFirestoreTransaction
	origGetAll := getAllInFirestoreTransaction
	origSet := setInFirestoreTransaction
	origRunTx := runFirestoreTransaction
	origDebug := Debugf
	defer func() {
		keyToDocRef = origKeyToDocRef
		keyToCollectionRef = origKeyToColRef
		createInFirestoreTransaction = origCreate
		deleteInFirestoreTransaction = origDelete
		getInFirestoreTransaction = origGet
		getAllInFirestoreTransaction = origGetAll
		setInFirestoreTransaction = origSet
		runFirestoreTransaction = origRunTx
		Debugf = origDebug
	}()

	Debugf = func(_ context.Context, _ string, _ ...any) {}

	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "k1"}
	}

	opts := dal.NewTransactionOptions(dal.TxWithReadonly())
	tx := transaction{
		db:      database{id: "db", client: &firestore.Client{}},
		options: opts,
	}

	if tx.ID() != "" {
		t.Fatal("expected empty ID")
	}
	if tx.Options() != opts {
		t.Fatal("expected matching Options")
	}

	// Insert without IDGenerator and key.ID empty -> auto generate ID
	origTxGet := getInFirestoreTransaction
	defer func() { getInFirestoreTransaction = origTxGet }()
	getInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return nil, nil
	}
	createInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef, _ interface{}) error {
		return nil
	}
	client := &firestore.Client{}
	origKeyToCol := keyToCollectionRef
	defer func() { keyToCollectionRef = origKeyToCol }()
	keyToCollectionRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.CollectionRef {
		return client.Collection("c1")
	}
	recNoID := dalrecord.NewRecordWithData(dalrecord.NewIncompleteKey("c1", reflect.String, nil), map[string]any{"v": 1})
	_ = tx.Insert(context.Background(), recNoID)

	_, _ = tx.create(context.Background(), &firestore.DocumentRef{ID: "k1"}, map[string]any{"v": 1})
	_, _ = tx.getByDocRef(context.Background(), &firestore.DocumentRef{ID: "k1"})

	// Upsert
	setInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef, _ interface{}) error {
		return nil
	}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "1"), map[string]any{"v": 1})
	if err := tx.Upsert(context.Background(), rec); err != nil {
		t.Fatalf("unexpected tx.Upsert error: %v", err)
	}

	// Set with Debugf
	if err := tx.Set(context.Background(), rec); err != nil {
		t.Fatalf("unexpected tx.Set error: %v", err)
	}

	// Delete with Debugf
	deleteInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef) error {
		return nil
	}
	if err := tx.Delete(context.Background(), rec.Key()); err != nil {
		t.Fatalf("unexpected tx.Delete error: %v", err)
	}

	// DeleteMulti success and error
	if err := tx.DeleteMulti(context.Background(), []*dalrecord.Key{rec.Key()}); err != nil {
		t.Fatalf("unexpected DeleteMulti error: %v", err)
	}
	deleteInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef) error {
		return errors.New("delete failed")
	}
	_ = tx.DeleteMulti(context.Background(), []*dalrecord.Key{rec.Key()})

	// SetMulti error path (break)
	setInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef, _ interface{}) error {
		return errors.New("set failed")
	}
	if err := tx.SetMulti(context.Background(), []dalrecord.Record{rec}); err == nil {
		t.Fatal("expected SetMulti error")
	}

	// InsertMulti
	createInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef, _ interface{}) error {
		return nil
	}
	if err := tx.InsertMulti(context.Background(), []dalrecord.Record{rec}); err != nil {
		t.Fatalf("unexpected InsertMulti error: %v", err)
	}

	// RunReadonlyTransaction and RunReadwriteTransaction
	db := database{id: "db", client: &firestore.Client{}}
	runFirestoreTransaction = func(_ *firestore.Client, ctx context.Context, f func(context.Context, *firestore.Transaction) error, _ ...firestore.TransactionOption) error {
		return f(ctx, nil)
	}
	if err := db.RunReadonlyTransaction(context.Background(), func(_ context.Context, _ dal.ReadTransaction) error {
		return nil
	}); err != nil {
		t.Fatalf("unexpected RunReadonlyTransaction error: %v", err)
	}

	// RunReadwriteTransaction success
	if err := db.RunReadwriteTransaction(context.Background(), func(_ context.Context, _ dal.ReadwriteTransaction) error {
		return nil
	}); err != nil {
		t.Fatalf("unexpected RunReadwriteTransaction error: %v", err)
	}

	// RunReadwriteTransaction AlreadyExists error mapping
	runFirestoreTransaction = func(_ *firestore.Client, _ context.Context, _ func(context.Context, *firestore.Transaction) error, _ ...firestore.TransactionOption) error {
		return status.Error(codes.AlreadyExists, "document already exists")
	}
	if err := db.RunReadwriteTransaction(context.Background(), func(_ context.Context, _ dal.ReadwriteTransaction) error {
		return nil
	}); !errors.Is(err, dalrecord.ErrRecordExists) {
		t.Fatalf("expected ErrRecordExists, got: %v", err)
	}
}

func TestGetter_ErrorHandlingAndMap(t *testing.T) {
	origKeyToDocRef := keyToDocRef
	origGetByDocRef := getByDocRef
	origGetAll := getAllFirestore
	origGetInTx := getInFirestoreTransaction
	origDebug := Debugf
	defer func() {
		keyToDocRef = origKeyToDocRef
		getByDocRef = origGetByDocRef
		getAllFirestore = origGetAll
		getInFirestoreTransaction = origGetInTx
		Debugf = origDebug
	}()

	Debugf = func(_ context.Context, _ string, _ ...any) {}

	keyToDocRef = func(_ *dalrecord.Key, _ *firestore.Client) *firestore.DocumentRef {
		return &firestore.DocumentRef{ID: "g1"}
	}

	key := dalrecord.NewKeyWithID("c", "1")
	m := make(map[string]any)
	rec := dalrecord.NewRecordWithData(key, m)

	// handleGetByKeyError nil error
	if err := handleGetByKeyError(key, nil); err != nil {
		t.Fatalf("expected nil from handleGetByKeyError(key, nil), got %v", err)
	}
	// handleGetByKeyError NotFound code
	nfErr := status.Error(codes.NotFound, "not found")
	if err := handleGetByKeyError(key, nfErr); !dalrecord.IsNotFound(err) {
		t.Fatalf("expected ErrNotFoundByKey, got %v", err)
	}

	// getAndUnmarshal with getByKey error
	getByDocRef = func(_ context.Context, _ *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return nil, nfErr
	}
	db := database{id: "db", client: &firestore.Client{}}
	if err := db.Get(context.Background(), rec); !dalrecord.IsNotFound(err) {
		t.Fatalf("expected not found error from db.Get, got %v", err)
	}

	// Exists
	if exists, err := db.Exists(context.Background(), key); exists || err != nil {
		t.Fatalf("expected exists false, err nil, got %v, %v", exists, err)
	}

	// getMulti error from getAll
	getAllFirestore = func(_ context.Context, _ *firestore.Client, _ []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
		return nil, errors.New("getAll failed")
	}
	if err := db.GetMulti(context.Background(), []dalrecord.Record{rec}); err == nil {
		t.Fatal("expected GetMulti error")
	}

	// tx.GetMulti error from getAll
	origTxGetAll := getAllInFirestoreTransaction
	defer func() { getAllInFirestoreTransaction = origTxGetAll }()
	getAllInFirestoreTransaction = func(_ *firestore.Transaction, _ []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
		return nil, errors.New("tx getAll failed")
	}
	tx := transaction{db: db}
	if err := tx.GetMulti(context.Background(), []dalrecord.Record{rec}); err == nil {
		t.Fatal("expected tx.GetMulti error")
	}

	// tx.Get and tx.Exists
	getInFirestoreTransaction = func(_ *firestore.Transaction, _ *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return nil, nfErr
	}
	if err := tx.Get(context.Background(), rec); !dalrecord.IsNotFound(err) {
		t.Fatalf("expected not found error from tx.Get, got %v", err)
	}
	if exists, err := tx.Exists(context.Background(), key); exists || err != nil {
		t.Fatalf("expected exists false, err nil, got %v, %v", exists, err)
	}
}

func TestQuery_FromAndOrderBranches(t *testing.T) {
	// dalQuery2firestoreIterator nil client panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil client")
			}
		}()
		_, _ = dalQuery2firestoreIterator(context.Background(), nil, nil)
	}()

	// non-structured query error
	if _, err := dalQuery2firestoreIterator(context.Background(), dummyQuery{}, &firestore.Client{}); err == nil {
		t.Fatal("expected error on non-structured query")
	}

	// from unknown type error
	qUnknown := dal.NewQueryBuilder(dal.From(dal.NewQuerySource(nil, "sub"))).SelectKeysOnly(reflect.String)
	if _, err := dalQuery2firestoreIterator(context.Background(), qUnknown, &firestore.Client{}); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported on unknown from type, got: %v", err)
	}

	// firestoreOrderExpression with FieldRef.IsID() == true
	idField := dal.Field(firestore.DocumentID)
	expr := firestoreOrderExpression(idField)
	if expr != firestore.DocumentID {
		t.Fatalf("expected DocumentID, got %v", expr)
	}
}

type fakeDocumentSnapshot struct {
	Ref        *firestore.DocumentRef
	CreateTime time.Time
	UpdateTime time.Time
	ReadTime   time.Time
	c          *firestore.Client
	proto      *firestorepb.Document
}

type mockDataWrapper struct {
	data any
}

func (m mockDataWrapper) Data() any {
	return m.data
}

func TestDefaultWrappers(t *testing.T) {
	recoverFunc := func(f func()) {
		defer func() { _ = recover() }()
		f()
	}

	recoverFunc(func() { _, _ = deleteByDocRef(context.Background(), &firestore.DocumentRef{}) })
	recoverFunc(func() { _, _ = createNonTransactional(context.Background(), &firestore.DocumentRef{}, nil) })
	recoverFunc(func() { _, _ = setFirestore(context.Background(), &firestore.DocumentRef{}, nil) })
	recoverFunc(func() { _, _ = getByDocRef(context.Background(), &firestore.DocumentRef{}) })
	recoverFunc(func() { _, _ = docIteratorNext(&firestore.DocumentIterator{}) })
	recoverFunc(func() { _ = setInFirestoreTransaction(&firestore.Transaction{}, &firestore.DocumentRef{}, nil) })
	recoverFunc(func() { _ = createInFirestoreTransaction(&firestore.Transaction{}, &firestore.DocumentRef{}, nil) })
	recoverFunc(func() { _ = deleteInFirestoreTransaction(&firestore.Transaction{}, &firestore.DocumentRef{}) })
	txWithErr := (&firestore.Transaction{}).WithReadOptions(firestore.ReadTime(time.Now()))
	_, _ = getInFirestoreTransaction(txWithErr, &firestore.DocumentRef{})
	recoverFunc(func() { _, _ = getAllInFirestoreTransaction(&firestore.Transaction{}, nil) })
	recoverFunc(func() { _ = updateInFirestoreTransaction(&firestore.Transaction{}, &firestore.DocumentRef{}, nil) })
	recoverFunc(func() { _ = dataTo(&firestore.DocumentSnapshot{}, nil) })
	recoverFunc(func() { _, _ = getAllFirestore(context.Background(), &firestore.Client{}, nil) })
	recoverFunc(func() { _ = newBulkWriter(context.Background(), database{client: &firestore.Client{}}) })
	recoverFunc(func() { _ = runFirestoreTransaction(&firestore.Client{}, context.Background(), nil) })
}

func TestDocSnapshotToRecord_Branches(t *testing.T) {
	key := dalrecord.NewKeyWithID("c", "1")
	recMap := dalrecord.NewRecordWithData(key, map[string]any{})

	// 1. !docSnapshot.Exists()
	snapNonExistent := &firestore.DocumentSnapshot{}
	if err := docSnapshotToRecord(snapNonExistent, recMap, dataTo); err != nil {
		t.Fatalf("expected nil error on non-existent snapshot, got %v", err)
	}
	if recMap.Exists() {
		t.Fatal("expected record not to exist for non-existent snapshot")
	}

	// 2. Existing snapshot with map data
	fakeSnap := fakeDocumentSnapshot{
		Ref: (&firestore.Client{}).Doc("c/1"),
		proto: &firestorepb.Document{
			Fields: map[string]*firestorepb.Value{
				"title": {ValueType: &firestorepb.Value_StringValue{StringValue: "test"}},
			},
		},
	}
	snapExisting := (*firestore.DocumentSnapshot)(unsafe.Pointer(&fakeSnap))

	recMap2 := dalrecord.NewRecordWithData(key, map[string]any{})
	if err := docSnapshotToRecord(snapExisting, recMap2, dataTo); err != nil {
		t.Fatalf("unexpected docSnapshotToRecord error: %v", err)
	}
	if recMap2.Data().(map[string]any)["title"] != "test" {
		t.Fatalf("expected title 'test', got %v", recMap2.Data())
	}

	// 3. Existing snapshot with non-map data, dataTo returns codes.NotFound
	recTyped := dalrecord.NewRecordWithData(key, &struct{ A int }{})
	mockDataToNotFound := func(ds *firestore.DocumentSnapshot, p interface{}) error {
		return status.Error(codes.NotFound, "not found")
	}
	if err := docSnapshotToRecord(snapExisting, recTyped, mockDataToNotFound); err == nil {
		t.Fatal("expected error from docSnapshotToRecord")
	} else if !dalrecord.IsNotFound(err) {
		t.Fatalf("expected not found error, got %v", err)
	}

	// 4. Existing snapshot with non-map data, dataTo succeeds
	mockDataToSuccess := func(ds *firestore.DocumentSnapshot, p interface{}) error {
		return nil
	}
	if err := docSnapshotToRecord(snapExisting, recTyped, mockDataToSuccess); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 5. getAndUnmarshal when docSnapshotToRecord returns error
	db := database{client: &firestore.Client{}}
	origGetByDocRef := getByDocRef
	defer func() { getByDocRef = origGetByDocRef }()
	getByDocRef = func(ctx context.Context, dr *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return snapExisting, nil
	}
	origDocSnapshotToRecord := dataTo
	defer func() { dataTo = origDocSnapshotToRecord }()
	dataTo = mockDataToNotFound

	recFail := dalrecord.NewRecordWithData(key, &struct{ A int }{})
	if err := db.Get(context.Background(), recFail); err == nil {
		t.Fatal("expected Get error when unmarshal fails")
	}

	origGetAllFirestore := getAllFirestore
	defer func() { getAllFirestore = origGetAllFirestore }()
	getAllFirestore = func(ctx context.Context, client *firestore.Client, drs []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
		return []*firestore.DocumentSnapshot{snapExisting}, nil
	}

	mockDataToErr := func(ds *firestore.DocumentSnapshot, p interface{}) error {
		return errors.New("marshal error")
	}
	dataTo = mockDataToErr
	_ = db.GetMulti(context.Background(), []dalrecord.Record{recFail})
	if recFail.Error() == nil {
		t.Fatal("expected recFail.Error() to be non-nil")
	}
}

func TestQuery_AllFromBranches(t *testing.T) {
	client := &firestore.Client{}
	ctx := context.Background()

	// 1. dal.CollectionRef value
	colVal := dal.NewRootCollectionRef("c1", "")
	qColVal := dal.NewQueryBuilder(dal.From(colVal)).SelectKeysOnly(reflect.String)
	if it, err := dalQuery2firestoreIterator(ctx, qColVal, client); err != nil || it == nil {
		t.Fatalf("expected iterator, got err: %v", err)
	}

	// 2. *dal.CollectionRef pointer
	colPtr := dal.NewRootCollectionRef("c2", "")
	qColPtr := dal.NewQueryBuilder(dal.From(&colPtr)).SelectKeysOnly(reflect.String)
	if it, err := dalQuery2firestoreIterator(ctx, qColPtr, client); err != nil || it == nil {
		t.Fatalf("expected iterator, got err: %v", err)
	}

	// 3. dal.CollectionGroupRef value
	grpVal := dal.NewCollectionGroupRef("g1", "")
	qGrpVal := dal.NewQueryBuilder(dal.From(grpVal)).SelectKeysOnly(reflect.String)
	if it, err := dalQuery2firestoreIterator(ctx, qGrpVal, client); err != nil || it == nil {
		t.Fatalf("expected iterator, got err: %v", err)
	}

	// 4. *dal.CollectionGroupRef pointer
	grpPtr := dal.NewCollectionGroupRef("g2", "")
	qGrpPtr := dal.NewQueryBuilder(dal.From(&grpPtr)).SelectKeysOnly(reflect.String)
	if it, err := dalQuery2firestoreIterator(ctx, qGrpPtr, client); err != nil || it == nil {
		t.Fatalf("expected iterator, got err: %v", err)
	}

	// 5. Query with Where error
	qWhereErr := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).
		Where(dal.WhereField("a", dal.Operator("invalid"), 1)).
		SelectKeysOnly(reflect.String)
	if _, err := dalQuery2firestoreIterator(ctx, qWhereErr, client); err == nil {
		t.Fatal("expected error from invalid where operator")
	}

	// 6. Query with OrderBy ascending and descending
	qOrder := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).
		OrderBy(dal.Ascending(dal.Field("a")), dal.Descending(dal.Field("b"))).
		SelectKeysOnly(reflect.String)
	if it, err := dalQuery2firestoreIterator(ctx, qOrder, client); err != nil || it == nil {
		t.Fatalf("expected iterator, got err: %v", err)
	}
}

func TestFirestoreReader_NextBranches(t *testing.T) {
	client := &firestore.Client{}
	ctx := context.Background()

	// NewDatabase queryExecutor getRecordsReader
	db := NewDatabase("testdb", client)
	q := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).SelectKeysOnly(reflect.String)
	r, err := db.ExecuteQueryToRecordsReader(ctx, q)
	if err != nil || r == nil {
		t.Fatalf("expected query reader, got: %v", err)
	}

	// Reader Close and Cursor
	if err := r.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
	if _, err := r.Cursor(); !errors.Is(err, dal.ErrNotImplementedYet) {
		t.Fatalf("expected ErrNotImplementedYet for Cursor, got: %v", err)
	}

	// Next() with iterator.Done
	origDocNext := docIteratorNext
	defer func() { docIteratorNext = origDocNext }()
	docIteratorNext = func(it *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
		return nil, iterator.Done
	}
	if _, err := r.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("expected ErrNoMoreRecords, got: %v", err)
	}

	// Next() with unexpected error
	docIteratorNext = func(it *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
		return nil, errors.New("read error")
	}
	if _, err := r.Next(); err == nil {
		t.Fatal("expected read error")
	}

	// Next() with DataWrapper returning nil
	fakeSnap := fakeDocumentSnapshot{
		Ref: client.Doc("c/123"),
		proto: &firestorepb.Document{
			Fields: map[string]*firestorepb.Value{
				"val": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 42}},
			},
		},
	}
	snapExisting := (*firestore.DocumentSnapshot)(unsafe.Pointer(&fakeSnap))
	docIteratorNext = func(it *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
		return snapExisting, nil
	}

	qIntoWrapperNil := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).
		SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "123"), mockDataWrapper{data: nil})
		})
	rWrapperNil, err := newFirestoreReader(ctx, client, qIntoWrapperNil)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	if _, err := rWrapperNil.Next(); err == nil {
		t.Fatal("expected error on DataWrapper.Data() returning nil")
	}

	// Next() with map[string]any data and int ID
	qIntoMap := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).
		SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithIncompleteKey("c", reflect.String, map[string]any{})
		})
	rMap, err := newFirestoreReader(ctx, client, qIntoMap)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	recRead, err := rMap.Next()
	if err != nil {
		t.Fatalf("unexpected Next error: %v", err)
	}
	if recRead.Data().(map[string]any)["val"] != int64(42) {
		t.Fatalf("expected val 42, got %v", recRead.Data())
	}

	// Next() with struct target (DataTo)
	qIntoStruct := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).
		SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithIncompleteKey("c", reflect.String, &struct{ Val int }{})
		})
	rStruct, err := newFirestoreReader(ctx, client, qIntoStruct)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	if _, err := rStruct.Next(); err != nil {
		t.Fatalf("unexpected Next error with struct: %v", err)
	}

	// Next() with non-pointer data so doc.DataTo fails
	qIntoNonPtr := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("c", ""))).
		SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithIncompleteKey("c", reflect.String, struct{ Val int }{})
		})
	rNonPtr, err := newFirestoreReader(ctx, client, qIntoNonPtr)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	if _, err := rNonPtr.Next(); err == nil {
		t.Fatal("expected error on non-pointer struct in Next()")
	}
}
