package dalgo2firestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
)

func (db database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	options = append(options, dal.TxWithReadonly())
	firestoreTxOptions := createFirestoreTransactionOptions(options)
	err = db.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		return f(ctx, transaction{db: db, tx: tx, QueryExecutor: db.QueryExecutor})
	}, firestoreTxOptions...)
	if Debugf != nil {
		Debugf(ctx, "RunReadonlyTransaction() completed in %v, err: %v", time.Since(started), err)
	}
	return
}

func (db database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	firestoreTxOptions := createFirestoreTransactionOptions(options)
	err = db.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		return f(ctx, transaction{db: db, tx: tx, QueryExecutor: db.QueryExecutor})
	}, firestoreTxOptions...)
	if Debugf != nil {
		Debugf(ctx, "RunReadwriteTransaction() completed in %v, err: %v", time.Since(started), err)
	}
	return err
}

func createFirestoreTransactionOptions(opts []dal.TransactionOption) (options []firestore.TransactionOption) {
	to := dal.NewTransactionOptions(opts...)
	if to.IsReadonly() {
		options = append(options, firestore.ReadOnly)
	}
	return
}

var _ dal.Transaction = (*transaction)(nil)
var _ dal.ReadwriteTransaction = (*transaction)(nil)

type transaction struct {
	db      database
	tx      *firestore.Transaction
	options dal.TransactionOptions
	dal.QueryExecutor
}

func (tx transaction) Close(_ context.Context) error {
	panic("TODO: implement or remove me")
}

func (tx transaction) ID() string {
	return ""
}

func (tx transaction) Options() dal.TransactionOptions {
	return tx.options
}

func (tx transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	options := dal.NewInsertOptions(opts...)
	if options.IDGenerator() == nil {
		// Preserve historical transactional behavior: an incomplete key always gets
		// Firestore's native client-side auto-generated ID, even without
		// dal.WithAdapterGeneratedID. The ID is assigned to the dalgo key BEFORE
		// writing so the key reflects the stored document, and the write itself
		// goes through tx.tx.Create so it stays part of the transaction
		// (the previous CollectionRef.Add wrote outside the transaction).
		if key := record.Key(); key.ID == nil || key.ID == "" {
			key.ID = keyToCollectionRef(key, tx.db.client).NewDoc().ID
		}
	}
	err = insertWithOptions(ctx, tx.db, record, tx.create, options)
	if Debugf != nil {
		Debugf(ctx, "tx.Insert(%v) completed in %v, err: %v", record.Key(), time.Since(started), err)
	}
	return
}

// create adapts firestore.Transaction.Create to the createFunc signature.
// Transactional writes return no per-write result.
func (tx transaction) create(_ context.Context, docRef *firestore.DocumentRef, data interface{}) (*firestore.WriteResult, error) {
	return nil, tx.tx.Create(docRef, data)
}

func (tx transaction) Upsert(ctx context.Context, record dal.Record) error {
	return tx.Set(ctx, record)
}

func (tx transaction) getByDocRef(_ context.Context, dr *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
	return tx.tx.Get(dr)
}

func (tx transaction) Get(ctx context.Context, record dal.Record) error {
	return getAndUnmarshal(ctx, record, tx.db.client, tx.getByDocRef)
}

func (tx transaction) Exists(ctx context.Context, key *dal.Key) (exists bool, err error) {
	return existsByKey(ctx, key, tx.db.client, tx.getByDocRef)
}

func (tx transaction) Set(ctx context.Context, record dal.Record) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	key := record.Key()
	dr := keyToDocRef(key, tx.db.client)
	err = tx.tx.Set(dr, record.Data())
	if Debugf != nil {
		Debugf(ctx, "tx.Set(%v) completed in %v, err: %v", key, time.Since(started), err)
	}
	return err
}

func (tx transaction) Delete(ctx context.Context, key *dal.Key) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	dr := keyToDocRef(key, tx.db.client)
	err = tx.tx.Delete(dr)
	if Debugf != nil {
		Debugf(ctx, "tx.Delete(%v) completed in %v, err: %v", key, time.Since(started), err)
	}
	return
}

func (tx transaction) GetMulti(ctx context.Context, records []dal.Record) error {
	return getMulti(ctx, records, "tx", tx.db.client,
		func(_ context.Context, drs []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
			return tx.tx.GetAll(drs)
		},
	)
}

func getMulti(
	ctx context.Context,
	records []dal.Record,
	caller string,
	client *firestore.Client,
	getAll func(ctx context.Context, drs []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error),
) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	dr := make([]*firestore.DocumentRef, len(records))
	for i, r := range records {
		dr[i] = keyToDocRef(r.Key(), client)
	}

	var ds []*firestore.DocumentSnapshot
	if ds, err = getAll(ctx, dr); err != nil {
		return fmt.Errorf("failed to getFirestore %d records by keys: %w", len(records), err)
	}

	var errs []error
	for i, d := range ds {
		if err = docSnapshotToRecord(d, records[i], dataTo); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		err = fmt.Errorf(caller+".getMulti() failed to getFirestore %d out of %d records requested by keys: %w", len(errs), len(records), errors.Join(errs...))
	}

	logMultiRecords(ctx, caller+".GetMulti", records, started, err)

	return nil

}

func (tx transaction) SetMulti(ctx context.Context, records []dal.Record) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	for _, record := range records { // TODO: can we do this in parallel?
		doc := keyToDocRef(record.Key(), tx.db.client)
		record.SetError(nil) // Mark record as not having an error
		_, err = doc.Set(ctx, record.Data())
		if err != nil {
			record.SetError(err)
			break
		}
	}
	logMultiRecords(ctx, "tx.SetMulti", records, started, err)
	return nil
}

func (tx transaction) DeleteMulti(ctx context.Context, keys []*dal.Key) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	for _, k := range keys {
		dr := keyToDocRef(k, tx.db.client)
		if err = tx.tx.Delete(dr); err != nil {
			err = fmt.Errorf("failed to deleteByDocRef record: %w", err)
			break
		}
	}
	logMultiKeys(ctx, "tx.DeleteMulti", keys, started, err)
	return nil
}

func (tx transaction) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	err = insertMulti(ctx, tx.db, records, tx.create, opts...)
	logMultiRecords(ctx, "tx.InsertMulti", records, started, err)
	return
}
