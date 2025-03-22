package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"time"
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
	idGenerator := options.IDGenerator()
	key := record.Key()
	if key.ID == nil && idGenerator != nil {
		key.ID = idGenerator(ctx, record)
	}
	dr := keyToDocRef(key, tx.db.client)
	record.SetError(nil) // Mark record as not having an error
	data := record.Data()
	err = tx.tx.Create(dr, data)
	if Debugf != nil {
		Debugf(ctx, "tx.Insert(%v) completed in %v, err: %v", key, time.Since(started), err)
	}
	return
}

func (tx transaction) Upsert(ctx context.Context, record dal.Record) error {
	return tx.Set(ctx, record)
}

func (tx transaction) Get(ctx context.Context, record dal.Record) error {
	return get(ctx, record, tx.db.client, func(_ context.Context, dr *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
		return tx.tx.Get(dr)
	})
}

func get(
	ctx context.Context,
	record dal.Record,
	client *firestore.Client,
	getByDocRef func(ctx context.Context, dr *firestore.DocumentRef) (*firestore.DocumentSnapshot, error),
) (err error) {
	var started time.Time
	if Debugf != nil {
		started = time.Now()
	}
	key := record.Key()
	docRef := keyToDocRef(key, client)
	var docSnapshot *firestore.DocumentSnapshot
	docSnapshot, err = getByDocRef(ctx, docRef)
	err = docSnapshotToRecord(err, docSnapshot, record, dataTo)
	if Debugf != nil {
		Debugf(ctx, "get(%v) completed in %v, err: %v", key, time.Since(started), err)
	}
	return
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
		if err = docSnapshotToRecord(nil, d, records[i], dataTo); err != nil {
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
	_, err = insertMulti(ctx, tx.db, records, func(ctx context.Context, docRef *firestore.DocumentRef, data any) (result *firestore.WriteResult, err error) {
		return nil, tx.tx.Create(docRef, data)
	}, opts...)
	logMultiRecords(ctx, "tx.InsertMulti", records, started, err)
	return
}
