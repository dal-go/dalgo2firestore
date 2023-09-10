package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

func (db database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	options = append(options, dal.TxWithReadonly())
	firestoreTxOptions := createFirestoreTransactionOptions(options)
	return db.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		return f(ctx, transaction{db: db, tx: tx, QueryExecutor: db.QueryExecutor})
	}, firestoreTxOptions...)
}

func (db database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	firestoreTxOptions := createFirestoreTransactionOptions(options)
	return db.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		return f(ctx, transaction{db: db, tx: tx, QueryExecutor: db.QueryExecutor})
	}, firestoreTxOptions...)
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

func (t transaction) Close(ctx context.Context) error {
	panic("TODO: implement or remove me")
}

func (t transaction) ID() string {
	return ""
}

func (t transaction) Options() dal.TransactionOptions {
	return t.options
}

func (t transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	options := dal.NewInsertOptions(opts...)
	idGenerator := options.IDGenerator()
	key := record.Key()
	if key.ID == nil {
		key.ID = idGenerator(ctx, record)
	}
	dr := t.db.keyToDocRef(key)
	record.SetError(nil) // Mark record as not having an error
	data := record.Data()
	return t.tx.Create(dr, data)
}

func (t transaction) Upsert(_ context.Context, record dal.Record) error {
	dr := t.db.keyToDocRef(record.Key())
	return t.tx.Set(dr, record.Data())
}

func (t transaction) Get(_ context.Context, record dal.Record) error {
	key := record.Key()
	docRef := t.db.keyToDocRef(key)
	docSnapshot, err := t.tx.Get(docRef)
	return docSnapshotToRecord(err, docSnapshot, record, func(ds *firestore.DocumentSnapshot, p interface{}) error {
		return ds.DataTo(p)
	})
}

func (t transaction) Set(ctx context.Context, record dal.Record) error {
	dr := t.db.keyToDocRef(record.Key())
	return t.tx.Set(dr, record.Data())
}

func (t transaction) Delete(ctx context.Context, key *dal.Key) error {
	dr := t.db.keyToDocRef(key)
	return t.tx.Delete(dr)
}

func (t transaction) GetMulti(ctx context.Context, records []dal.Record) error {
	dr := make([]*firestore.DocumentRef, len(records))
	for i, r := range records {
		dr[i] = t.db.keyToDocRef(r.Key())
	}
	ds, err := t.tx.GetAll(dr)
	if err != nil {
		return err
	}
	for i, d := range ds {
		err = docSnapshotToRecord(nil, d, records[i], func(ds *firestore.DocumentSnapshot, p interface{}) error {
			return ds.DataTo(p)
		})
		if err != nil && !dal.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (t transaction) SetMulti(ctx context.Context, records []dal.Record) error {
	for _, record := range records { // TODO: can we do this in parallel?
		doc := t.db.keyToDocRef(record.Key())
		record.SetError(nil) // Mark record as not having an error
		_, err := doc.Set(ctx, record.Data())
		if err != nil {
			record.SetError(err)
			return err
		}
	}
	return nil
}

func (t transaction) DeleteMulti(_ context.Context, keys []*dal.Key) error {
	for _, k := range keys {
		dr := t.db.keyToDocRef(k)
		if err := t.tx.Delete(dr); err != nil {
			return fmt.Errorf("failed to delete record: %w", err)
		}
	}
	return nil
}
