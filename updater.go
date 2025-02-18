package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
)

//type updater struct {
//	db *database
//}
//
//func newUpdater(db *database) updater {
//	return updater{
//		db: db,
//	}
//}

func (db database) Update(
	ctx context.Context,
	key *dal.Key,
	update []update.Update,
	preconditions ...dal.Precondition,
) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, d dal.ReadwriteTransaction) error {
		return d.Update(ctx, key, update, preconditions...)
	})
}

func (db database) UpdateMulti(
	ctx context.Context,
	keys []*dal.Key,
	updates []update.Update,
	preconditions ...dal.Precondition,
) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.UpdateMulti(ctx, keys, updates, preconditions...)
	})
}

func (tx transaction) Update(
	_ context.Context,
	key *dal.Key,
	updates []update.Update,
	preconditions ...dal.Precondition,
) error {
	dr := keyToDocRef(key, tx.db.client)
	fsUpdates := make([]firestore.Update, len(updates))
	for i, u := range updates {
		fsUpdates[i] = getFirestoreUpdate(u)
	}
	fsPreconditions := getUpdatePreconditions(preconditions)
	return tx.tx.Update(dr, fsUpdates, fsPreconditions...)
}

func (tx transaction) UpdateRecord(ctx context.Context, record dal.Record, updates []update.Update, preconditions ...dal.Precondition) error {
	return tx.Update(ctx, record.Key(), updates, preconditions...)
}

func (tx transaction) UpdateMulti(
	_ context.Context,
	keys []*dal.Key,
	updates []update.Update,
	preconditions ...dal.Precondition,
) error {
	fsPreconditions := getUpdatePreconditions(preconditions)
	for _, key := range keys {
		dr := keyToDocRef(key, tx.db.client)
		fsUpdates := make([]firestore.Update, len(updates))
		for i, u := range updates {
			fsUpdates[i] = getFirestoreUpdate(u)
		}
		if err := tx.tx.Update(dr, fsUpdates, fsPreconditions...); err != nil {
			keyPath := PathFromKey(key)
			return fmt.Errorf("failed to update record with key: %v: %w", keyPath, err)
		}
	}
	return nil
}

func getFirestoreUpdate(u update.Update) firestore.Update {
	value := u.Value()
	if value == update.DeleteField {
		value = firestore.Delete
	} else if transform, ok := dal.IsTransform(value); ok {
		name := transform.Name()
		switch name {
		case "increment":
			value = firestore.Increment(transform.Value())
		default:
			panic("unsupported transform operation: " + name)
		}
	}
	return firestore.Update{
		Path:      u.FieldName(),
		FieldPath: (firestore.FieldPath)(u.FieldPath()),
		Value:     value,
	}
}

func getUpdatePreconditions(preconditions []dal.Precondition) (fsPreconditions []firestore.Precondition) {
	p := dal.GetPreconditions(preconditions...)
	if p.Exists() {
		fsPreconditions = []firestore.Precondition{firestore.Exists}
	}
	return fsPreconditions
}
