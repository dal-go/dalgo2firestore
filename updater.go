package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

//type updater struct {
//	db *Database
//}
//
//func newUpdater(db *Database) updater {
//	return updater{
//		db: db,
//	}
//}

func (db Database) Update(
	ctx context.Context,
	key *dal.Key,
	update []dal.Update,
	preconditions ...dal.Precondition,
) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, d dal.ReadwriteTransaction) error {
		return d.Update(ctx, key, update, preconditions...)
	})
}

func (db Database) UpdateMulti(
	ctx context.Context,
	keys []*dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.UpdateMulti(ctx, keys, updates, preconditions...)
	})
}

func (t transaction) Update(
	_ context.Context,
	key *dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	dr := keyToDocRef(key, t.db.client)
	fsUpdates := make([]firestore.Update, len(updates))
	for i, u := range updates {
		fsUpdates[i] = getFirestoreUpdate(u)
	}
	fsPreconditions := getUpdatePreconditions(preconditions)
	return t.tx.Update(dr, fsUpdates, fsPreconditions...)
}

func (t transaction) UpdateMulti(
	_ context.Context,
	keys []*dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	fsPreconditions := getUpdatePreconditions(preconditions)
	for _, key := range keys {
		dr := keyToDocRef(key, t.db.client)
		fsUpdates := make([]firestore.Update, len(updates))
		for i, u := range updates {
			fsUpdates[i] = getFirestoreUpdate(u)
		}
		if err := t.tx.Update(dr, fsUpdates, fsPreconditions...); err != nil {
			keyPath := PathFromKey(key)
			return fmt.Errorf("failed to update record with key: %v: %w", keyPath, err)
		}
	}
	return nil
}

func getFirestoreUpdate(update dal.Update) firestore.Update {
	value := update.Value
	if value == dal.DeleteField {
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
		Path:      update.Field,
		FieldPath: (firestore.FieldPath)(update.FieldPath),
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
