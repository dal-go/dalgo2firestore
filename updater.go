package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
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
	update []dal.Update,
	preconditions ...dal.Precondition,
) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, d dal.ReadwriteTransaction) error {
		return d.Update(ctx, key, update, preconditions...)
	})
}

func (db database) UpdateMulti(
	ctx context.Context,
	keys []*dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.UpdateMulti(ctx, keys, updates, preconditions...)
	})
}

func (tx transaction) Update(
	_ context.Context,
	key *dal.Key,
	updates []dal.Update,
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

func (tx transaction) UpdateMulti(
	_ context.Context,
	keys []*dal.Key,
	updates []dal.Update,
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
