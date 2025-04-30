package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
	"strings"
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
	fsUpdates, err := getFirestoreUpdates(updates)
	if err != nil {
		return fmt.Errorf("updates for record with key=%s is invalid: %w", key, err)
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
	fsUpdates, err := getFirestoreUpdates(updates)
	if err != nil {
		ks := make([]string, len(keys))
		for i, key := range keys {
			ks[i] = key.String()
		}
		return fmt.Errorf("updates for records with keys=[%s] is invalid: %w", strings.Join(ks, ","), err)
	}
	for _, key := range keys {
		dr := keyToDocRef(key, tx.db.client)
		if err := tx.tx.Update(dr, fsUpdates, fsPreconditions...); err != nil {
			keyPath := PathFromKey(key)
			return fmt.Errorf("failed to update record with key=%s (path=%s): %w", key, keyPath, err)
		}
	}
	return nil
}

func getFirestoreUpdates(updates []update.Update) (fsUpdates []firestore.Update, err error) {
	fsUpdates = make([]firestore.Update, len(updates))
	for i, u := range updates {
		if fsUpdate, err := getFirestoreUpdate(u); err != nil {
			return nil, fmt.Errorf("updates[%d] is invalid: %w", i, err)
		} else {
			fsUpdates[i] = fsUpdate
		}
	}
	return fsUpdates, nil
}

func getFirestoreUpdate(u update.Update) (firestore.Update, error) {
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
	fsUpdate := firestore.Update{
		Path:      u.FieldName(),
		FieldPath: (firestore.FieldPath)(u.FieldPath()),
		Value:     value,
	}
	if fsUpdate.Path == "" && len(fsUpdate.FieldPath) == 0 {
		return fsUpdate, errors.New("has no Path nor FieldPath")
	}
	return fsUpdate, nil
}

func getUpdatePreconditions(preconditions []dal.Precondition) (fsPreconditions []firestore.Precondition) {
	p := dal.GetPreconditions(preconditions...)
	if p.Exists() {
		fsPreconditions = []firestore.Precondition{firestore.Exists}
	}
	return fsPreconditions
}
