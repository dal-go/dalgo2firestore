package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/strongo/dalgo/dal"
)

type updater struct {
	dtb *database
}

func newUpdater(dtb *database) updater {
	return updater{
		dtb: dtb,
	}
}

func (u updater) Update(
	ctx context.Context,
	key *dal.Key,
	update []dal.Update,
	preconditions ...dal.Precondition,
) error {
	return u.dtb.RunReadwriteTransaction(ctx, func(ctx context.Context, d dal.ReadwriteTransaction) error {
		return d.Update(ctx, key, update, preconditions...)
	})
}

func (u updater) UpdateMulti(
	ctx context.Context,
	keys []*dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	return u.dtb.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.UpdateMulti(ctx, keys, updates, preconditions...)
	})
}

func (t transaction) Update(
	_ context.Context,
	key *dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	dr := t.dtb.doc(key)
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
		dr := t.dtb.doc(key)
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
	if transform, ok := dal.IsTransform(value); ok {
		name := transform.Name()
		switch name {
		case "increment":
			value = firestore.Increment(value)
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
