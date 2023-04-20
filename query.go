package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

type queryProvider interface {
	Documents() *firestore.DocumentIterator
}

func dalQuery2firestoreIterator(c context.Context, q dal.Query, client *firestore.Client) (docIterator *firestore.DocumentIterator, err error) {
	query := client.Collection(q.From().Name).Offset(q.Offset())
	if limit := q.Limit(); limit > 0 {
		query.Limit(limit)
	}
	if startFrom := q.StartFrom(); startFrom != "" {
		query.StartAt(startFrom)
	}
	if query, err = dalWhereToFirestoreWhere(q.Where(), query); err != nil {
		return
	}
	return query.Documents(c), err
}

func dalWhereToFirestoreWhere(condition dal.Condition, query firestore.Query) (firestore.Query, error) {
	if condition == nil {
		return query, nil
	}
	switch cond := condition.(type) {
	case dal.Comparison:
		switch right := cond.Right.(type) {
		case dal.Constant:
			query.Where(cond.Left.String(), string(cond.Operator), right.Value)
		default:
			return query, dal.ErrNotSupported
		}

	case dal.GroupCondition:
		return query, dal.ErrNotImplementedYet
	default:
		return query, fmt.Errorf("%w: %T", dal.ErrNotSupported, cond)
	}
	return query, nil
}
