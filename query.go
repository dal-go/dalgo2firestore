package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

func dalQuery2firestoreIterator(c context.Context, q dal.Query, client *firestore.Client) (docIterator *firestore.DocumentIterator, err error) {
	if client == nil {
		panic("client is a required parameter, got nil")
	}
	query := client.Collection(q.From().Name).Offset(q.Offset())
	if limit := q.Limit(); limit > 0 {
		query.Limit(limit)
	}
	if startFrom := q.StartFrom(); startFrom != "" {
		query.StartAt(startFrom)
	}
	if where := q.Where(); where != nil {
		if query, err = applyWhere(where, query); err != nil {
			return
		}
	}
	if orderBy := q.OrderBy(); orderBy != nil {
		if query, err = applyOrderBy(orderBy, query); err != nil {
			return
		}
	}
	return query.Documents(c), err
}

func applyOrderBy(orderBy []dal.OrderExpression, q firestore.Query) (firestore.Query, error) {
	for _, o := range orderBy {
		expression := o.Expression().String()
		if o.Descending() {
			q = q.OrderBy(expression, firestore.Desc)
		} else {
			q = q.OrderBy(expression, firestore.Asc)
		}
	}
	return q, nil
}

func applyWhere(where dal.Condition, q firestore.Query) (firestore.Query, error) {
	var applyComparison = func(comparison dal.Comparison) error {
		switch left := comparison.Left.(type) {
		case dal.FieldRef:
			switch right := comparison.Right.(type) {
			case dal.Constant:
				q = q.Where(left.Name, string(comparison.Operator), right.Value)
			default:
				return fmt.Errorf("only FieldRef are supported as left operand, got: %T", right)
			}
		default:
			return fmt.Errorf("only FieldRef are supported as left operand, got: %T", left)
		}
		return nil
	}

	switch cond := where.(type) {
	case dal.GroupCondition:
		if cond.Operator() != dal.And {
			return q, fmt.Errorf("only AND operator is supported in group condition, got: %v", cond.Operator())
		}
		for _, c := range cond.Conditions() {
			switch c := c.(type) {
			case dal.Comparison:
				if err := applyComparison(c); err != nil {
					return q, err
				}
			default:
				return q, fmt.Errorf("only comparisons are supported in group condition, got: %T", c)
			}
		}
		return q, nil
	case dal.Comparison:
		return q, applyComparison(cond)
	default:
		return q, fmt.Errorf("only comparison or group conditions are supported at root level of where clause, got: %T", cond)
	}
}
