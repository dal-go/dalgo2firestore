package dalgo2firestore

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
)

func dalQuery2firestoreIterator(c context.Context, q dal.Query, client *firestore.Client) (docIterator *firestore.DocumentIterator, err error) {
	if client == nil {
		panic("client is a required parameter, got nil")
	}

	var query firestore.Query

	switch q := q.(type) {
	case dal.StructuredQuery:
		switch from := q.From().Base().(type) {
		case dal.CollectionRef:
			collectionPath := from.Path()
			query = client.Collection(collectionPath).Query
		case *dal.CollectionRef:
			collectionPath := from.Path()
			query = client.Collection(collectionPath).Query
		case dal.CollectionGroupRef:
			query = client.CollectionGroup(from.Name()).Query
		case *dal.CollectionGroupRef:
			query = client.CollectionGroup(from.Name()).Query
		default:
			err = fmt.Errorf("%w: query.From() return unknonw type: %T", dal.ErrNotSupported, from)
			return
		}

		if limit := q.Limit(); limit > 0 {
			query.Limit(limit)
		}
		if offset := q.Offset(); offset > 0 {
			query.Offset(offset)
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
	default:
		err = fmt.Errorf("only dal.StructuredQueries are supported, got %T ", q)
		return
	}
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

// dalOperator2firestore maps dalgo comparison operators to the operator strings
// accepted by the Firestore Go SDK. Most dalgo operators use the same spelling,
// but dal.In is "In" while Firestore expects "in".
var dalOperator2firestore = map[dal.Operator]string{
	dal.Equal:          "==",
	dal.GreaterThen:    ">",
	dal.GreaterOrEqual: ">=",
	dal.LessThen:       "<",
	dal.LessOrEqual:    "<=",
	dal.In:             "in",
}

func applyWhere(where dal.Condition, q firestore.Query) (firestore.Query, error) {
	var applyComparison = func(comparison dal.Comparison) error {
		switch left := comparison.Left.(type) {
		case dal.FieldRef:
			switch right := comparison.Right.(type) {
			case dal.Constant:
				operator, ok := dalOperator2firestore[comparison.Operator]
				if !ok {
					return fmt.Errorf("%w: operator %q is not supported by Firestore", dal.ErrNotSupported, comparison.Operator)
				}
				q = q.Where(left.Name(), operator, right.Value)
			case dal.Array:
				q = q.Where(left.Name(), "array-contains-any", right.Value)
			default:
				return fmt.Errorf("only FieldRef are supported as left operand, got: %T", right)
			}
		case dal.Constant:
			switch right := comparison.Right.(type) {
			case dal.FieldRef:
				switch comparison.Operator {
				case dal.In:
					q = q.Where(right.Name(), "array-contains", left.Value)
				default:
					return fmt.Errorf("only IN operator is supported for constant as left operand, got: %v", comparison.Operator)
				}
			default:
				return fmt.Errorf("only FieldRef is supported as right operand of a constant, got: %T", right)
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
