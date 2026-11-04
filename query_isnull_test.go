package dalgo2firestore

import (
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
)

func Test_applyWhere_isNull(t *testing.T) {
	baseQuery := (&firestore.Client{}).Collection("c").Query
	notNull := func(f string) dal.Condition { return dal.NewIsNotNullCondition(testFieldRef(t, f)) }

	t.Run("is_not_null_top_level", func(t *testing.T) {
		actual, err := applyWhere(notNull("f"), baseQuery)
		if err != nil {
			t.Fatal(err)
		}
		if want := baseQuery.Where("f", "!=", nil); !reflect.DeepEqual(actual, want) {
			t.Fatalf("got %+v want %+v", actual, want)
		}
	})

	t.Run("is_not_null_in_and_group", func(t *testing.T) {
		cond := dal.NewGroupCondition(dal.And, dal.WhereField("a", dal.Equal, 1), notNull("b"))
		actual, err := applyWhere(cond, baseQuery)
		if err != nil {
			t.Fatal(err)
		}
		if want := baseQuery.Where("a", "==", 1).Where("b", "!=", nil); !reflect.DeepEqual(actual, want) {
			t.Fatalf("got %+v want %+v", actual, want)
		}
	})

	for _, tt := range []struct {
		name string
		cond dal.Condition
	}{
		{"is_null_rejected", dal.NewIsNullCondition(testFieldRef(t, "f"))},
		{"is_null_in_group_rejected", dal.NewGroupCondition(dal.And, dal.NewIsNullCondition(testFieldRef(t, "f")))},
		{"non_field_operand_rejected", dal.NewIsNotNullCondition(dal.Constant{Value: 1})},
		{"non_field_operand_in_group_rejected", dal.NewGroupCondition(dal.And, dal.NewIsNotNullCondition(dal.Constant{Value: 1}))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := applyWhere(tt.cond, baseQuery); !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("expected ErrNotSupported, got %v", err)
			}
		})
	}
}
