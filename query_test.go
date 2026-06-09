package dalgo2firestore

import (
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
)

func testFieldRef(t *testing.T, name string) dal.FieldRef {
	t.Helper()
	comparison, ok := dal.WhereField(name, dal.Equal, 1).(dal.Comparison)
	if !ok {
		t.Fatalf("expected dal.WhereField to return dal.Comparison")
	}
	fieldRef, ok := comparison.Left.(dal.FieldRef)
	if !ok {
		t.Fatalf("expected left operand to be dal.FieldRef, got %T", comparison.Left)
	}
	return fieldRef
}

func Test_applyWhere_operator_mapping(t *testing.T) {
	baseQuery := (&firestore.Client{}).Collection("c").Query

	for _, tt := range []struct {
		name      string
		condition dal.Condition
		expected  firestore.Query
	}{
		{
			name:      "equal",
			condition: dal.WhereField("f", dal.Equal, 1),
			expected:  baseQuery.Where("f", "==", 1),
		},
		{
			name:      "greater_then",
			condition: dal.WhereField("f", dal.GreaterThen, 1),
			expected:  baseQuery.Where("f", ">", 1),
		},
		{
			name:      "greater_or_equal",
			condition: dal.WhereField("f", dal.GreaterOrEqual, 1),
			expected:  baseQuery.Where("f", ">=", 1),
		},
		{
			name:      "less_then",
			condition: dal.WhereField("f", dal.LessThen, 1),
			expected:  baseQuery.Where("f", "<", 1),
		},
		{
			name:      "less_or_equal",
			condition: dal.WhereField("f", dal.LessOrEqual, 1),
			expected:  baseQuery.Where("f", "<=", 1),
		},
		{
			// dal.In is the string "In" but the Firestore SDK only accepts "in".
			name:      "in_constant",
			condition: dal.WhereField("f", dal.In, dal.Constant{Value: []string{"a", "b"}}),
			expected:  baseQuery.Where("f", "in", []string{"a", "b"}),
		},
		{
			name:      "field_in_array_is_array_contains_any",
			condition: dal.WhereField("f", dal.In, []string{"a", "b"}),
			expected:  baseQuery.Where("f", "array-contains-any", []string{"a", "b"}),
		},
		{
			name:      "constant_in_field_is_array_contains",
			condition: dal.NewComparison(dal.Constant{Value: "v"}, dal.In, testFieldRef(t, "f")),
			expected:  baseQuery.Where("f", "array-contains", "v"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := applyWhere(tt.condition, baseQuery)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(actual, tt.expected) {
				t.Fatalf("unexpected firestore query:\n got: %+v\nwant: %+v", actual, tt.expected)
			}
		})
	}
}

func Test_applyWhere_unsupported_operator(t *testing.T) {
	baseQuery := (&firestore.Client{}).Collection("c").Query
	condition := dal.WhereField("f", dal.Operator("!="), 1)
	if _, err := applyWhere(condition, baseQuery); err == nil {
		t.Fatalf("expected error for unsupported operator")
	} else if !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("expected 'not supported' error, got: %v", err)
	}
}

func Test_applyWhere_group_condition(t *testing.T) {
	baseQuery := (&firestore.Client{}).Collection("c").Query

	t.Run("and_group_applies_all_comparisons", func(t *testing.T) {
		condition := dal.NewGroupCondition(dal.And,
			dal.WhereField("f1", dal.Equal, 1),
			dal.WhereField("f2", dal.In, dal.Constant{Value: []int{1, 2}}),
		)
		actual, err := applyWhere(condition, baseQuery)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := baseQuery.Where("f1", "==", 1).Where("f2", "in", []int{1, 2})
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("unexpected firestore query:\n got: %+v\nwant: %+v", actual, expected)
		}
	})

	t.Run("non_and_group_is_not_supported", func(t *testing.T) {
		condition := dal.NewGroupCondition(dal.Or, dal.WhereField("f", dal.Equal, 1))
		if _, err := applyWhere(condition, baseQuery); err == nil {
			t.Fatalf("expected error for OR group condition")
		}
	})

	t.Run("group_member_must_be_comparison", func(t *testing.T) {
		condition := dal.NewGroupCondition(dal.And,
			dal.NewGroupCondition(dal.And, dal.WhereField("f", dal.Equal, 1)),
		)
		if _, err := applyWhere(condition, baseQuery); err == nil {
			t.Fatalf("expected error for nested group condition")
		}
	})

	t.Run("group_member_comparison_error_is_propagated", func(t *testing.T) {
		condition := dal.NewGroupCondition(dal.And, dal.WhereField("f", dal.Operator("!="), 1))
		if _, err := applyWhere(condition, baseQuery); err == nil {
			t.Fatalf("expected error for unsupported operator in group condition")
		}
	})
}

func Test_applyWhere_unsupported_operands(t *testing.T) {
	baseQuery := (&firestore.Client{}).Collection("c").Query

	t.Run("constant_with_non_in_operator", func(t *testing.T) {
		condition := dal.NewComparison(dal.Constant{Value: "v"}, dal.Equal, testFieldRef(t, "f"))
		if _, err := applyWhere(condition, baseQuery); err == nil {
			t.Fatalf("expected error for constant left operand with non-IN operator")
		}
	})

	t.Run("constant_with_non_field_right_operand", func(t *testing.T) {
		condition := dal.NewComparison(dal.Constant{Value: "v"}, dal.In, dal.Constant{Value: "w"})
		if _, err := applyWhere(condition, baseQuery); err == nil {
			t.Fatalf("expected error for constant right operand of a constant")
		}
	})

	t.Run("field_with_field_right_operand", func(t *testing.T) {
		condition := dal.NewComparison(testFieldRef(t, "f1"), dal.Equal, testFieldRef(t, "f2"))
		if _, err := applyWhere(condition, baseQuery); err == nil {
			t.Fatalf("expected error for FieldRef right operand")
		}
	})

	t.Run("array_left_operand", func(t *testing.T) {
		condition := dal.NewComparison(dal.Array{Value: []string{"a"}}, dal.In, testFieldRef(t, "f"))
		if _, err := applyWhere(condition, baseQuery); err == nil {
			t.Fatalf("expected error for Array left operand")
		}
	})
}

func Test_applyWhere_unsupported_root_condition(t *testing.T) {
	baseQuery := (&firestore.Client{}).Collection("c").Query
	if _, err := applyWhere(nil, baseQuery); err == nil {
		t.Fatalf("expected error for unsupported root condition type")
	}
}
