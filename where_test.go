package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Where(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		operator       string
		values         []any
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE condition",
			initialWheres: []where{},
			column:        "id",
			operator:      "=",
			values:        []any{1},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
		},
		{
			name: "should add a second WHERE condition with AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column:   "name",
			operator: "=",
			values:   []any{"John"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryBasic, column: "name", operator: "=", conj: "AND", args: []any{"John"}},
			},
		},
		{
			name:     "should default to AND when conjunction is missing",
			column:   "status",
			operator: "=",
			values:   []any{"active"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
			},
		},
		{
			name:          "should handle different operators",
			initialWheres: []where{},
			column:        "age",
			operator:      ">",
			values:        []any{18},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "age", operator: ">", conj: "AND", args: []any{18}},
			},
		},
		{
			name:          "should handle IN operator",
			initialWheres: []where{},
			column:        "category",
			operator:      "IN",
			values:        []any{"A", "B"},
			expectedWheres: []where{
				{queryType: QueryIn, column: "category", operator: "IN", conj: "AND", args: []any{"A", "B"}},
			},
		},
		{
			name:          "should handle NOT IN operator",
			initialWheres: []where{},
			column:        "order_id",
			operator:      "NOT IN",
			values:        []any{1, 2, 3},
			expectedWheres: []where{
				{queryType: QueryIn, column: "order_id", operator: "NOT IN", conj: "AND", args: []any{1, 2, 3}},
			},
		},
		{
			name:          "should handle IS NULL operator",
			initialWheres: []where{},
			column:        "deleted_at",
			operator:      "IS NULL",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryNull, column: "deleted_at", operator: "IS NULL", conj: "AND", args: []any{}},
			},
		},
		{
			name:          "should handle IS NOT NULL operator",
			initialWheres: []where{},
			column:        "updated_at",
			operator:      "IS NOT NULL",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryNull, column: "updated_at", operator: "IS NOT NULL", conj: "AND", args: []any{}},
			},
		},
		{
			name:          "should handle BETWEEN operator",
			initialWheres: []where{},
			column:        "age",
			operator:      "BETWEEN",
			values:        []any{18, 30},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "AND", args: []any{18, 30}},
			},
		},
		{
			name:          "should handle BETWEEN operator with slice",
			initialWheres: []where{},
			column:        "quantity",
			operator:      "BETWEEN",
			values:        []any{[]int{50, 100}},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "quantity", operator: "BETWEEN", conj: "AND", args: []any{50, 100}},
			},
		},
		{
			name:          "should handle NOT BETWEEN operator",
			initialWheres: []where{},
			column:        "price",
			operator:      "NOT BETWEEN",
			values:        []any{100, 200},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "price", operator: "NOT BETWEEN", conj: "AND", args: []any{100, 200}},
			},
		},
		{
			name:          "should handle NOT BETWEEN operator with slice",
			initialWheres: []where{},
			column:        "quantity",
			operator:      "NOT BETWEEN",
			values:        []any{[]int{50, 100}},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "quantity", operator: "NOT BETWEEN", conj: "AND", args: []any{50, 100}},
			},
		},
		{
			name:          "should handle empty values slice for basic operator",
			initialWheres: []where{},
			column:        "id",
			operator:      "=",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{}},
			},
		},
		{
			name:          "should handle nil values for basic operator",
			initialWheres: []where{},
			column:        "id",
			operator:      "=",
			values:        nil,
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{}},
			},
		},
		{
			name:          "should ignore values for IS NULL operator",
			initialWheres: []where{},
			column:        "deleted_at",
			operator:      "IS NULL",
			values:        []any{1, 2, 3}, // These should be ignored
			expectedWheres: []where{
				{queryType: QueryNull, column: "deleted_at", operator: "IS NULL", conj: "AND", args: []any{}},
			},
		},
		{
			name:          "should ignore values for IS NOT NULL operator",
			initialWheres: []where{},
			column:        "updated_at",
			operator:      "IS NOT NULL",
			values:        []any{1, 2, 3}, // These should be ignored
			expectedWheres: []where{
				{queryType: QueryNull, column: "updated_at", operator: "IS NOT NULL", conj: "AND", args: []any{}},
			},
		},
		{
			name:     "should handle nil values for BETWEEN operator",
			column:   "age",
			operator: "BETWEEN",
			values:   nil,
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "AND", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle empty values slice for BETWEEN operator",
			column:   "age",
			operator: "BETWEEN",
			values:   []any{},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "AND", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle single value for BETWEEN operator",
			column:   "age",
			operator: "BETWEEN",
			values:   []any{10},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "AND", args: []any{10, nil}},
			},
		},
		{
			name:     "should handle more than 2 values for BETWEEN operator (values are ignored)",
			column:   "age",
			operator: "BETWEEN",
			values:   []any{10, 18, 25},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "AND", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle nil values for NOT BETWEEN operator",
			column:   "age",
			operator: "NOT BETWEEN",
			values:   nil,
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "AND", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle empty values slice for NOT BETWEEN operator",
			column:   "age",
			operator: "NOT BETWEEN",
			values:   []any{},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "AND", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle single value slice for NOT BETWEEN operator",
			column:   "age",
			operator: "NOT BETWEEN",
			values:   []any{10},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "AND", args: []any{10, nil}},
			},
		},
		{
			name:     "should handle more than 2 values slice for NOT BETWEEN operator (values are ignored)",
			column:   "age",
			operator: "NOT BETWEEN",
			values:   []any{10, 18, 25},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "AND", args: []any{nil, nil}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.Where(tt.column, tt.operator, tt.values...)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected Where() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhere(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		operator       string
		values         []any
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE condition",
			initialWheres: []where{},
			column:        "id",
			operator:      "=",
			values:        []any{1},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "OR", args: []any{1}},
			},
		},
		{
			name: "should add a second OR WHERE condition after an AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column:   "name",
			operator: "=",
			values:   []any{"John"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryBasic, column: "name", operator: "=", conj: "OR", args: []any{"John"}},
			},
		},
		{
			name:          "should handle different operators with OR",
			initialWheres: []where{},
			column:        "age",
			operator:      ">",
			values:        []any{18},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "age", operator: ">", conj: "OR", args: []any{18}},
			},
		},
		{
			name:          "should handle IN operator with OR",
			initialWheres: []where{},
			column:        "category",
			operator:      "IN",
			values:        []any{"A", "B"},
			expectedWheres: []where{
				{queryType: QueryIn, column: "category", operator: "IN", conj: "OR", args: []any{"A", "B"}},
			},
		},
		{
			name:           "should handle NOT IN operator with OR",
			initialWheres:  []where{},
			column:         "order_id",
			operator:       "NOT IN",
			values:         []any{1, 2, 3},
			expectedWheres: []where{{queryType: QueryIn, column: "order_id", operator: "NOT IN", conj: "OR", args: []any{1, 2, 3}}},
		},
		{
			name:          "should handle IS NULL operator with OR",
			initialWheres: []where{},
			column:        "deleted_at",
			operator:      "IS NULL",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryNull, column: "deleted_at", operator: "IS NULL", conj: "OR", args: []any{}},
			},
		},
		{
			name:          "should handle IS NOT NULL operator with OR",
			initialWheres: []where{},
			column:        "updated_at",
			operator:      "IS NOT NULL",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryNull, column: "updated_at", operator: "IS NOT NULL", conj: "OR", args: []any{}},
			},
		},
		{
			name:          "should handle BETWEEN operator with OR",
			initialWheres: []where{},
			column:        "age",
			operator:      "BETWEEN",
			values:        []any{18, 30},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "OR", args: []any{18, 30}},
			},
		},
		{
			name:          "should handle NOT BETWEEN operator with OR",
			initialWheres: []where{},
			column:        "price",
			operator:      "NOT BETWEEN",
			values:        []any{100, 200},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "price", operator: "NOT BETWEEN", conj: "OR", args: []any{100, 200}},
			},
		},
		{
			name:           "should handle empty values slice for basic operator with OR",
			initialWheres:  []where{},
			column:         "id",
			operator:       "=",
			values:         []any{},
			expectedWheres: []where{{queryType: QueryBasic, column: "id", operator: "=", conj: "OR", args: []any{}}},
		},
		{
			name:          "should handle nil values for basic operator with OR",
			initialWheres: []where{},
			column:        "id",
			operator:      "=",
			values:        nil,
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "OR", args: []any{}},
			},
		},
		{
			name:          "should ignore values for IS NULL operator with OR",
			initialWheres: []where{},
			column:        "deleted_at",
			operator:      "IS NULL",
			values:        []any{1, 2, 3}, // These should be ignored
			expectedWheres: []where{
				{queryType: QueryNull, column: "deleted_at", operator: "IS NULL", conj: "OR", args: []any{}},
			},
		},
		{
			name:          "should ignore values for IS NOT NULL operator with OR",
			initialWheres: []where{},
			column:        "updated_at",
			operator:      "IS NOT NULL",
			values:        []any{1, 2, 3}, // These should be ignored
			expectedWheres: []where{
				{queryType: QueryNull, column: "updated_at", operator: "IS NOT NULL", conj: "OR", args: []any{}},
			},
		},
		{
			name:     "should handle nil values for BETWEEN operator with OR",
			column:   "age",
			operator: "BETWEEN",
			values:   nil,
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "OR", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle empty values slice for BETWEEN operator with OR",
			column:   "age",
			operator: "BETWEEN",
			values:   []any{},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "OR", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle single value slice for BETWEEN operator with OR",
			column:   "age",
			operator: "BETWEEN",
			values:   []any{10},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "OR", args: []any{10, nil}},
			},
		},
		{
			name:     "should handle more than 2 values slice for BETWEEN operator with OR (values are ignored)",
			column:   "age",
			operator: "BETWEEN",
			values:   []any{10, 18, 25},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "OR", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle nil values for NOT BETWEEN operator with OR",
			column:   "age",
			operator: "NOT BETWEEN",
			values:   nil,
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "OR", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle empty values slice for NOT BETWEEN operator with OR",
			column:   "age",
			operator: "NOT BETWEEN",
			values:   []any{},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "OR", args: []any{nil, nil}},
			},
		},
		{
			name:     "should handle single value slice for NOT BETWEEN operator with OR",
			column:   "age",
			operator: "NOT BETWEEN",
			values:   []any{10},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "OR", args: []any{10, nil}},
			},
		},
		{
			name:     "should handle more than 2 values slice for NOT BETWEEN operator with OR (values are ignored)",
			column:   "age",
			operator: "NOT BETWEEN",
			values:   []any{10, 18, 25},
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "OR", args: []any{nil, nil}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhere(tt.column, tt.operator, tt.values...)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhere() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		from           any
		to             any
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE BETWEEN condition",
			initialWheres: []where{},
			column:        "age",
			from:          18,
			to:            30,
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "AND", args: []any{18, 30}},
			},
		},
		{
			name: "should add a second WHERE BETWEEN condition with AND",
			initialWheres: []where{
				{queryType: QueryBetween, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "created_at",
			from:   "2023-01-01",
			to:     "2023-12-31",
			expectedWheres: []where{
				{queryType: QueryBetween, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryBetween, column: "created_at", operator: "BETWEEN", conj: "AND", args: []any{"2023-01-01", "2023-12-31"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.WhereBetween(tt.column, tt.from, tt.to)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected WhereBetween() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		from           any
		to             any
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE BETWEEN condition",
			initialWheres: []where{},
			column:        "age",
			from:          18,
			to:            30,
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "BETWEEN", conj: "OR", args: []any{18, 30}},
			},
		},
		{
			name: "should add a second OR WHERE BETWEEN condition after an AND",
			initialWheres: []where{
				{queryType: QueryBetween, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "created_at",
			from:   "2023-01-01",
			to:     "2023-12-31",
			expectedWheres: []where{
				{queryType: QueryBetween, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryBetween, column: "created_at", operator: "BETWEEN", conj: "OR", args: []any{"2023-01-01", "2023-12-31"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhereBetween(tt.column, tt.from, tt.to)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhereBetween() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereNotBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		from           any
		to             any
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE NOT BETWEEN condition",
			initialWheres: []where{},
			column:        "age",
			from:          18,
			to:            30,
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "AND", args: []any{18, 30}},
			},
		},
		{
			name: "should add a second WHERE NOT BETWEEN condition with AND",
			initialWheres: []where{
				{queryType: QueryBetween, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "created_at",
			from:   "2023-01-01",
			to:     "2023-12-31",
			expectedWheres: []where{
				{queryType: QueryBetween, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryBetween, column: "created_at", operator: "NOT BETWEEN", conj: "AND", args: []any{"2023-01-01", "2023-12-31"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.WhereNotBetween(tt.column, tt.from, tt.to)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected WhereNotBetween() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereNotBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		from           any
		to             any
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE NOT BETWEEN condition",
			initialWheres: []where{},
			column:        "age",
			from:          18,
			to:            30,
			expectedWheres: []where{
				{queryType: QueryBetween, column: "age", operator: "NOT BETWEEN", conj: "OR", args: []any{18, 30}},
			},
		},
		{
			name: "should add a second OR WHERE NOT BETWEEN condition after an AND",
			initialWheres: []where{
				{queryType: QueryBetween, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "created_at",
			from:   "2023-01-01",
			to:     "2023-12-31",
			expectedWheres: []where{
				{queryType: QueryBetween, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryBetween, column: "created_at", operator: "NOT BETWEEN", conj: "OR", args: []any{"2023-01-01", "2023-12-31"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhereNotBetween(tt.column, tt.from, tt.to)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhereNotBetween() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		values         []any
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE IN condition with a single value",
			initialWheres: []where{},
			column:        "id",
			values:        []any{1},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "IN", conj: "AND", args: []any{1}},
			},
		},
		{
			name:          "should add a single WHERE IN condition with multiple values",
			initialWheres: []where{},
			column:        "status",
			values:        []any{"active", "pending"},
			expectedWheres: []where{
				{queryType: QueryIn, column: "status", operator: "IN", conj: "AND", args: []any{"active", "pending"}},
			},
		},
		{
			name: "should add a second WHERE IN condition with AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "category",
			values: []any{"electronics", "books"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryIn, column: "category", operator: "IN", conj: "AND", args: []any{"electronics", "books"}},
			},
		},
		{
			name:          "should handle slice with nil values",
			initialWheres: []where{},
			column:        "id",
			values:        []any{nil},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "IN", conj: "AND", args: []any{nil}},
			},
		},
		{
			name:          "should handle empty values slice",
			initialWheres: []where{},
			column:        "id",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "IN", conj: "AND", args: []any{}},
			},
		},
		{
			name:          "should handle nil value",
			initialWheres: []where{},
			column:        "id",
			values:        nil,
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "IN", conj: "AND", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.WhereIn(tt.column, tt.values...)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected WhereIn() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		values         []any
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE IN condition with a single value",
			initialWheres: []where{},
			column:        "id",
			values:        []any{1},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "IN", conj: "OR", args: []any{1}},
			},
		},
		{
			name:          "should add a single OR WHERE IN condition with multiple values",
			initialWheres: []where{},
			column:        "status",
			values:        []any{"active", "pending"},
			expectedWheres: []where{
				{queryType: QueryIn, column: "status", operator: "IN", conj: "OR", args: []any{"active", "pending"}},
			},
		},
		{
			name: "should add a second OR WHERE IN condition after an AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "category",
			values: []any{"electronics", "books"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryIn, column: "category", operator: "IN", conj: "OR", args: []any{"electronics", "books"}},
			},
		},
		{
			name:          "should handle slice with nil values with OR",
			initialWheres: []where{},
			column:        "id",
			values:        []any{nil},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "IN", conj: "OR", args: []any{nil}},
			},
		},
		{
			name:          "should handle empty values slice with OR",
			initialWheres: []where{},
			column:        "id",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "IN", conj: "OR", args: []any{}},
			},
		},
		{
			name:          "should handle nil value with OR",
			initialWheres: []where{},
			column:        "id",
			values:        nil,
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "IN", conj: "OR", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhereIn(tt.column, tt.values...)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhereIn() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereNotIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		values         []any
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE NOT IN condition with a single value",
			initialWheres: []where{},
			column:        "id",
			values:        []any{1},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "NOT IN", conj: "AND", args: []any{1}},
			},
		},
		{
			name:          "should add a single WHERE NOT IN condition with multiple values",
			initialWheres: []where{},
			column:        "status",
			values:        []any{"deleted", "archived"},
			expectedWheres: []where{
				{queryType: QueryIn, column: "status", operator: "NOT IN", conj: "AND", args: []any{"deleted", "archived"}},
			},
		},
		{
			name: "should add a second WHERE NOT IN condition with AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "category",
			values: []any{"electronics", "books"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryIn, column: "category", operator: "NOT IN", conj: "AND", args: []any{"electronics", "books"}},
			},
		},
		{
			name:          "should handle slice with nil values",
			initialWheres: []where{},
			column:        "id",
			values:        []any{nil},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "NOT IN", conj: "AND", args: []any{nil}},
			},
		},
		{
			name:          "should handle empty values slice",
			initialWheres: []where{},
			column:        "id",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "NOT IN", conj: "AND", args: []any{}},
			},
		},
		{
			name:          "should handle nil value",
			initialWheres: []where{},
			column:        "id",
			values:        nil,
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "NOT IN", conj: "AND", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.WhereNotIn(tt.column, tt.values...)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected WhereNotIn() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereNotIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		values         []any
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE NOT IN condition with a single value",
			initialWheres: []where{},
			column:        "id",
			values:        []any{1},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "NOT IN", conj: "OR", args: []any{1}},
			},
		},
		{
			name:          "should add a single OR WHERE NOT IN condition with multiple values",
			initialWheres: []where{},
			column:        "status",
			values:        []any{"deleted", "archived"},
			expectedWheres: []where{
				{queryType: QueryIn, column: "status", operator: "NOT IN", conj: "OR", args: []any{"deleted", "archived"}},
			},
		},
		{
			name: "should add a second OR WHERE NOT IN condition after an AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "category",
			values: []any{"electronics", "books"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryIn, column: "category", operator: "NOT IN", conj: "OR", args: []any{"electronics", "books"}},
			},
		},
		{
			name:          "should handle slice with nil values with OR",
			initialWheres: []where{},
			column:        "id",
			values:        []any{nil},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "NOT IN", conj: "OR", args: []any{nil}},
			},
		},
		{
			name:          "should handle empty values slice with OR",
			initialWheres: []where{},
			column:        "id",
			values:        []any{},
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "NOT IN", conj: "OR", args: []any{}},
			},
		},
		{
			name:          "should handle slice with nil values with OR",
			initialWheres: []where{},
			column:        "id",
			values:        nil,
			expectedWheres: []where{
				{queryType: QueryIn, column: "id", operator: "NOT IN", conj: "OR", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhereNotIn(tt.column, tt.values...)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhereNotIn() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE NULL condition",
			initialWheres: []where{},
			column:        "deleted_at",
			expectedWheres: []where{
				{queryType: QueryNull, column: "deleted_at", operator: "IS NULL", conj: "AND", args: []any{}},
			},
		},
		{
			name: "should add a second WHERE NULL condition with AND",
			initialWheres: []where{
				{queryType: QueryNull, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "email_verified_at",
			expectedWheres: []where{
				{queryType: QueryNull, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryNull, column: "email_verified_at", operator: "IS NULL", conj: "AND", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.WhereNull(tt.column)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected WhereNull() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE NULL condition",
			initialWheres: []where{},
			column:        "deleted_at",
			expectedWheres: []where{
				{queryType: QueryNull, column: "deleted_at", operator: "IS NULL", conj: "OR", args: []any{}},
			},
		},
		{
			name: "should add a second OR WHERE NULL condition after an AND",
			initialWheres: []where{
				{queryType: QueryNull, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "email_verified_at",
			expectedWheres: []where{
				{queryType: QueryNull, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryNull, column: "email_verified_at", operator: "IS NULL", conj: "OR", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhereNull(tt.column)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhereNull() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE NOT NULL condition",
			initialWheres: []where{},
			column:        "deleted_at",
			expectedWheres: []where{
				{queryType: QueryNull, column: "deleted_at", operator: "IS NOT NULL", conj: "AND", args: []any{}},
			},
		},
		{
			name: "should add a second WHERE NOT NULL condition with AND",
			initialWheres: []where{
				{queryType: QueryNull, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "email_verified_at",
			expectedWheres: []where{
				{queryType: QueryNull, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryNull, column: "email_verified_at", operator: "IS NOT NULL", conj: "AND", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.WhereNotNull(tt.column)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected WhereNotNull() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE NOT NULL condition",
			initialWheres: []where{},
			column:        "deleted_at",
			expectedWheres: []where{
				{queryType: QueryNull, column: "deleted_at", operator: "IS NOT NULL", conj: "OR", args: []any{}},
			},
		},
		{
			name: "should add a second OR WHERE NOT NULL condition after an AND",
			initialWheres: []where{
				{queryType: QueryNull, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column: "email_verified_at",
			expectedWheres: []where{
				{queryType: QueryNull, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryNull, column: "email_verified_at", operator: "IS NOT NULL", conj: "OR", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhereNotNull(tt.column)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhereNotNull() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		expression     string
		args           []any
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE RAW condition",
			initialWheres: []where{},
			expression:    "id = ? AND name = ?",
			args:          []any{1, "John"},
			expectedWheres: []where{
				{queryType: QueryRaw, expr: "id = ? AND name = ?", conj: "AND", args: []any{1, "John"}},
			},
		},
		{
			name: "should add a second WHERE RAW condition with AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
			},
			expression: "created_at > ? - INTERVAL '1 day'",
			args:       []any{"2025-09-25"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
				{queryType: QueryRaw, expr: "created_at > ? - INTERVAL '1 day'", conj: "AND", args: []any{"2025-09-25"}},
			},
		},
		{
			name:          "should handle raw expression with no arguments",
			initialWheres: []where{},
			expression:    "column_a = column_b",
			args:          []any{},
			expectedWheres: []where{
				{queryType: QueryRaw, expr: "column_a = column_b", conj: "AND", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.WhereRaw(tt.expression, tt.args...)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected WhereRaw() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		expression     string
		args           []any
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE RAW condition",
			initialWheres: []where{},
			expression:    "id = ? OR name = ?",
			args:          []any{1, "John"},
			expectedWheres: []where{
				{queryType: QueryRaw, expr: "id = ? OR name = ?", conj: "OR", args: []any{1, "John"}},
			},
		},
		{
			name: "should add a second OR WHERE RAW condition after an AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
			},
			expression: "created_at < ? - INTERVAL '1 month'",
			args:       []any{"2025-09-25"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
				{queryType: QueryRaw, expr: "created_at < ? - INTERVAL '1 month'", conj: "OR", args: []any{"2025-09-25"}},
			},
		},
		{
			name:          "should handle raw expression with no arguments with OR",
			initialWheres: []where{},
			expression:    "column_c != column_d",
			args:          []any{},
			expectedWheres: []where{
				{queryType: QueryRaw, expr: "column_c != column_d", conj: "OR", args: []any{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhereRaw(tt.expression, tt.args...)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhereRaw() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		groupFn        func(QueryBuilder)
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE group condition",
			initialWheres: []where{},
			groupFn: func(qb QueryBuilder) {
				qb.Where("status", "=", "active").OrWhere("status", "=", "pending")
			},
			expectedWheres: []where{
				{
					queryType: QueryNested,
					conj:      "AND",
					nested: []where{
						{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
						{queryType: QueryBasic, column: "status", operator: "=", conj: "OR", args: []any{"pending"}},
					},
				},
			},
		},
		{
			name: "should add a WHERE group condition after an existing WHERE",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			groupFn: func(qb QueryBuilder) {
				qb.Where("age", ">", 18).Where("age", "<", 65)
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{
					queryType: QueryNested,
					conj:      "AND",
					nested: []where{
						{queryType: QueryBasic, column: "age", operator: ">", conj: "AND", args: []any{18}},
						{queryType: QueryBasic, column: "age", operator: "<", conj: "AND", args: []any{65}},
					},
				},
			},
		},
		{
			name: "should add a nested WHERE group with raw and basic conditions",
			initialWheres: []where{
				{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
			},
			groupFn: func(qb QueryBuilder) {
				qb.WhereRaw("amount > ?", 100).OrWhere("currency", "=", "USD")
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
				{
					queryType: QueryNested,
					conj:      "AND",
					nested: []where{
						{queryType: QueryRaw, expr: "amount > ?", conj: "AND", args: []any{100}},
						{queryType: QueryBasic, column: "currency", operator: "=", conj: "OR", args: []any{"USD"}},
					},
				},
			},
		},
		{
			name: "should add a deeply nested WHERE group with multiple AND or OR conditions",
			initialWheres: []where{
				{queryType: QueryBasic, column: "user_id", operator: "=", conj: "AND", args: []any{123}},
			},
			groupFn: func(qb QueryBuilder) {
				qb.
					Where("status", "=", "active").
					OrWhereGroup(func(nestedQb QueryBuilder) {
						nestedQb.
							Where("category", "=", "premium").
							WhereRaw("price > ?", 100)
					}).
					WhereIn("region", []any{"north", "east"})
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "user_id", operator: "=", conj: "AND", args: []any{123}},
				{
					queryType: QueryNested,
					conj:      "AND",
					nested: []where{
						{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
						{
							queryType: QueryNested,
							conj:      "OR",
							nested: []where{
								{queryType: QueryBasic, column: "category", operator: "=", conj: "AND", args: []any{"premium"}},
								{queryType: QueryRaw, expr: "price > ?", conj: "AND", args: []any{100}},
							},
						},
						{queryType: QueryIn, column: "region", operator: "IN", conj: "AND", args: []any{[]any{"north", "east"}}},
					},
				},
			},
		},
		{
			name:          "should not add an empty WHERE group",
			initialWheres: []where{},
			groupFn: func(qb QueryBuilder) {
				// Do nothing
			},
			expectedWheres: []where{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.WhereGroup(tt.groupFn)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected WhereGroup() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		groupFn        func(QueryBuilder)
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE group condition",
			initialWheres: []where{},
			groupFn: func(qb QueryBuilder) {
				qb.Where("status", "=", "active").Where("status", "=", "pending")
			},
			expectedWheres: []where{
				{
					queryType: QueryNested,
					conj:      "OR",
					nested: []where{
						{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"active"}},
						{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"pending"}},
					},
				},
			},
		},
		{
			name: "should add an OR WHERE group condition after an existing WHERE",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			groupFn: func(qb QueryBuilder) {
				qb.Where("age", ">", 18).OrWhere("age", "<", 10)
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{
					queryType: QueryNested,
					conj:      "OR",
					nested: []where{
						{queryType: QueryBasic, column: "age", operator: ">", conj: "AND", args: []any{18}},
						{queryType: QueryBasic, column: "age", operator: "<", conj: "OR", args: []any{10}},
					},
				},
			},
		},
		{
			name: "should add a deeply nested OR WHERE group with multiple AND or OR conditions",
			initialWheres: []where{
				{queryType: QueryBasic, column: "user_id", operator: "=", conj: "AND", args: []any{123}},
			},
			groupFn: func(qb QueryBuilder) {
				qb.
					Where("status", "=", "inactive").
					OrWhereGroup(func(nestedQb QueryBuilder) {
						nestedQb.
							Where("category", "=", "basic").
							OrWhereRaw("price < ?", 50)
					}).
					OrWhereIn("region", []any{"south", "west"})
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "user_id", operator: "=", conj: "AND", args: []any{123}},
				{
					queryType: QueryNested,
					conj:      "OR",
					nested: []where{
						{queryType: QueryBasic, column: "status", operator: "=", conj: "AND", args: []any{"inactive"}},
						{
							queryType: QueryNested,
							conj:      "OR",
							nested: []where{
								{queryType: QueryBasic, column: "category", operator: "=", conj: "AND", args: []any{"basic"}},
								{queryType: QueryRaw, expr: "price < ?", conj: "OR", args: []any{50}},
							},
						},
						{queryType: QueryIn, column: "region", operator: "IN", conj: "OR", args: []any{[]any{"south", "west"}}},
					},
				},
			},
		},
		{
			name:          "should not add an empty OR WHERE group",
			initialWheres: []where{},
			groupFn: func(qb QueryBuilder) {
				// Do nothing
			},
			expectedWheres: []where{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhereGroup(tt.groupFn)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhereGroup() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		operator       string
		subFn          func(QueryBuilder)
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE subquery condition",
			initialWheres: []where{},
			column:        "user_id",
			operator:      "IN",
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("users").Where("status", "=", "active")
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "user_id",
					operator:  "IN",
					conj:      "AND",
					sub:       New(PostgresDialect{}).Select("id").From("users").Where("status", "=", "active"),
				},
			},
		},
		{
			name: "should add a second WHERE subquery condition with AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column:   "product_id",
			operator: "NOT IN",
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("products").Where("stock", "<", 10)
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{
					queryType: QuerySub,
					column:    "product_id",
					operator:  "NOT IN",
					conj:      "AND",
					sub:       New(PostgresDialect{}).Select("id").From("products").Where("stock", "<", 10),
				},
			},
		},
		{
			name:          "should handle subquery with no conditions",
			initialWheres: []where{},
			column:        "category_id",
			operator:      "=",
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("categories")
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "category_id",
					operator:  "=",
					conj:      "AND",
					sub:       New(PostgresDialect{}).Select("id").From("categories"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				wheres:  tt.initialWheres,
			}

			// Act
			result := b.WhereSub(tt.column, tt.operator, tt.subFn)

			// Assert
			// We need to compare the sub-builders separately as direct comparison of interfaces fails
			assert.Equal(t, len(tt.expectedWheres), len(b.wheres), "expected number of wheres to match")
			if len(tt.expectedWheres) > 0 {
				for i, expectedWhere := range tt.expectedWheres {
					actualWhere := b.wheres[i]
					assert.Equal(t, expectedWhere.queryType, actualWhere.queryType, "expected query type to match")
					assert.Equal(t, expectedWhere.column, actualWhere.column, "expected column to match")
					assert.Equal(t, expectedWhere.operator, actualWhere.operator, "expected operator to match")
					assert.Equal(t, expectedWhere.conj, actualWhere.conj, "expected conj to match")
					assert.Equal(t, expectedWhere.args, actualWhere.args, "expected args to match")

					if expectedWhere.queryType == QuerySub {
						expectedSubBuilder := expectedWhere.sub.(*builder)
						actualSubBuilder := actualWhere.sub.(*builder)
						assert.Equal(t, expectedSubBuilder.table, actualSubBuilder.table, "expected table to match")
						assert.Equal(t, expectedSubBuilder.columns, actualSubBuilder.columns, "expected columns to match")
						assert.Equal(t, expectedSubBuilder.wheres, actualSubBuilder.wheres, "expected wheres to match")
					}
				}
			}

			assert.Equal(t, b, result, "expected WhereSub() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		column         string
		operator       string
		subFn          func(QueryBuilder)
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE subquery condition",
			initialWheres: []where{},
			column:        "user_id",
			operator:      "IN",
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("users").Where("status", "=", "inactive")
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "user_id",
					operator:  "IN",
					conj:      "OR",
					sub:       New(PostgresDialect{}).Select("id").From("users").Where("status", "=", "inactive"),
				},
			},
		},
		{
			name: "should add a second OR WHERE subquery condition after an AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column:   "product_id",
			operator: "NOT IN",
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("products").Where("stock", ">", 50)
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{
					queryType: QuerySub,
					column:    "product_id",
					operator:  "NOT IN",
					conj:      "OR",
					sub:       New(PostgresDialect{}).Select("id").From("products").Where("stock", ">", 50),
				},
			},
		},
		{
			name:     "should handle subquery with no conditions with OR",
			column:   "category_id",
			operator: "=",
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("categories")
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "category_id",
					operator:  "=",
					conj:      "OR",
					sub:       New(PostgresDialect{}).Select("id").From("categories"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres, dialect: PostgresDialect{}}

			// Act
			result := b.OrWhereSub(tt.column, tt.operator, tt.subFn)

			// Assert
			// We need to compare the sub-builders separately as direct comparison of interfaces fails
			assert.Equal(t, len(tt.expectedWheres), len(b.wheres), "expected number of wheres to match")

			if len(tt.expectedWheres) > 0 {
				for i, expectedWhere := range tt.expectedWheres {
					actualWhere := b.wheres[i]
					assert.Equal(t, expectedWhere.queryType, actualWhere.queryType, "expected QueryType to match")
					assert.Equal(t, expectedWhere.column, actualWhere.column, "expected column to match")
					assert.Equal(t, expectedWhere.operator, actualWhere.operator, "expected operator to match")
					assert.Equal(t, expectedWhere.conj, actualWhere.conj, "expected conj to match")
					assert.Equal(t, expectedWhere.args, actualWhere.args, "expected args to match")

					if expectedWhere.queryType == QuerySub {
						expectedSubBuilder := expectedWhere.sub.(*builder)
						actualSubBuilder := actualWhere.sub.(*builder)
						assert.Equal(t, expectedSubBuilder.table, actualSubBuilder.table, "expected table to match")
						assert.Equal(t, expectedSubBuilder.columns, actualSubBuilder.columns, "expected columns to match")
						assert.Equal(t, expectedSubBuilder.wheres, actualSubBuilder.wheres, "expected wheres to match")
					}
				}
			}

			assert.Equal(t, b, result, "expected OrWhereSub() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		subFn          func(QueryBuilder)
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE EXISTS condition",
			initialWheres: []where{},
			subFn: func(qb QueryBuilder) {
				qb.Select("id").
					From("users").
					Where("status", "=", "active")
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "",
					operator:  "EXISTS",
					conj:      "AND",
					sub:       New(PostgresDialect{}).Select("id").From("users").Where("status", "=", "active"),
				},
			},
		},
		{
			name: "should add a second WHERE EXISTS condition with AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			subFn: func(qb QueryBuilder) {
				qb.Select("id").
					From("orders").
					Where("user_id", "=", 1)
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{
					queryType: QuerySub,
					column:    "",
					operator:  "EXISTS",
					conj:      "AND",
					sub:       New(PostgresDialect{}).Select("id").From("orders").Where("user_id", "=", 1),
				},
			},
		},
		{
			name:          "should handle EXISTS with empty subquery",
			initialWheres: []where{},
			subFn: func(qb QueryBuilder) {
				qb.From("users") // No select, but From is enough for a valid subquery
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "",
					operator:  "EXISTS",
					conj:      "AND",
					sub:       New(PostgresDialect{}).From("users"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				wheres:  tt.initialWheres,
			}

			// Act
			result := b.WhereExists(tt.subFn)

			// Assert
			// We need to compare the sub-builders separately as direct comparison of interfaces fails
			assert.Equal(t, len(tt.expectedWheres), len(b.wheres), "expected number of wheres to match")
			if len(tt.expectedWheres) > 0 {
				for i, expectedWhere := range tt.expectedWheres {
					actualWhere := b.wheres[i]
					assert.Equal(t, expectedWhere.queryType, actualWhere.queryType, "expected query type to match")
					assert.Equal(t, expectedWhere.column, actualWhere.column, "expected column to match")
					assert.Equal(t, expectedWhere.operator, actualWhere.operator, "expected operator to match")
					assert.Equal(t, expectedWhere.conj, actualWhere.conj, "expected conj to match")
					assert.Equal(t, expectedWhere.args, actualWhere.args, "expected args to match")

					if expectedWhere.queryType == QuerySub {
						expectedSubBuilder := expectedWhere.sub.(*builder)
						actualSubBuilder := actualWhere.sub.(*builder)
						assert.Equal(t, expectedSubBuilder.table, actualSubBuilder.table, "expected table to match")
						assert.Equal(t, expectedSubBuilder.columns, actualSubBuilder.columns, "expected columns to match")
						assert.Equal(t, expectedSubBuilder.wheres, actualSubBuilder.wheres, "expected wheres to match")
					}
				}
			}

			assert.Equal(t, b, result, "expected WhereExists() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		subFn          func(QueryBuilder)
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE EXISTS condition",
			initialWheres: []where{},
			subFn: func(qb QueryBuilder) {
				qb.Select("id").
					From("users").
					Where("status", "=", "inactive")
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "",
					operator:  "EXISTS",
					conj:      "OR",
					sub:       New(PostgresDialect{}).Select("id").From("users").Where("status", "=", "inactive"),
				},
			},
		},
		{
			name: "should add a second OR WHERE EXISTS condition after an AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			subFn: func(qb QueryBuilder) {
				qb.Select("id").
					From("orders").
					Where("user_id", "=", 2)
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{
					queryType: QuerySub,
					column:    "",
					operator:  "EXISTS",
					conj:      "OR",
					sub:       New(PostgresDialect{}).Select("id").From("orders").Where("user_id", "=", 2),
				},
			},
		},
		{
			name:          "should handle OR EXISTS with empty subquery",
			initialWheres: []where{},
			subFn: func(qb QueryBuilder) {
				qb.From("products") // No select, but From is enough for a valid subquery
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "",
					operator:  "EXISTS",
					conj:      "OR",
					sub:       New(PostgresDialect{}).From("products"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres, dialect: PostgresDialect{}}

			// Act
			result := b.OrWhereExists(tt.subFn)

			// Assert
			// We need to compare the sub-builders separately as direct comparison of interfaces fails
			assert.Equal(t, len(tt.expectedWheres), len(b.wheres), "expected number of wheres to match")

			if len(tt.expectedWheres) > 0 {
				for i, expectedWhere := range tt.expectedWheres {
					actualWhere := b.wheres[i]
					assert.Equal(t, expectedWhere.queryType, actualWhere.queryType, "expected QueryType to match")
					assert.Equal(t, expectedWhere.column, actualWhere.column, "expected column to match")
					assert.Equal(t, expectedWhere.operator, actualWhere.operator, "expected operator to match")
					assert.Equal(t, expectedWhere.conj, actualWhere.conj, "expected conj to match")
					assert.Equal(t, expectedWhere.args, actualWhere.args, "expected args to match")

					if expectedWhere.queryType == QuerySub {
						expectedSubBuilder := expectedWhere.sub.(*builder)
						actualSubBuilder := actualWhere.sub.(*builder)
						assert.Equal(t, expectedSubBuilder.table, actualSubBuilder.table, "expected table to match")
						assert.Equal(t, expectedSubBuilder.columns, actualSubBuilder.columns, "expected columns to match")
						assert.Equal(t, expectedSubBuilder.wheres, actualSubBuilder.wheres, "expected wheres to match")
					}
				}
			}

			assert.Equal(t, b, result, "expected OrWhereExists() to return the same builder instance")
		})
	}
}

func TestBuilder_WhereNotExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		subFn          func(QueryBuilder)
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE NOT EXISTS condition",
			initialWheres: []where{},
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("users").Where("status", "=", "active")
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "",
					operator:  "NOT EXISTS",
					conj:      "AND",
					sub:       New(PostgresDialect{}).Select("id").From("users").Where("status", "=", "active"),
				},
			},
		},
		{
			name: "should add a second WHERE NOT EXISTS condition with AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("orders").Where("user_id", "=", 1)
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{
					queryType: QuerySub,
					column:    "",
					operator:  "NOT EXISTS",
					conj:      "AND",
					sub:       New(PostgresDialect{}).Select("id").From("orders").Where("user_id", "=", 1),
				},
			},
		},
		{
			name:          "should handle NOT EXISTS with empty subquery",
			initialWheres: []where{},
			subFn: func(qb QueryBuilder) {
				qb.From("users") // No select, but From is enough for a valid subquery
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "",
					operator:  "NOT EXISTS",
					conj:      "AND",
					sub:       New(PostgresDialect{}).From("users"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				wheres:  tt.initialWheres,
			}

			// Act
			result := b.WhereNotExists(tt.subFn)

			// Assert
			// We need to compare the sub-builders separately as direct comparison of interfaces fails
			assert.Equal(t, len(tt.expectedWheres), len(b.wheres), "expected number of wheres to match")
			if len(tt.expectedWheres) > 0 {
				for i, expectedWhere := range tt.expectedWheres {
					actualWhere := b.wheres[i]
					assert.Equal(t, expectedWhere.queryType, actualWhere.queryType, "expected query type to match")
					assert.Equal(t, expectedWhere.column, actualWhere.column, "expected column to match")
					assert.Equal(t, expectedWhere.operator, actualWhere.operator, "expected operator to match")
					assert.Equal(t, expectedWhere.conj, actualWhere.conj, "expected conj to match")
					assert.Equal(t, expectedWhere.args, actualWhere.args, "expected args to match")

					if expectedWhere.queryType == QuerySub {
						expectedSubBuilder := expectedWhere.sub.(*builder)
						actualSubBuilder := actualWhere.sub.(*builder)
						assert.Equal(t, expectedSubBuilder.table, actualSubBuilder.table, "expected table to match")
						assert.Equal(t, expectedSubBuilder.columns, actualSubBuilder.columns, "expected columns to match")
						assert.Equal(t, expectedSubBuilder.wheres, actualSubBuilder.wheres, "expected wheres to match")
					}
				}
			}

			assert.Equal(t, b, result, "expected WhereNotExists() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhereNotExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []where
		subFn          func(QueryBuilder)
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE NOT EXISTS condition",
			initialWheres: []where{},
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("users").Where("status", "=", "inactive")
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "",
					operator:  "NOT EXISTS",
					conj:      "OR",
					sub:       New(PostgresDialect{}).Select("id").From("users").Where("status", "=", "inactive"),
				},
			},
		},
		{
			name: "should add a second OR WHERE NOT EXISTS condition after an AND",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			subFn: func(qb QueryBuilder) {
				qb.Select("id").From("orders").Where("user_id", "=", 2)
			},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{
					queryType: QuerySub,
					column:    "",
					operator:  "NOT EXISTS",
					conj:      "OR",
					sub:       New(PostgresDialect{}).Select("id").From("orders").Where("user_id", "=", 2),
				},
			},
		},
		{
			name:          "should handle OR NOT EXISTS with empty subquery",
			initialWheres: []where{},
			subFn: func(qb QueryBuilder) {
				qb.From("products") // No select, but From is enough for a valid subquery
			},
			expectedWheres: []where{
				{
					queryType: QuerySub,
					column:    "",
					operator:  "NOT EXISTS",
					conj:      "OR",
					sub:       New(PostgresDialect{}).From("products"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres, dialect: PostgresDialect{}}

			// Act
			result := b.OrWhereNotExists(tt.subFn)

			// Assert
			// We need to compare the sub-builders separately as direct comparison of interfaces fails
			assert.Equal(t, len(tt.expectedWheres), len(b.wheres), "expected number of wheres to match")

			if len(tt.expectedWheres) > 0 {
				for i, expectedWhere := range tt.expectedWheres {
					actualWhere := b.wheres[i]
					assert.Equal(t, expectedWhere.queryType, actualWhere.queryType, "expected QueryType to match")
					assert.Equal(t, expectedWhere.column, actualWhere.column, "expected column to match")
					assert.Equal(t, expectedWhere.operator, actualWhere.operator, "expected operator to match")
					assert.Equal(t, expectedWhere.conj, actualWhere.conj, "expected conj to match")
					assert.Equal(t, expectedWhere.args, actualWhere.args, "expected args to match")

					if expectedWhere.queryType == QuerySub {
						expectedSubBuilder := expectedWhere.sub.(*builder)
						actualSubBuilder := actualWhere.sub.(*builder)
						assert.Equal(t, expectedSubBuilder.table, actualSubBuilder.table, "expected table to match")
						assert.Equal(t, expectedSubBuilder.columns, actualSubBuilder.columns, "expected columns to match")
						assert.Equal(t, expectedSubBuilder.wheres, actualSubBuilder.wheres, "expected wheres to match")
					}
				}
			}

			assert.Equal(t, b, result, "expected OrWhereNotExists() to return the same builder instance")
		})
	}
}

// -----------------
// --- BENCHMARK ---
// -----------------

func BenchmarkBuilder_Where(b *testing.B) {
	builder := &builder{}
	column := "id"
	operator := "="
	value := 1

	for b.Loop() {
		builder.Where(column, operator, value)
	}
}

func BenchmarkBuilder_OrWhere(b *testing.B) {
	builder := &builder{}
	column := "status"
	operator := "="
	value := "active"

	for b.Loop() {
		builder.OrWhere(column, operator, value)
	}
}

func BenchmarkBuilder_WhereBetween(b *testing.B) {
	builder := &builder{}
	column := "created_at"
	from := "2023-01-01"
	to := "2023-12-31"

	for b.Loop() {
		builder.WhereBetween(column, from, to)
	}
}

func BenchmarkBuilder_OrWhereBetween(b *testing.B) {
	builder := &builder{}
	column := "updated_at"
	from := "2023-01-01"
	to := "2023-12-31"

	for b.Loop() {
		builder.OrWhereBetween(column, from, to)
	}
}

func BenchmarkBuilder_WhereNotBetween(b *testing.B) {
	builder := &builder{}
	column := "age"
	from := 18
	to := 65

	for b.Loop() {
		builder.WhereNotBetween(column, from, to)
	}
}

func BenchmarkBuilder_OrWhereNotBetween(b *testing.B) {
	builder := &builder{}
	column := "price"
	from := 100
	to := 500

	for b.Loop() {
		builder.OrWhereNotBetween(column, from, to)
	}
}

func BenchmarkBuilder_WhereIn(b *testing.B) {
	builder := &builder{}
	column := "category"
	values := []any{"electronics", "books", "clothing"}

	for b.Loop() {
		builder.WhereIn(column, values)
	}
}

func BenchmarkBuilder_OrWhereIn(b *testing.B) {
	builder := &builder{}
	column := "tag"
	values := []any{"new", "featured", "sale"}

	for b.Loop() {
		builder.OrWhereIn(column, values)
	}
}

func BenchmarkBuilder_WhereNotIn(b *testing.B) {
	builder := &builder{}
	column := "status"
	values := []any{"deleted", "archived"}

	for b.Loop() {
		builder.WhereNotIn(column, values)
	}
}

func BenchmarkBuilder_OrWhereNotIn(b *testing.B) {
	builder := &builder{}
	column := "country"
	values := []any{"US", "CA", "MX"}

	for b.Loop() {
		builder.OrWhereNotIn(column, values)
	}
}

func BenchmarkBuilder_WhereNull(b *testing.B) {
	builder := &builder{}
	column := "deleted_at"

	for b.Loop() {
		builder.WhereNull(column)
	}
}

func BenchmarkBuilder_OrWhereNull(b *testing.B) {
	builder := &builder{}
	column := "email_verified_at"

	for b.Loop() {
		builder.OrWhereNull(column)
	}
}

func BenchmarkBuilder_WhereNotNull(b *testing.B) {
	builder := &builder{}
	column := "updated_at"

	for b.Loop() {
		builder.WhereNotNull(column)
	}
}

func BenchmarkBuilder_OrWhereNotNull(b *testing.B) {
	builder := &builder{}
	column := "last_login_at"

	for b.Loop() {
		builder.OrWhereNotNull(column)
	}
}

func BenchmarkBuilder_WhereRaw(b *testing.B) {
	builder := &builder{}
	expression := "id = ? AND name = ?"
	args := []any{1, "John"}

	for b.Loop() {
		builder.WhereRaw(expression, args...)
	}
}

func BenchmarkBuilder_OrWhereRaw(b *testing.B) {
	builder := &builder{}
	expression := "created_at < NOW() - INTERVAL '1 month'"
	args := []any{}

	for b.Loop() {
		builder.OrWhereRaw(expression, args...)
	}
}

func BenchmarkBuilder_WhereGroup(b *testing.B) {
	builder := &builder{}
	groupFn := func(qb QueryBuilder) {
		qb.
			Where("status", "=", "active").
			OrWhere("status", "=", "pending").
			WhereGroup(func(nestedQb QueryBuilder) {
				nestedQb.
					Where("category", "=", "premium").
					WhereRaw("price > ?", 100)
			})
	}

	for b.Loop() {
		builder.WhereGroup(groupFn)
	}
}

func BenchmarkBuilder_OrWhereGroup(b *testing.B) {
	builder := &builder{}
	groupFn := func(qb QueryBuilder) {
		qb.
			Where("age", ">", 18).
			OrWhere("age", "<", 10).
			WhereGroup(func(nestedQb QueryBuilder) {
				nestedQb.
					Where("country", "=", "US").
					OrWhereRaw("zip_code LIKE ?", "90210%")
			})
	}

	for b.Loop() {
		builder.OrWhereGroup(groupFn)
	}
}

func BenchmarkBuilder_WhereSub(b *testing.B) {
	builder := &builder{dialect: PostgresDialect{}}
	column := "user_id"
	operator := "IN"
	subFn := func(qb QueryBuilder) {
		qb.Select("id").From("users").Where("status", "=", "active")
	}

	for b.Loop() {
		builder.WhereSub(column, operator, subFn)
	}
}

func BenchmarkBuilder_OrWhereSub(b *testing.B) {
	builder := &builder{dialect: PostgresDialect{}}
	column := "product_id"
	operator := "NOT IN"
	subFn := func(qb QueryBuilder) {
		qb.Select("id").From("products").Where("stock", "<", 10)
	}

	for b.Loop() {
		builder.OrWhereSub(column, operator, subFn)
	}
}

func BenchmarkBuilder_WhereExists(b *testing.B) {
	builder := &builder{dialect: PostgresDialect{}}
	subFn := func(qb QueryBuilder) {
		qb.Select("id").From("users").Where("status", "=", "active")
	}

	for b.Loop() {
		builder.WhereExists(subFn)
	}
}

func BenchmarkBuilder_OrWhereExists(b *testing.B) {
	builder := &builder{dialect: PostgresDialect{}}
	subFn := func(qb QueryBuilder) {
		qb.Select("id").From("orders").Where("user_id", "=", 1)
	}

	for b.Loop() {
		builder.OrWhereExists(subFn)
	}
}

func BenchmarkBuilder_WhereNotExists(b *testing.B) {
	builder := &builder{dialect: PostgresDialect{}}
	subFn := func(qb QueryBuilder) {
		qb.Select("id").From("products").Where("stock", "<", 5)
	}

	for b.Loop() {
		builder.WhereNotExists(subFn)
	}
}

func BenchmarkBuilder_OrWhereNotExists(b *testing.B) {
	builder := &builder{dialect: PostgresDialect{}}
	subFn := func(qb QueryBuilder) {
		qb.Select("id").From("categories").Where("is_active", "=", false)
	}

	for b.Loop() {
		builder.OrWhereNotExists(subFn)
	}
}
