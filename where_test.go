package goquerybuilder

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
		value          any
		expectedWheres []where
	}{
		{
			name:          "should add a single WHERE condition",
			initialWheres: []where{},
			column:        "id",
			operator:      "=",
			value:         1,
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
			value:    "John",
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryBasic, column: "name", operator: "=", conj: "AND", args: []any{"John"}},
			},
		},
		{
			name:          "should handle different operators",
			initialWheres: []where{},
			column:        "age",
			operator:      ">",
			value:         18,
			expectedWheres: []where{
				{queryType: QueryBasic, column: "age", operator: ">", conj: "AND", args: []any{18}},
			},
		},
		{
			name:          "should handle IN operator",
			initialWheres: []where{},
			column:        "category",
			operator:      "IN",
			value:         []string{"A", "B"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "category", operator: "IN", conj: "AND", args: []any{[]string{"A", "B"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.Where(tt.column, tt.operator, tt.value)

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
		value          any
		expectedWheres []where
	}{
		{
			name:          "should add a single OR WHERE condition",
			initialWheres: []where{},
			column:        "id",
			operator:      "=",
			value:         1,
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "OR", args: []any{1}},
			},
		},
		{
			name: "should add an OR WHERE condition after an AND condition",
			initialWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
			},
			column:   "name",
			operator: "=",
			value:    "John",
			expectedWheres: []where{
				{queryType: QueryBasic, column: "id", operator: "=", conj: "AND", args: []any{1}},
				{queryType: QueryBasic, column: "name", operator: "=", conj: "OR", args: []any{"John"}},
			},
		},
		{
			name:          "should handle different operators with OR",
			initialWheres: []where{},
			column:        "age",
			operator:      "<",
			value:         18,
			expectedWheres: []where{
				{queryType: QueryBasic, column: "age", operator: "<", conj: "OR", args: []any{18}},
			},
		},
		{
			name:          "should handle IN operator with OR",
			initialWheres: []where{},
			column:        "category",
			operator:      "NOT IN",
			value:         []string{"A", "B"},
			expectedWheres: []where{
				{queryType: QueryBasic, column: "category", operator: "NOT IN", conj: "OR", args: []any{[]string{"A", "B"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{wheres: tt.initialWheres}

			// Act
			result := b.OrWhere(tt.column, tt.operator, tt.value)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhere() to return the same builder instance")
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
