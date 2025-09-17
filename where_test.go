package goquerybuilder

import (
	"testing"

	"github.com/shoraid/go-querybuilder/dialect"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_Where(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []condition
		column         string
		operator       string
		value          any
		expectedWheres []condition
		expectedArgs   []any
	}{
		{
			name:          "should add a single WHERE condition",
			initialWheres: []condition{},
			column:        "id",
			operator:      "=",
			value:         1,
			expectedWheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{1},
		},
		{
			name: "should add a second WHERE condition with AND",
			initialWheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
			},
			column:   "name",
			operator: "=",
			value:    "John",
			expectedWheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
				{conj: "AND", query: `"name" = $2`, argIndexes: []int{1}},
			},
			expectedArgs: []any{1, "John"},
		},
		{
			name:          "should handle different operators",
			initialWheres: []condition{},
			column:        "age",
			operator:      ">",
			value:         18,
			expectedWheres: []condition{
				{conj: "AND", query: `"age" > $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{18},
		},
		{
			name:          "should quote column name correctly",
			initialWheres: []condition{},
			column:        "user_name",
			operator:      "LIKE",
			value:         "test%",
			expectedWheres: []condition{
				{conj: "AND", query: `"user_name" LIKE $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{"test%"},
		},
		{
			name:          "should default operator to '=' for invalid operator",
			initialWheres: []condition{},
			column:        "status",
			operator:      "UNKNOWN",
			value:         "active",
			expectedWheres: []condition{
				{conj: "AND", query: `"status" = $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{"active"},
		},
		{
			name:          "should handle IN operator",
			initialWheres: []condition{},
			column:        "category",
			operator:      "IN",
			value:         []string{"A", "B"},
			expectedWheres: []condition{
				{conj: "AND", query: `"category" IN $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{[]string{"A", "B"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: dialect.PostgresDialect{}, // Use Postgres for placeholder generation
				wheres:  tt.initialWheres,
				args:    tt.expectedArgs[:len(tt.initialWheres)], // Initialize args based on initial wheres
			}

			// Act
			result := b.Where(tt.column, tt.operator, tt.value)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, tt.expectedArgs, b.args, "expected args to be updated correctly")
			assert.Equal(t, b, result, "expected Where() to return the same builder instance")
		})
	}
}

func TestBuilder_OrWhere(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialWheres  []condition
		column         string
		operator       string
		value          any
		expectedWheres []condition
		expectedArgs   []any
	}{
		{
			name:          "should add a single OR WHERE condition",
			initialWheres: []condition{},
			column:        "id",
			operator:      "=",
			value:         1,
			expectedWheres: []condition{
				{conj: "OR", query: `"id" = $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{1},
		},
		{
			name: "should add an OR WHERE condition after an AND condition",
			initialWheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
			},
			column:   "name",
			operator: "=",
			value:    "John",
			expectedWheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
				{conj: "OR", query: `"name" = $2`, argIndexes: []int{1}},
			},
			expectedArgs: []any{1, "John"},
		},
		{
			name:          "should handle different operators with OR",
			initialWheres: []condition{},
			column:        "age",
			operator:      "<",
			value:         18,
			expectedWheres: []condition{
				{conj: "OR", query: `"age" < $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{18},
		},
		{
			name:          "should default operator to '=' for invalid operator with OR",
			initialWheres: []condition{},
			column:        "status",
			operator:      "UNKNOWN",
			value:         "inactive",
			expectedWheres: []condition{
				{conj: "OR", query: `"status" = $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{"inactive"},
		},
		{
			name:          "should handle IN operator with OR",
			initialWheres: []condition{},
			column:        "category",
			operator:      "NOT IN",
			value:         []string{"A", "B"},
			expectedWheres: []condition{
				{conj: "OR", query: `"category" NOT IN $1`, argIndexes: []int{0}},
			},
			expectedArgs: []any{[]string{"A", "B"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: dialect.PostgresDialect{}, // Use Postgres for placeholder generation
				wheres:  tt.initialWheres,
				args:    tt.expectedArgs[:len(tt.initialWheres)], // Initialize args based on initial wheres
			}

			// Act
			result := b.OrWhere(tt.column, tt.operator, tt.value)

			// Assert
			assert.Equal(t, tt.expectedWheres, b.wheres, "expected wheres to be updated correctly")
			assert.Equal(t, tt.expectedArgs, b.args, "expected args to be updated correctly")
			assert.Equal(t, b, result, "expected OrWhere() to return the same builder instance")
		})
	}
}
