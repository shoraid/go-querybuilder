package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_OrderBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		initialOrderBys  []orderBy
		column           string
		direction        string
		expectedOrderBys []orderBy
	}{
		{
			name:            "should add single ASC order by clause",
			initialOrderBys: []orderBy{},
			column:          "id",
			direction:       "asc",
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
			},
		},
		{
			name:            "should add single DESC order by clause",
			initialOrderBys: []orderBy{},
			column:          "name",
			direction:       "DESC",
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "name", dir: "DESC"},
			},
		},
		{
			name:            "should default to ASC for invalid direction",
			initialOrderBys: []orderBy{},
			column:          "created_at",
			direction:       "invalid",
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "created_at", dir: "ASC"},
			},
		},
		{
			name: "should handle multiple order by calls",
			initialOrderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
			},
			column:    "email",
			direction: "ASC",
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
				{queryType: QueryBasic, column: "email", dir: "ASC"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{orderBys: tt.initialOrderBys}

			// Act
			result := b.OrderBy(tt.column, tt.direction)

			// Assert
			assert.Equal(t, tt.expectedOrderBys, b.orderBys, "expected order by clauses to be added")
			assert.Equal(t, b, result, "expected OrderBy() to return the same builder instance")
		})
	}
}

func TestBuilder_OrderByRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		initialOrderBys  []orderBy
		expression       string
		args             []any
		expectedOrderBys []orderBy
		expectedArgs     []any
	}{
		{
			name:            "should add raw order by expression",
			initialOrderBys: []orderBy{},
			expression:      "LENGTH(name) DESC",
			args:            []any{},
			expectedOrderBys: []orderBy{
				{queryType: QueryRaw, expr: "LENGTH(name) DESC", args: []any{}},
			},
			expectedArgs: []any{},
		},
		{
			name: "should add another raw order by expression",
			initialOrderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
			},
			expression: "RANDOM()",
			args:       []any{},
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
				{queryType: QueryRaw, expr: "RANDOM()", args: []any{}},
			},
			expectedArgs: []any{},
		},
		{
			name:       "should handle complex raw expression with multiple args",
			expression: "CASE WHEN amount > ? THEN ? ELSE ? END DESC",
			args:       []any{100, 1, 0},
			expectedOrderBys: []orderBy{
				{queryType: QueryRaw, expr: "CASE WHEN amount > ? THEN ? ELSE ? END DESC", args: []any{100, 1, 0}},
			},
			expectedArgs: []any{100, 1, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{orderBys: tt.initialOrderBys}

			// Act
			result := b.OrderByRaw(tt.expression, tt.args...)

			// Assert
			assert.Equal(t, tt.expectedOrderBys, b.orderBys, "expected raw order by clauses to be added")
			assert.Equal(t, b, result, "expected OrderByRaw() to return the same builder instance")
		})
	}
}

func TestBuilder_OrderBySafe(t *testing.T) {
	t.Parallel()

	whitelist := map[string]string{
		"id":         "id",
		"name":       "name",
		"email":      "u.email",
		"created_at": "created_at",
	}

	tests := []struct {
		name             string
		initialOrderBys  []orderBy
		userInput        string
		dir              string
		whitelist        map[string]string
		expectedOrderBys []orderBy
	}{
		{
			name:            "should add valid column and direction",
			initialOrderBys: []orderBy{},
			userInput:       "id",
			dir:             "asc",
			whitelist:       whitelist,
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
			},
		},
		{
			name:            "should add valid column with DESC direction",
			initialOrderBys: []orderBy{},
			userInput:       "name",
			dir:             "DESC",
			whitelist:       whitelist,
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "name", dir: "DESC"},
			},
		},
		{
			name:            "should default to ASC for invalid direction",
			initialOrderBys: []orderBy{},
			userInput:       "created_at",
			dir:             "invalid",
			whitelist:       whitelist,
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "created_at", dir: "ASC"},
			},
		},
		{
			name:            "should handle column with alias from whitelist",
			initialOrderBys: []orderBy{},
			userInput:       "email",
			dir:             "desc",
			whitelist:       whitelist,
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "u.email", dir: "DESC"},
			},
		},
		{
			name: "should handle multiple safe order by calls",
			initialOrderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
			},
			userInput: "name",
			dir:       "ASC",
			whitelist: whitelist,
			expectedOrderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
				{queryType: QueryBasic, column: "name", dir: "ASC"},
			},
		},
		{
			name:             "should not add invalid column",
			initialOrderBys:  []orderBy{},
			userInput:        "invalid_col",
			dir:              "asc",
			whitelist:        whitelist,
			expectedOrderBys: []orderBy{},
		},
		{
			name:             "should not add column if whitelist is empty",
			initialOrderBys:  []orderBy{},
			userInput:        "id",
			dir:              "asc",
			whitelist:        map[string]string{},
			expectedOrderBys: []orderBy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{orderBys: tt.initialOrderBys}

			// Act
			result := b.OrderBySafe(tt.userInput, tt.dir, tt.whitelist)

			// Assert
			assert.Equal(t, tt.expectedOrderBys, b.orderBys, "expected order by clauses to be updated correctly")
			assert.Equal(t, b, result, "expected OrderBySafe() to return the same builder instance")
		})
	}
}

// -----------------
// --- BENCHMARK ---
// -----------------

func BenchmarkBuilder_OrderBy(b *testing.B) {
	builder := &builder{}
	column := "created_at"
	direction := "DESC"

	for b.Loop() {
		builder.OrderBy(column, direction)
	}
}

func BenchmarkBuilder_OrderByRaw(b *testing.B) {
	builder := &builder{}
	expression := "LENGTH(name) DESC"
	args := []any{}

	for b.Loop() {
		builder.OrderByRaw(expression, args...)
	}
}

func BenchmarkBuilder_OrderBySafe(b *testing.B) {
	builder := &builder{}
	userInput := "created_at"
	dir := "DESC"
	whitelist := map[string]string{
		"id":         "id",
		"name":       "name",
		"email":      "u.email",
		"created_at": "created_at",
	}

	for b.Loop() {
		builder.OrderBySafe(userInput, dir, whitelist)
	}
}
