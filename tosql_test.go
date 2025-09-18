package goquerybuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_ToSQL_Select_Simple(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		action        string
		table         string
		columns       []string
		expectedSQL   string
		expectedError string
	}{
		{
			name:          "should return error when table is empty",
			action:        "select",
			table:         "",
			expectedSQL:   "",
			expectedError: "no table specified",
		},
		{
			name:        "should build select all query when columns are empty",
			action:      "select",
			table:       "users",
			columns:     []string{},
			expectedSQL: `SELECT * FROM "users"`,
		},
		{
			name:        "should build select with single column",
			action:      "select",
			table:       "users",
			columns:     []string{"id"},
			expectedSQL: `SELECT "id" FROM "users"`,
		},
		{
			name:        "should build select with multiple columns",
			action:      "select",
			table:       "users",
			columns:     []string{"id", "name", "email"},
			expectedSQL: `SELECT "id", "name", "email" FROM "users"`,
		},
		{
			name:        "should build select with table alias",
			action:      "select",
			table:       "users u",
			columns:     []string{"u.id", "u.name"},
			expectedSQL: `SELECT "u"."id", "u"."name" FROM "users" AS u`,
		},
		{
			name:          "should return error on unsupported action",
			action:        "drop",
			table:         "users",
			expectedSQL:   "",
			expectedError: "unsupported action: drop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{}, // use Postgres for quoting
				action:  tt.action,
				table:   tt.table,
				columns: tt.columns,
				limit:   -1,
				offset:  -1,
			}

			// Act
			sql, args, err := b.ToSQL()

			// Assert
			if tt.expectedError != "" {
				assert.Error(t, err, "expected an error")
				assert.Contains(t, err.Error(), tt.expectedError, "expected error message to contain output")
				assert.Empty(t, sql, "expected empty SQL on error")
				assert.Empty(t, args, "expected empty args on error")
				return
			}

			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Empty(t, args, "expected no args for simple SELECT")
		})
	}
}

func TestBuilder_ToSQL_Select_Where(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		table        string
		wheres       []condition
		args         []any
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name:  "should build select with single where clause",
			table: "users",
			wheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
			},
			args:         []any{1},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1`,
			expectedArgs: []any{1},
		},
		{
			name:  "should build select with multiple where clauses",
			table: "users",
			wheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
				{conj: "AND", query: `"name" = $2`, argIndexes: []int{1}},
			},
			args:         []any{1, "John"},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 AND "name" = $2`,
			expectedArgs: []any{1, "John"},
		},
		{
			name:  "should build select with OR where clause",
			table: "products",
			wheres: []condition{
				{conj: "AND", query: `"category" = $1`, argIndexes: []int{0}},
				{conj: "OR", query: `"price" < $2`, argIndexes: []int{1}},
			},
			args:         []any{"electronics", 100},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 OR "price" < $2`,
			expectedArgs: []any{"electronics", 100},
		},
		{
			name:  "should handle where conditions with multiple args",
			table: "orders",
			wheres: []condition{
				{conj: "AND", query: `"status" IN ($1, $2)`, argIndexes: []int{0, 1}},
			},
			args:         []any{"pending", "processing"},
			expectedSQL:  `SELECT * FROM "orders" WHERE "status" IN ($1, $2)`,
			expectedArgs: []any{"pending", "processing"},
		},
		{
			name:         "should handle empty where conditions",
			table:        "users",
			wheres:       []condition{},
			args:         []any{},
			expectedSQL:  `SELECT * FROM "users"`,
			expectedArgs: nil,
		},
		{
			name:  "should handle where group",
			table: "users",
			wheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
				{conj: "AND", query: `("name" = $2 OR "age" > $3)`, argIndexes: []int{1, 2}},
			},
			args:         []any{1, "John", 25},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 AND ("name" = $2 OR "age" > $3)`,
			expectedArgs: []any{1, "John", 25},
		},
		{
			name:  "should handle nested where group",
			table: "products",
			wheres: []condition{
				{conj: "AND", query: `"category" = $1`, argIndexes: []int{0}},
				{conj: "AND", query: `("price" < $2 OR ("stock" > $3 AND "available" = $4))`, argIndexes: []int{1, 2, 3}},
			},
			args:         []any{"electronics", 100, 10, true},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND ("price" < $2 OR ("stock" > $3 AND "available" = $4))`,
			expectedArgs: []any{"electronics", 100, 10, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				action:  "select",
				table:   tt.table,
				wheres:  tt.wheres,
				args:    tt.args,
				limit:   -1,
				offset:  -1,
			}

			// Act
			sql, args, err := b.ToSQL()

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestBuilder_ToSQL_Select_OrderBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		table       string
		orderBys    []string
		expectedSQL string
	}{
		{
			name:        "should build select with single order by",
			table:       "users",
			orderBys:    []string{`"id" ASC`},
			expectedSQL: `SELECT * FROM "users" ORDER BY "id" ASC`,
		},
		{
			name:        "should build select with multiple order by",
			table:       "users",
			orderBys:    []string{`"name" DESC`, `"created_at" ASC`},
			expectedSQL: `SELECT * FROM "users" ORDER BY "name" DESC, "created_at" ASC`,
		},
		{
			name:        "should build select with raw order by",
			table:       "products",
			orderBys:    []string{`LENGTH(name) DESC`},
			expectedSQL: `SELECT * FROM "products" ORDER BY LENGTH(name) DESC`,
		},
		{
			name:        "should build select with mixed order by",
			table:       "orders",
			orderBys:    []string{`"status" ASC`, `CASE WHEN amount > 100 THEN 1 ELSE 0 END DESC`},
			expectedSQL: `SELECT * FROM "orders" ORDER BY "status" ASC, CASE WHEN amount > 100 THEN 1 ELSE 0 END DESC`,
		},
		{
			name:        "should not add order by clause if empty",
			table:       "items",
			orderBys:    []string{},
			expectedSQL: `SELECT * FROM "items"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect:  PostgresDialect{},
				action:   "select",
				table:    tt.table,
				orderBys: tt.orderBys,
				limit:    -1,
				offset:   -1,
			}

			// Act
			sql, args, err := b.ToSQL()

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Empty(t, args, "expected no args for order by SELECT")
		})
	}
}

func TestBuilder_ToSQL_Select_LimitOffset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		table       string
		limit       int
		offset      int
		expectedSQL string
	}{
		{
			name:        "should build select with limit",
			table:       "users",
			limit:       10,
			offset:      -1, // default
			expectedSQL: `SELECT * FROM "users" LIMIT 10`,
		},
		{
			name:        "should build select with offset",
			table:       "users",
			limit:       -1, // default
			offset:      5,
			expectedSQL: `SELECT * FROM "users" OFFSET 5`,
		},
		{
			name:        "should build select with limit and offset",
			table:       "users",
			limit:       10,
			offset:      5,
			expectedSQL: `SELECT * FROM "users" LIMIT 10 OFFSET 5`,
		},
		{
			name:        "should ignore negative limit and offset",
			table:       "users",
			limit:       -10,
			offset:      -5,
			expectedSQL: `SELECT * FROM "users"`,
		},
		{
			name:        "should handle zero limit and offset",
			table:       "users",
			limit:       0,
			offset:      0,
			expectedSQL: `SELECT * FROM "users" LIMIT 0 OFFSET 0`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				action:  "select",
				table:   tt.table,
				limit:   tt.limit,
				offset:  tt.offset,
			}

			// Act
			sql, args, err := b.ToSQL()

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Empty(t, args, "expected no args for limit/offset SELECT")
		})
	}
}

func TestBuilder_ToSQL_Select_Combined(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		table        string
		columns      []string
		wheres       []condition
		args         []any
		orderBys     []string
		limit        int
		offset       int
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name:         "should combine columns, order by, limit, and offset",
			table:        "products p",
			columns:      []string{"p.id", "p.name"},
			orderBys:     []string{`"p"."name" ASC`, `"p"."price" DESC`},
			limit:        10,
			offset:       20,
			expectedSQL:  `SELECT "p"."id", "p"."name" FROM "products" AS p ORDER BY "p"."name" ASC, "p"."price" DESC LIMIT 10 OFFSET 20`,
			expectedArgs: []any{},
		},
		{
			name:    "should combine columns, where, and order by",
			table:   "users",
			columns: []string{"id", "name"},
			wheres: []condition{
				{conj: "AND", query: `"id" = $1`, argIndexes: []int{0}},
				{conj: "OR", query: `"status" = $2`, argIndexes: []int{1}},
			},
			args:         []any{1, "active"},
			orderBys:     []string{`"name" ASC`},
			limit:        -1,
			offset:       -1,
			expectedSQL:  `SELECT "id", "name" FROM "users" WHERE "id" = $1 OR "status" = $2 ORDER BY "name" ASC`,
			expectedArgs: []any{1, "active"},
		},
		{
			name:    "should combine columns, where, limit, and offset",
			table:   "products",
			columns: []string{"name", "price"},
			wheres: []condition{
				{conj: "AND", query: `"price" > $1`, argIndexes: []int{0}},
			},
			args:         []any{50},
			orderBys:     []string{},
			limit:        5,
			offset:       10,
			expectedSQL:  `SELECT "name", "price" FROM "products" WHERE "price" > $1 LIMIT 5 OFFSET 10`,
			expectedArgs: []any{50},
		},
		{
			name:    "should combine columns, where, order by, limit, and offset",
			table:   "users u",
			columns: []string{"u.id", "u.name"},
			wheres: []condition{
				{conj: "AND", query: `"u"."id" = $1`, argIndexes: []int{0}},
				{conj: "AND", query: `"u"."age" > $2`, argIndexes: []int{1}},
			},
			args:         []any{1, 18},
			orderBys:     []string{`"u"."name" ASC`},
			limit:        10,
			offset:       0,
			expectedSQL:  `SELECT "u"."id", "u"."name" FROM "users" AS u WHERE "u"."id" = $1 AND "u"."age" > $2 ORDER BY "u"."name" ASC LIMIT 10 OFFSET 0`,
			expectedArgs: []any{1, 18},
		},
		{
			name:    "should combine all clauses",
			table:   "orders o",
			columns: []string{"o.id", "o.total_amount", "u.name AS customer_name"},
			wheres: []condition{
				{conj: "AND", query: `"o"."status" = $1`, argIndexes: []int{0}},
				{conj: "AND", query: `"o"."order_date" > $2`, argIndexes: []int{1}},
				{conj: "OR", query: `"o"."total_amount" > $3`, argIndexes: []int{2}},
			},
			args: []any{
				"completed",
				"2023-01-01",
				1000,
			},
			orderBys:    []string{`"o"."order_date" DESC`, `"o"."total_amount" DESC`},
			limit:       20,
			offset:      40,
			expectedSQL: `SELECT "o"."id", "o"."total_amount", "u"."name" AS customer_name FROM "orders" AS o WHERE "o"."status" = $1 AND "o"."order_date" > $2 OR "o"."total_amount" > $3 ORDER BY "o"."order_date" DESC, "o"."total_amount" DESC LIMIT 20 OFFSET 40`,
			expectedArgs: []any{
				"completed",
				"2023-01-01",
				1000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect:  PostgresDialect{},
				action:   "select",
				table:    tt.table,
				columns:  tt.columns,
				wheres:   tt.wheres,
				args:     tt.args,
				orderBys: tt.orderBys,
				limit:    tt.limit,
				offset:   tt.offset,
			}

			// Act
			sql, args, err := b.ToSQL()

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			if len(tt.expectedArgs) == 0 {
				assert.Empty(t, args, "expected no args")
			} else {
				assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
			}
		})
	}
}
