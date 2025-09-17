package goquerybuilder

import (
	"testing"

	"github.com/shoraid/go-querybuilder/dialect"
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
				dialect: dialect.PostgresDialect{}, // use Postgres for quoting
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
				assert.Contains(t, err.Error(), tt.expectedError, "error message should match")
				assert.Empty(t, sql, "SQL should be empty on error")
				assert.Empty(t, args, "Args should be empty on error")
				return
			}

			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "SQL output should match")
			assert.Empty(t, args, "expected no args for simple SELECT")
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
				dialect:  dialect.PostgresDialect{},
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
			assert.Equal(t, tt.expectedSQL, sql, "SQL output should match")
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
				dialect: dialect.PostgresDialect{},
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
		name        string
		table       string
		columns     []string
		orderBys    []string
		limit       int
		offset      int
		expectedSQL string
	}{
		{
			name:        "should combine columns, order by, limit, and offset",
			table:       "products p",
			columns:     []string{"p.id", "p.name"},
			orderBys:    []string{`"p"."name" ASC`, `"p"."price" DESC`},
			limit:       10,
			offset:      20,
			expectedSQL: `SELECT "p"."id", "p"."name" FROM "products" AS p ORDER BY "p"."name" ASC, "p"."price" DESC LIMIT 10 OFFSET 20`,
		},
		{
			name:        "should combine with only limit",
			table:       "users",
			columns:     []string{"id"},
			orderBys:    []string{},
			limit:       5,
			offset:      -1,
			expectedSQL: `SELECT "id" FROM "users" LIMIT 5`,
		},
		{
			name:        "should combine with only order by",
			table:       "orders",
			columns:     []string{},
			orderBys:    []string{`"created_at" DESC`},
			limit:       -1,
			offset:      -1,
			expectedSQL: `SELECT * FROM "orders" ORDER BY "created_at" DESC`,
		},
		{
			name:        "should combine with all clauses, no columns (select *)",
			table:       "items",
			columns:     []string{},
			orderBys:    []string{`"category" ASC`},
			limit:       100,
			offset:      0,
			expectedSQL: `SELECT * FROM "items" ORDER BY "category" ASC LIMIT 100 OFFSET 0`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect:  dialect.PostgresDialect{},
				action:   "select",
				table:    tt.table,
				columns:  tt.columns,
				orderBys: tt.orderBys,
				limit:    tt.limit,
				offset:   tt.offset,
			}

			// Act
			sql, args, err := b.ToSQL()

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "SQL output should match")
			assert.Empty(t, args, "expected no args for combined SELECT")
		})
	}
}
