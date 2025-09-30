package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresDialect_Capabilities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		expectedExcept    bool
		expectedFullJoin  bool
		expectedIntersect bool
		expectedReturning bool
	}{
		{
			name:              "should return correct capabilities for Postgres",
			expectedExcept:    true,
			expectedFullJoin:  true,
			expectedIntersect: true,
			expectedReturning: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			d := PostgresDialect{}

			// Act
			caps := d.Capabilities()

			// Assert
			assert.Equal(t, tt.expectedExcept, caps.SupportsExcept, "expected SupportsExcept to match")
			assert.Equal(t, tt.expectedFullJoin, caps.SupportsFullJoin, "expected SupportsFullJoin to match")
			assert.Equal(t, tt.expectedIntersect, caps.SupportsIntersect, "expected SupportsIntersect to match")
			assert.Equal(t, tt.expectedReturning, caps.SupportsReturning, "expected SupportsReturning to match")
		})
	}
}

func TestPostgresDialect_Placeholder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		n        int
		expected string
	}{
		{
			name:     "should return $1 for first placeholder",
			n:        1,
			expected: "$1",
		},
		{
			name:     "should return $2 for second placeholder",
			n:        2,
			expected: "$2",
		},
		{
			name:     "should return $10 for tenth placeholder",
			n:        10,
			expected: "$10",
		},
		{
			name:     "should return $100 for hundredth placeholder",
			n:        100,
			expected: "$100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			d := PostgresDialect{}

			// Act
			result := d.Placeholder(tt.n)

			// Assert
			assert.Equal(t, tt.expected, result, "expected placeholder to match")
		})
	}
}

func TestPostgresDialect_WrapColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "should quote simple column without alias",
			input:    "id",
			expected: `"id"`,
		},
		{
			name:     "should quote column with alias using AS",
			input:    "name AS username",
			expected: `"name" AS "username"`,
		},
		{
			name:     "should quote column with alias using mixed case",
			input:    "Name AS UserName",
			expected: `"Name" AS "UserName"`,
		},
		{
			name:     "should quote table.column without alias",
			input:    "users.id",
			expected: `"users"."id"`,
		},
		{
			name:     "should quote table.column with alias",
			input:    "users.id AS user_id",
			expected: `"users"."id" AS "user_id"`,
		},
		{
			name:     "should quote column with SQL function and alias",
			input:    "COUNT(id) AS total",
			expected: `"COUNT(id)" AS "total"`, // function treated as identifier
		},
		{
			name:     "should handle extra spaces before alias",
			input:    "email     AS    email_address",
			expected: `"email" AS "email_address"`,
		},
		{
			name:     "should quote column with underscore",
			input:    "user_name",
			expected: `"user_name"`,
		},
		{
			name:     "should quote column with number",
			input:    "column1",
			expected: `"column1"`,
		},
		{
			name:     "should quote column with special char",
			input:    "order-items",
			expected: `"order-items"`,
		},
		{
			name:     "should not quote empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			d := PostgresDialect{}

			// Act
			result := d.WrapColumn(tt.input)

			// Assert
			assert.Equal(t, tt.expected, result, "expected quoted column to match")
		})
	}
}

func TestPostgresDialect_WrapIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "should quote single identifier",
			input:    "users",
			expected: `"users"`,
		},
		{
			name:     "should quote schema and table",
			input:    "public.users",
			expected: `"public"."users"`,
		},
		{
			name:     "should quote table and column",
			input:    "users.id",
			expected: `"users"."id"`,
		},
		{
			name:     "should quote multi-level identifier",
			input:    "db1.public.users",
			expected: `"db1"."public"."users"`,
		},
		{
			name:     "should quote identifier with underscore",
			input:    "user_profile",
			expected: `"user_profile"`,
		},
		{
			name:     "should quote identifier with number",
			input:    "column1",
			expected: `"column1"`,
		},
		{
			name:     "should quote identifier with mixed case",
			input:    "UserName",
			expected: `"UserName"`,
		},
		{
			name:     "should quote identifier with special char",
			input:    "order-items",
			expected: `"order-items"`,
		},
		{
			name:     "should not quote empty identifier",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			d := PostgresDialect{}

			// Act
			result := d.WrapIdentifier(tt.input)

			// Assert
			assert.Equal(t, tt.expected, result, "expected quoted identifier to match")
		})
	}
}

func TestPostgresDialect_WrapTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "should quote simple table without alias",
			input:    "users",
			expected: `"users"`,
		},
		{
			name:     "should quote schema-qualified table without alias",
			input:    "public.users",
			expected: `"public"."users"`,
		},
		{
			name:     "should quote table with alias",
			input:    "users u",
			expected: `"users" AS "u"`,
		},
		{
			name:     "should quote table with alias containing number",
			input:    "orders o1",
			expected: `"orders" AS "o1"`,
		},
		{
			name:     "should handle extra spaces between table and alias",
			input:    "users     u",
			expected: `"users" AS "u"`,
		},
		{
			name:     "should quote table name with underscore",
			input:    "user_profile up",
			expected: `"user_profile" AS "up"`,
		},
		{
			name:     "should quote table name with hyphen",
			input:    "order-items oi",
			expected: `"order-items" AS "oi"`,
		},
		{
			name:     "should quote table name without alias but with mixed case",
			input:    "UserTable",
			expected: `"UserTable"`,
		},
		{
			name:     "should quote table name with alias and mixed case",
			input:    "UserTable ut",
			expected: `"UserTable" AS "ut"`,
		},
		{
			name:     "should not quote empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			d := PostgresDialect{}

			// Act
			result := d.WrapTable(tt.input)

			// Assert
			assert.Equal(t, tt.expected, result, "expected quoted table with alias to match")
		})
	}
}

func TestPostgresDialect_CompileSelect_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build select all query when columns are empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users")
			},
			expectedSQL:  `SELECT * FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build select with single basic column",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					From("users")
			},
			expectedSQL:  `SELECT "id" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build select with multiple basic columns",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id", "name", "email").
					From("users")
			},
			expectedSQL:  `SELECT "id", "name", "email" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build select with table alias and qualified columns",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("u.id", "u.name", "u.email AS user_email").
					From("public.users u")
			},
			expectedSQL:  `SELECT "u"."id", "u"."name", "u"."email" AS "user_email" FROM "public"."users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name: "should wrap aggregate function (current query builder behavior)",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id", "COUNT(*)", "SUM(orders.price) as total_price").
					From("orders")
			},
			expectedSQL:  `SELECT "id", "COUNT(*)", "SUM(orders"."price)" AS "total_price" FROM "orders"`,
			expectedArgs: []any{},
		},
		{
			name: "should return error when table is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("")
			},
			expectedSQL:   "",
			expectedArgs:  []any{},
			expectedError: "no table specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_SelectRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name: "should build raw select expression without args",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectRaw("COUNT(*) AS total").
					From("users")
			},
			expectedSQL:  `SELECT COUNT(*) AS total FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build raw select expression with single arg",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectRaw("DATE(timestamp) > ? AS log_date", "2025-01-01").
					From("logs")
			},
			expectedSQL:  `SELECT DATE(timestamp) > $1 AS log_date FROM "logs"`,
			expectedArgs: []any{"2025-01-01"},
		},
		{
			name: "should build raw select expression with multiple args",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectRaw("value BETWEEN ? AND ? AS range_check", 10, 20).
					From("metrics")
			},
			expectedSQL:  `SELECT value BETWEEN $1 AND $2 AS range_check FROM "metrics"`,
			expectedArgs: []any{10, 20},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_SelectSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name: "should build safe select expression",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"id", "name"}
				whitelist := map[string]string{
					"id":   "id",
					"name": "name",
				}

				return b.
					SelectSafe(userInput, whitelist).
					From("users")
			},
			expectedSQL:  `SELECT "id", "name" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build safe select expression with alias",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"id", "name", "u.email"}
				whitelist := map[string]string{
					"id":      "u.id",
					"name":    "u.name",
					"u.email": "u.email",
				}

				return b.
					SelectSafe(userInput, whitelist).
					From("users u")
			},
			expectedSQL:  `SELECT "u"."id", "u"."name", "u"."email" FROM "users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name: "should ignore inputs not in whitelist",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"id", "not_allowed"}
				whitelist := map[string]string{
					"id": "id",
				}

				return b.
					SelectSafe(userInput, whitelist).
					From("users")
			},
			expectedSQL:  `SELECT "id" FROM "users"`,
			expectedArgs: []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_AddSelect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name: "should add a single column to select",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelect("name").
					From("users")
			},
			expectedSQL:  `SELECT "id", "name" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should add multiple columns to select",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelect("name", "email").
					AddSelect("age").
					From("users")
			},
			expectedSQL:  `SELECT "id", "name", "email", "age" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should add alias column to select",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("u.id").
					AddSelect("u.name AS user_name").
					From("users u")
			},
			expectedSQL:  `SELECT "u"."id", "u"."name" AS "user_name" FROM "users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name: "should add select expression to an empty select",
			build: func(b *builder) QueryBuilder {
				return b.
					AddSelect("email").
					From("users")
			},
			expectedSQL:  `SELECT "email" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should add wrapped aggregate function (current query builder behavior)",
			build: func(b *builder) QueryBuilder {
				return b.
					AddSelect("COUNT(*)", "SUM(orders.price) as total_price").
					From("orders")
			},
			expectedSQL:  `SELECT "COUNT(*)", "SUM(orders"."price)" AS "total_price" FROM "orders"`,
			expectedArgs: []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_AddSelectRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name: "should add raw select expression",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectRaw("COUNT(*) AS total").
					From("users")
			},
			expectedSQL:  `SELECT "id", COUNT(*) AS total FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should add raw select expression with args",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectRaw("DATE(created_at) > ?", "2023-01-01").
					From("users")
			},
			expectedSQL:  `SELECT "id", DATE(created_at) > $1 FROM "users"`,
			expectedArgs: []any{"2023-01-01"},
		},
		{
			name: "should add multiple raw select expressions with multiple args",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectRaw("DATE(created_at) > ?", "2023-01-01").
					AddSelectRaw("EXTRACT(YEAR FROM created_at) BETWEEN ? AND ?", 2020, 2023).
					From("users")
			},
			expectedSQL:  `SELECT "id", DATE(created_at) > $1, EXTRACT(YEAR FROM created_at) BETWEEN $2 AND $3 FROM "users"`,
			expectedArgs: []any{"2023-01-01", 2020, 2023},
		},
		{
			name: "should add raw select expression to an empty select",
			build: func(b *builder) QueryBuilder {
				return b.
					AddSelectRaw("COUNT(*) AS total").
					From("users")
			},
			expectedSQL:  `SELECT COUNT(*) AS total FROM "users"`,
			expectedArgs: []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_AddSelectSafe(t *testing.T) {
	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name: "should add safe select expression",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"name"}
				whitelist := map[string]string{
					"id":   "id",
					"name": "name",
				}

				return b.
					Select("id").
					AddSelectSafe(userInput, whitelist).
					From("users")
			},
			expectedSQL:  `SELECT "id", "name" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should add safe select expression with alias",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"email"}
				whitelist := map[string]string{
					"email": "u.email",
				}

				return b.
					Select("u.id").
					AddSelectSafe(userInput, whitelist).
					From("users u")
			},
			expectedSQL:  `SELECT "u"."id", "u"."email" FROM "users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name: "should ignore inputs not in whitelist when adding safe select",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"not_allowed", "name"}
				whitelist := map[string]string{
					"name": "name",
				}

				return b.
					Select("id").
					AddSelectSafe(userInput, whitelist).
					From("users")
			},
			expectedSQL:  `SELECT "id", "name" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should add safe select expression to an empty select",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"name"}
				whitelist := map[string]string{
					"name": "name",
				}

				return b.
					AddSelectSafe(userInput, whitelist).
					From("users")
			},
			expectedSQL:  `SELECT "name" FROM "users"`,
			expectedArgs: []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

			// Assert
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match output")
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_Select_Where_Simple(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		table         string
		wheres        []where
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name:  "should build select with single basic where clause",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1`,
			expectedArgs: []any{1},
		},
		{
			name:  "should build select with multiple basic where clauses",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
				{conj: "AND", queryType: QueryBasic, column: "name", operator: "=", args: []any{"John"}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 AND "name" = $2`,
			expectedArgs: []any{1, "John"},
		},
		{
			name:  "should build select with OR where clause",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "OR", queryType: QueryBasic, column: "price", operator: "<", args: []any{100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 OR "price" < $2`,
			expectedArgs: []any{"electronics", 100},
		},
		{
			name:         "should handle empty where conditions",
			table:        "users",
			wheres:       []where{},
			expectedSQL:  `SELECT * FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name:  "should default to AND when conjunction is missing",
			table: "products",
			wheres: []where{
				{queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{ /* no conj provided */ queryType: QueryBasic, column: "price", operator: ">", args: []any{100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND "price" > $2`,
			expectedArgs: []any{"electronics", 100},
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
				limit:   -1,
				offset:  -1,
			}

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build single basic between clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereBetween("price", 10, 100)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" BETWEEN $1 AND $2)`,
			expectedArgs: []any{10, 100},
		},
		{
			name: "should build multiple basic between clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereBetween("price", 10, 100).
					WhereBetween("weight", 1, 5)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" BETWEEN $1 AND $2) AND ("weight" BETWEEN $3 AND $4)`,
			expectedArgs: []any{10, 100, 1, 5},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereBetween("", 10, 100)
			},
			expectedError: "WHERE clause requires non-empty column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build single OR between clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("quantity", ">", 5).
					OrWhereBetween("price", 10, 100)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "quantity" > $1 OR ("price" BETWEEN $2 AND $3)`,
			expectedArgs: []any{5, 10, 100},
		},
		{
			name: "should build multiple OR between clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("quantity", ">", 5).
					OrWhereBetween("price", 10, 100).
					OrWhereBetween("weight", 1, 5)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "quantity" > $1 OR ("price" BETWEEN $2 AND $3) OR ("weight" BETWEEN $4 AND $5)`,
			expectedArgs: []any{5, 10, 100, 1, 5},
		},
		{
			name: "should treat leading OrWhereBetween as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereBetween("price", 10, 100)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" BETWEEN $1 AND $2)`,
			expectedArgs: []any{10, 100},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereBetween("", 10, 100)
			},
			expectedError: "WHERE clause requires non-empty column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereNotBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build single basic not between clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereNotBetween("price", 10, 100)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" NOT BETWEEN $1 AND $2)`,
			expectedArgs: []any{10, 100},
		},
		{
			name: "should build multiple basic not between clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereNotBetween("price", 10, 100).
					WhereNotBetween("weight", 1, 5)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" NOT BETWEEN $1 AND $2) AND ("weight" NOT BETWEEN $3 AND $4)`,
			expectedArgs: []any{10, 100, 1, 5},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereNotBetween("", 10, 100)
			},
			expectedError: "WHERE clause requires non-empty column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereNotBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build single OR not between clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("quantity", ">", 5).
					OrWhereNotBetween("price", 10, 100)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "quantity" > $1 OR ("price" NOT BETWEEN $2 AND $3)`,
			expectedArgs: []any{5, 10, 100},
		},
		{
			name: "should build multiple OR not between clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("quantity", ">", 5).
					OrWhereNotBetween("price", 10, 100).
					OrWhereNotBetween("weight", 1, 5)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "quantity" > $1 OR ("price" NOT BETWEEN $2 AND $3) OR ("weight" NOT BETWEEN $4 AND $5)`,
			expectedArgs: []any{5, 10, 100, 1, 5},
		},
		{
			name: "should treat leading OrWhereNotBetween as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereNotBetween("price", 10, 100)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" NOT BETWEEN $1 AND $2)`,
			expectedArgs: []any{10, 100},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereNotBetween("", 10, 100)
			},
			expectedError: "WHERE clause requires non-empty column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build IN clause with single value on customers",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					WhereIn("id", 1)
			},
			expectedSQL:  `SELECT * FROM "customers" WHERE "id" IN ($1)`,
			expectedArgs: []any{1},
		},
		{
			name: "should build IN clause with multiple values on orders",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("orders").
					WhereIn("id", 101, 102, 103)
			},
			expectedSQL:  `SELECT * FROM "orders" WHERE "id" IN ($1, $2, $3)`,
			expectedArgs: []any{101, 102, 103},
		},
		{
			name: "should build multiple IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereIn("category_id", 1, 2).
					WhereIn("status", "active", "pending")
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" IN ($1, $2) AND "status" IN ($3, $4)`,
			expectedArgs: []any{1, 2, "active", "pending"},
		},
		{
			name: "should build IN clause from single slice on products",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereIn("category_id", []int{11, 12, 13})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" IN ($1, $2, $3)`,
			expectedArgs: []any{11, 12, 13},
		},
		{
			name: "should build IN clause from multiple slices on employees",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("employees").
					WhereIn("department_id", []int{1, 2}, []string{"HR", "Finance"})
			},
			expectedSQL:  `SELECT * FROM "employees" WHERE "department_id" IN ($1, $2, $3, $4)`,
			expectedArgs: []any{1, 2, "HR", "Finance"},
		},
		{
			name: "should build IN clause from mixed values and slice on invoices",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("invoices").
					WhereIn("status", "paid", []string{"pending", "overdue"})
			},
			expectedSQL:  `SELECT * FROM "invoices" WHERE "status" IN ($1, $2, $3)`,
			expectedArgs: []any{"paid", "pending", "overdue"},
		},
		{
			name: "should build IN clause with boolean values on accounts",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("accounts").
					WhereIn("is_verified", true, false)
			},
			expectedSQL:  `SELECT * FROM "accounts" WHERE "is_verified" IN ($1, $2)`,
			expectedArgs: []any{true, false},
		},
		{
			name: "should replace empty slice with 1=0 on shipments",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("shipments").
					Where("carrier", "=", "DHL").
					WhereIn("tracking_number", []any{})
			},
			expectedSQL:  `SELECT * FROM "shipments" WHERE "carrier" = $1 AND 1 = 0`,
			expectedArgs: []any{"DHL"},
		},
		{
			name: "should return error when column name is empty on warehouses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("warehouses").
					WhereIn("", 1, 2)
			},
			expectedError: "WHERE clause requires non-empty column",
		},
		{
			name: "should return error when nil is passed directly on products",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					WhereIn("status", nil, "active")
			},
			expectedError: "IN clause does not support nil, use IS NULL instead",
		},
		{
			name: "should return error when slice contains nil on customers",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					Where("country", "=", "US").
					WhereIn("segment", []any{"premium", nil})
			},
			expectedError: "IN clause contains nil value in slice, use IS NULL instead",
		},
		{
			name: "should return error when nested slice is passed on suppliers",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("suppliers").
					WhereIn("id", [][]int{{1, 2}})
			},
			expectedError: "IN clause does not support nested slices",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build OR IN clause with single value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					Where("country", "=", "US").
					OrWhereIn("id", 1)
			},
			expectedSQL:  `SELECT * FROM "customers" WHERE "country" = $1 OR "id" IN ($2)`,
			expectedArgs: []any{"US", 1},
		},
		{
			name: "should build OR IN clause with multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("orders").
					Where("status", "=", "pending").
					OrWhereIn("id", 101, 102, 103)
			},
			expectedSQL:  `SELECT * FROM "orders" WHERE "status" = $1 OR "id" IN ($2, $3, $4)`,
			expectedArgs: []any{"pending", 101, 102, 103},
		},
		{
			name: "should build multiple OR IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "=", 1).
					OrWhereIn("unit_id", 1, 2).
					OrWhereIn("status", "active", "pending")
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" = $1 OR "unit_id" IN ($2, $3) OR "status" IN ($4, $5)`,
			expectedArgs: []any{1, 1, 2, "active", "pending"},
		},
		{
			name: "should build OR IN clause from single slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 50).
					OrWhereIn("category_id", []int{11, 12, 13})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" IN ($2, $3, $4)`,
			expectedArgs: []any{50, 11, 12, 13},
		},
		{
			name: "should build OR IN clause from multiple slices",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("employees").
					Where("active", "=", true).
					OrWhereIn("department_id", []int{1, 2}, []string{"HR", "Finance"})
			},
			expectedSQL:  `SELECT * FROM "employees" WHERE "active" = $1 OR "department_id" IN ($2, $3, $4, $5)`,
			expectedArgs: []any{true, 1, 2, "HR", "Finance"},
		},
		{
			name: "should build OR IN clause from mixed values and slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("invoices").
					Where("amount", ">", 1000).
					OrWhereIn("status", "paid", []string{"pending", "overdue"})
			},
			expectedSQL:  `SELECT * FROM "invoices" WHERE "amount" > $1 OR "status" IN ($2, $3, $4)`,
			expectedArgs: []any{1000, "paid", "pending", "overdue"},
		},
		{
			name: "should build OR IN clause with boolean values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("accounts").
					Where("created_at", ">", "2023-01-01").
					OrWhereIn("is_verified", true, false)
			},
			expectedSQL:  `SELECT * FROM "accounts" WHERE "created_at" > $1 OR "is_verified" IN ($2, $3)`,
			expectedArgs: []any{"2023-01-01", true, false},
		},
		{
			name: "should replace empty slice with 1=0 in OR clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("shipments").
					Where("carrier", "=", "DHL").
					OrWhereIn("tracking_number", []any{})
			},
			expectedSQL:  `SELECT * FROM "shipments" WHERE "carrier" = $1 OR 1 = 0`,
			expectedArgs: []any{"DHL"},
		},
		{
			name: "should return error when column name is empty in OR clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("warehouses").
					Where("location", "=", "NYC").
					OrWhereIn("", 1, 2)
			},
			expectedError: "WHERE clause requires non-empty column",
		},
		{
			name: "should return error when nil is passed directly in OR clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhereIn("status", nil, "active")
			},
			expectedError: "IN clause does not support nil, use IS NULL instead",
		},
		{
			name: "should return error when slice contains nil in OR clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					Where("country", "=", "US").
					OrWhereIn("segment", []any{"premium", nil})
			},
			expectedError: "IN clause contains nil value in slice, use IS NULL instead",
		},
		{
			name: "should return error when nested slice is passed in OR clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("suppliers").
					Where("active", "=", true).
					OrWhereIn("id", [][]int{{1, 2}})
			},
			expectedError: "IN clause does not support nested slices",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereNotIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build NOT IN clause with single value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					WhereNotIn("id", 1)
			},
			expectedSQL:  `SELECT * FROM "customers" WHERE "id" NOT IN ($1)`,
			expectedArgs: []any{1},
		},
		{
			name: "should build NOT IN clause with multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("orders").
					WhereNotIn("id", 101, 102, 103)
			},
			expectedSQL:  `SELECT * FROM "orders" WHERE "id" NOT IN ($1, $2, $3)`,
			expectedArgs: []any{101, 102, 103},
		},
		{
			name: "should build multiple NOT IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereNotIn("category_id", 1, 2).
					WhereNotIn("status", "inactive", "archived")
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" NOT IN ($1, $2) AND "status" NOT IN ($3, $4)`,
			expectedArgs: []any{1, 2, "inactive", "archived"},
		},
		{
			name: "should build NOT IN clause from single slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereNotIn("category_id", []int{11, 12, 13})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" NOT IN ($1, $2, $3)`,
			expectedArgs: []any{11, 12, 13},
		},
		{
			name: "should build NOT IN clause from multiple slices",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("employees").
					WhereNotIn("department_id", []int{1, 2}, []string{"HR", "Finance"})
			},
			expectedSQL:  `SELECT * FROM "employees" WHERE "department_id" NOT IN ($1, $2, $3, $4)`,
			expectedArgs: []any{1, 2, "HR", "Finance"},
		},
		{
			name: "should build NOT IN clause from mixed values and slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("invoices").
					WhereNotIn("status", "paid", []string{"pending", "overdue"})
			},
			expectedSQL:  `SELECT * FROM "invoices" WHERE "status" NOT IN ($1, $2, $3)`,
			expectedArgs: []any{"paid", "pending", "overdue"},
		},
		{
			name: "should build NOT IN clause with boolean values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("accounts").
					WhereNotIn("is_verified", true, false)
			},
			expectedSQL:  `SELECT * FROM "accounts" WHERE "is_verified" NOT IN ($1, $2)`,
			expectedArgs: []any{true, false},
		},
		{
			name: "should replace empty slice with 1=1",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("shipments").
					Where("carrier", "=", "DHL").
					WhereNotIn("tracking_number", []any{})
			},
			expectedSQL:  `SELECT * FROM "shipments" WHERE "carrier" = $1 AND 1 = 1`,
			expectedArgs: []any{"DHL"},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("warehouses").
					WhereNotIn("", 1, 2)
			},
			expectedError: "WHERE clause requires non-empty column",
		},
		{
			name: "should return error when nil is passed directly",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					WhereNotIn("status", nil, "active")
			},
			expectedError: "IN clause does not support nil, use IS NULL instead",
		},
		{
			name: "should return error when slice contains nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					Where("country", "=", "US").
					WhereNotIn("segment", []any{"premium", nil})
			},
			expectedError: "IN clause contains nil value in slice, use IS NULL instead",
		},
		{
			name: "should return error when nested slice is passed",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("suppliers").
					WhereNotIn("id", [][]int{{1, 2}})
			},
			expectedError: "IN clause does not support nested slices",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereNotIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build OR NOT IN clause with single value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					Where("country", "=", "US").
					OrWhereNotIn("id", 1)
			},
			expectedSQL:  `SELECT * FROM "customers" WHERE "country" = $1 OR "id" NOT IN ($2)`,
			expectedArgs: []any{"US", 1},
		},
		{
			name: "should build OR NOT IN clause with multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("orders").
					Where("status", "=", "pending").
					OrWhereNotIn("id", 101, 102, 103)
			},
			expectedSQL:  `SELECT * FROM "orders" WHERE "status" = $1 OR "id" NOT IN ($2, $3, $4)`,
			expectedArgs: []any{"pending", 101, 102, 103},
		},
		{
			name: "should build multiple OR NOT IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "=", 1).
					OrWhereNotIn("unit_id", 1, 2).
					OrWhereNotIn("status", "inactive", "archived")
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" = $1 OR "unit_id" NOT IN ($2, $3) OR "status" NOT IN ($4, $5)`,
			expectedArgs: []any{1, 1, 2, "inactive", "archived"},
		},
		{
			name: "should build OR NOT IN clause from single slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 50).
					OrWhereNotIn("category_id", []int{11, 12, 13})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" NOT IN ($2, $3, $4)`,
			expectedArgs: []any{50, 11, 12, 13},
		},
		{
			name: "should build OR NOT IN clause from multiple slices",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("employees").
					Where("active", "=", true).
					OrWhereNotIn("department_id", []int{1, 2}, []string{"HR", "Finance"})
			},
			expectedSQL:  `SELECT * FROM "employees" WHERE "active" = $1 OR "department_id" NOT IN ($2, $3, $4, $5)`,
			expectedArgs: []any{true, 1, 2, "HR", "Finance"},
		},
		{
			name: "should build OR NOT IN clause from mixed values and slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("invoices").
					Where("amount", ">", 1000).
					OrWhereNotIn("status", "paid", []string{"pending", "overdue"})
			},
			expectedSQL:  `SELECT * FROM "invoices" WHERE "amount" > $1 OR "status" NOT IN ($2, $3, $4)`,
			expectedArgs: []any{1000, "paid", "pending", "overdue"},
		},
		{
			name: "should build OR NOT IN clause with boolean values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("accounts").
					Where("created_at", ">", "2023-01-01").
					OrWhereNotIn("is_verified", true, false)
			},
			expectedSQL:  `SELECT * FROM "accounts" WHERE "created_at" > $1 OR "is_verified" NOT IN ($2, $3)`,
			expectedArgs: []any{"2023-01-01", true, false},
		},
		{
			name: "should replace empty slice with 1=1 in OR NOT IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("shipments").
					Where("carrier", "=", "DHL").
					OrWhereNotIn("tracking_number", []any{})
			},
			expectedSQL:  `SELECT * FROM "shipments" WHERE "carrier" = $1 OR 1 = 1`,
			expectedArgs: []any{"DHL"},
		},
		{
			name: "should return error when column name is empty in OR NOT IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("warehouses").
					Where("location", "=", "NYC").
					OrWhereNotIn("", 1, 2)
			},
			expectedError: "WHERE clause requires non-empty column",
		},
		{
			name: "should return error when nil is passed directly in OR NOT IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhereNotIn("status", nil, "active")
			},
			expectedError: "IN clause does not support nil, use IS NULL instead",
		},
		{
			name: "should return error when slice contains nil in OR NOT IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					Where("country", "=", "US").
					OrWhereNotIn("segment", []any{"premium", nil})
			},
			expectedError: "IN clause contains nil value in slice, use IS NULL instead",
		},
		{
			name: "should return error when nested slice is passed in OR NOT IN clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("suppliers").
					Where("active", "=", true).
					OrWhereNotIn("id", [][]int{{1, 2}})
			},
			expectedError: "IN clause does not support nested slices",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build single IS NULL clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNull("email")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NULL`,
			expectedArgs: []any{},
		},
		{
			name: "should build multiple IS NULL clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNull("email").
					WhereNull("phone")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NULL AND "phone" IS NULL`,
			expectedArgs: []any{},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNull("")
			},
			expectedError: "WHERE clause requires non-empty column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build single OR IS NULL clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "pending").
					OrWhereNull("email")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR "email" IS NULL`,
			expectedArgs: []any{"pending"},
		},
		{
			name: "should build multiple OR IS NULL clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "pending").
					OrWhereNull("email").
					OrWhereNull("phone")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR "email" IS NULL OR "phone" IS NULL`,
			expectedArgs: []any{"pending"},
		},
		{
			name: "should treat leading OrWhereNull as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereNull("email")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NULL`,
			expectedArgs: []any{},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereNull("")
			},
			expectedError: "WHERE clause requires non-empty column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build single IS NOT NULL clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNotNull("email")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NOT NULL`,
			expectedArgs: []any{},
		},
		{
			name: "should build multiple IS NOT NULL clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNotNull("email").
					WhereNotNull("phone")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NOT NULL AND "phone" IS NOT NULL`,
			expectedArgs: []any{},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNotNull("")
			},
			expectedError: "WHERE clause requires non-empty column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build single OR IS NOT NULL clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "pending").
					OrWhereNotNull("email")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR "email" IS NOT NULL`,
			expectedArgs: []any{"pending"},
		},
		{
			name: "should build multiple OR IS NOT NULL clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "pending").
					OrWhereNotNull("email").
					OrWhereNotNull("phone")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR "email" IS NOT NULL OR "phone" IS NOT NULL`,
			expectedArgs: []any{"pending"},
		},
		{
			name: "should treat leading OrWhereNotNull as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereNotNull("email")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NOT NULL`,
			expectedArgs: []any{},
		},
		{
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereNotNull("")
			},
			expectedError: "WHERE clause requires non-empty column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build raw where clause without args",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereRaw("id = 1")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE id = 1`,
			expectedArgs: []any{},
		},
		{
			name: "should build raw where clause with single arg",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereRaw("name = ?", "John Doe")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE name = $1`,
			expectedArgs: []any{"John Doe"},
		},
		{
			name: "should build raw where clause with multiple args",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereRaw("age BETWEEN ? AND ?", 20, 30)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE age BETWEEN $1 AND $2`,
			expectedArgs: []any{20, 30},
		},
		{
			name: "should combine raw where with other where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereRaw("created_at > ?", "2023-01-01")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 AND created_at > $2`,
			expectedArgs: []any{"active", "2023-01-01"},
		},
		{
			name: "should handle multiple raw where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereRaw("status = 'active'").
					WhereRaw("created_at > ?", "2023-01-01").
					WhereRaw("age > ?", 25)

			},
			expectedSQL:  `SELECT * FROM "users" WHERE status = 'active' AND created_at > $1 AND age > $2`,
			expectedArgs: []any{"2023-01-01", 25},
		},
		{
			name: "should return error when raw query is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereRaw("")
			},
			expectedError: "WHERE RAW clause requires a non-empty query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build OR raw where clause without args",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereRaw("id = 1")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR id = 1`,
			expectedArgs: []any{"inactive"},
		},
		{
			name: "should build OR raw where clause with single arg",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereRaw("name = ?", "John Doe")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR name = $2`,
			expectedArgs: []any{"inactive", "John Doe"},
		},
		{
			name: "should build OR raw where clause with multiple args",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereRaw("age BETWEEN ? AND ?", 20, 30)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR age BETWEEN $2 AND $3`,
			expectedArgs: []any{"inactive", 20, 30},
		},
		{
			name: "should handle multiple OR raw where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereRaw("created_at > ?", "2023-01-01").
					OrWhereRaw("age > ?", 25)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR created_at > $2 OR age > $3`,
			expectedArgs: []any{"inactive", "2023-01-01", 25},
		},
		{
			name: "should treat leading OrWhereRaw as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereRaw("id = ?", 1)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE id = $1`,
			expectedArgs: []any{1},
		},
		{
			name: "should return error when raw query is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereRaw("")
			},
			expectedError: "WHERE RAW clause requires a non-empty query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build grouped where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18).
							OrWhere("age", "<", 65)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 AND ("age" > $2 OR "age" < $3)`,
			expectedArgs: []any{"active", 18, 65},
		},
		{
			name: "should build nested grouped where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18).
							OrWhereGroup(func(q2 QueryBuilder) {
								q2.Where("role", "=", "admin").
									Where("department", "=", "IT")
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 AND ("age" > $2 OR ("role" = $3 AND "department" = $4))`,
			expectedArgs: []any{"active", 18, "admin", "IT"},
		},
		{
			name: "should handle empty group",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereGroup(func(q QueryBuilder) {
						// Empty group
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1`,
			expectedArgs: []any{"active"},
		},
		{
			name: "should handle group with only one condition",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 AND ("age" > $2)`,
			expectedArgs: []any{"active", 18},
		},
		{
			name: "should handle leading WhereGroup as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18).
							OrWhere("age", "<", 65)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE ("age" > $1 OR "age" < $2)`,
			expectedArgs: []any{18, 65},
		},
		{
			name: "should handle 5-level nested where group",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereGroup(func(q1 QueryBuilder) {
						q1.Where("level1", "=", 1).
							WhereGroup(func(q2 QueryBuilder) {
								q2.Where("level2", "=", 2).
									WhereGroup(func(q3 QueryBuilder) {
										q3.Where("level3", "=", 3).
											WhereGroup(func(q4 QueryBuilder) {
												q4.Where("level4", "=", 4).
													WhereGroup(func(q5 QueryBuilder) {
														q5.Where("level5", "=", 5)
													})
											})
									})
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE ("level1" = $1 AND ("level2" = $2 AND ("level3" = $3 AND ("level4" = $4 AND ("level5" = $5)))))`,
			expectedArgs: []any{1, 2, 3, 4, 5},
		},
		{
			name: "should handle 10-level nested where group",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereGroup(func(q1 QueryBuilder) {
						q1.Where("level1", "=", 1).
							WhereGroup(func(q2 QueryBuilder) {
								q2.Where("level2", "=", 2).
									WhereGroup(func(q3 QueryBuilder) {
										q3.Where("level3", "=", 3).
											WhereGroup(func(q4 QueryBuilder) {
												q4.Where("level4", "=", 4).
													WhereGroup(func(q5 QueryBuilder) {
														q5.Where("level5", "=", 5).
															WhereGroup(func(q6 QueryBuilder) {
																q6.Where("level6", "=", 6).
																	WhereGroup(func(q7 QueryBuilder) {
																		q7.Where("level7", "=", 7).
																			WhereGroup(func(q8 QueryBuilder) {
																				q8.Where("level8", "=", 8).
																					WhereGroup(func(q9 QueryBuilder) {
																						q9.Where("level9", "=", 9).
																							WhereGroup(func(q10 QueryBuilder) {
																								q10.Where("level10", "=", 10)
																							})
																					})
																			})
																	})
															})
													})
											})
									})
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE ("level1" = $1 AND ("level2" = $2 AND ("level3" = $3 AND ("level4" = $4 AND ("level5" = $5 AND ("level6" = $6 AND ("level7" = $7 AND ("level8" = $8 AND ("level9" = $9 AND ("level10" = $10))))))))))`,
			expectedArgs: []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build OR grouped where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					OrWhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18).
							Where("age", "<", 65)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR ("age" > $2 AND "age" < $3)`,
			expectedArgs: []any{"active", 18, 65},
		},
		{
			name: "should build nested OR grouped where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					OrWhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18).
							OrWhereGroup(func(q2 QueryBuilder) {
								q2.Where("role", "=", "admin").
									Where("department", "=", "IT")
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR ("age" > $2 OR ("role" = $3 AND "department" = $4))`,
			expectedArgs: []any{"active", 18, "admin", "IT"},
		},
		{
			name: "should handle empty group",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					OrWhereGroup(func(q QueryBuilder) {
						// Empty group
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1`,
			expectedArgs: []any{"active"},
		},
		{
			name: "should handle group with only one condition",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					OrWhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR ("age" > $2)`,
			expectedArgs: []any{"active", 18},
		},
		{
			name: "should handle leading OrWhereGroup as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18).
							Where("age", "<", 65)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE ("age" > $1 AND "age" < $2)`,
			expectedArgs: []any{18, 65},
		},
		{
			name: "should handle 5-level nested or where group",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("initial_status", "=", "pending").
					OrWhereGroup(func(q1 QueryBuilder) {
						q1.Where("level1", "=", 1).
							OrWhereGroup(func(q2 QueryBuilder) {
								q2.Where("level2", "=", 2).
									OrWhereGroup(func(q3 QueryBuilder) {
										q3.Where("level3", "=", 3).
											OrWhereGroup(func(q4 QueryBuilder) {
												q4.Where("level4", "=", 4).
													OrWhereGroup(func(q5 QueryBuilder) {
														q5.Where("level5", "=", 5)
													})
											})
									})
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "initial_status" = $1 OR ("level1" = $2 OR ("level2" = $3 OR ("level3" = $4 OR ("level4" = $5 OR ("level5" = $6)))))`,
			expectedArgs: []any{"pending", 1, 2, 3, 4, 5},
		},
		{
			name: "should handle 10-level nested or where group",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("initial_status", "=", "pending").
					OrWhereGroup(func(q1 QueryBuilder) {
						q1.Where("level1", "=", 1).
							OrWhereGroup(func(q2 QueryBuilder) {
								q2.Where("level2", "=", 2).
									OrWhereGroup(func(q3 QueryBuilder) {
										q3.Where("level3", "=", 3).
											OrWhereGroup(func(q4 QueryBuilder) {
												q4.Where("level4", "=", 4).
													OrWhereGroup(func(q5 QueryBuilder) {
														q5.Where("level5", "=", 5).
															OrWhereGroup(func(q6 QueryBuilder) {
																q6.Where("level6", "=", 6).
																	OrWhereGroup(func(q7 QueryBuilder) {
																		q7.Where("level7", "=", 7).
																			OrWhereGroup(func(q8 QueryBuilder) {
																				q8.Where("level8", "=", 8).
																					OrWhereGroup(func(q9 QueryBuilder) {
																						q9.Where("level9", "=", 9).
																							OrWhereGroup(func(q10 QueryBuilder) {
																								q10.Where("level10", "=", 10)
																							})
																					})
																			})
																	})
															})
													})
											})
									})
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "initial_status" = $1 OR ("level1" = $2 OR ("level2" = $3 OR ("level3" = $4 OR ("level4" = $5 OR ("level5" = $6 OR ("level6" = $7 OR ("level7" = $8 OR ("level8" = $9 OR ("level9" = $10 OR ("level10" = $11))))))))))`,
			expectedArgs: []any{"pending", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_WhereSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build where clause with subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereSub("id", "IN", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" IN (SELECT "user_id" FROM "orders" WHERE "amount" > $1)`,
			expectedArgs: []any{100},
		},
		{
			name: "should build where clause with subquery and alias",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users u").
					WhereSub("u.id", "IN", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" AS "u" WHERE "u"."id" IN (SELECT "user_id" FROM "orders" WHERE "amount" > $1)`,
			expectedArgs: []any{100},
		},
		{
			name: "should build where clause with subquery and multiple conditions",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "=", 1).
					WhereSub("id", "NOT IN", func(q QueryBuilder) {
						q.Select("product_id").
							From("order_items").
							Where("quantity", ">", 5)
					})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" = $1 AND "id" NOT IN (SELECT "product_id" FROM "order_items" WHERE "quantity" > $2)`,
			expectedArgs: []any{1, 5},
		},
		{
			name: "should build where clause with subquery and EXISTS operator",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereSub("", "EXISTS", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE EXISTS (SELECT "user_id" FROM "orders" WHERE orders.user_id = users.id)`,
			expectedArgs: []any{},
		},
		{
			name: "should build deeply nested subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereSub("id", "IN", func(q1 QueryBuilder) {
						q1.Select("user_id").
							From("orders").
							WhereSub("order_id", "IN", func(q2 QueryBuilder) {
								q2.Select("id").
									From("order_items").
									Where("product_id", "=", 1).
									WhereSub("item_status", "IN", func(q3 QueryBuilder) {
										q3.Select("status_id").
											From("item_statuses").
											Where("status_name", "=", "completed")
									})
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" IN (SELECT "user_id" FROM "orders" WHERE "order_id" IN (SELECT "id" FROM "order_items" WHERE "product_id" = $1 AND "item_status" IN (SELECT "status_id" FROM "item_statuses" WHERE "status_name" = $2)))`,
			expectedArgs: []any{1, "completed"},
		},
		{
			name: "should return error when subquery builder is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereSub("id", "IN", nil)
			},
			expectedError: "WHERE SUB clause cannot be empty",
		},
		{
			name: "should return error when subquery is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereSub("id", "IN", func(q QueryBuilder) {
						// Empty subquery
					})
			},
			expectedError: "WHERE SUB clause cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_OrWhereSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should build OR where clause with subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereSub("id", "IN", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR "id" IN (SELECT "user_id" FROM "orders" WHERE "amount" > $2)`,
			expectedArgs: []any{"inactive", 100},
		},
		{
			name: "should build OR where clause with subquery and alias",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users u").
					Where("status", "=", "inactive").
					OrWhereSub("u.id", "IN", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" AS "u" WHERE "status" = $1 OR "u"."id" IN (SELECT "user_id" FROM "orders" WHERE "amount" > $2)`,
			expectedArgs: []any{"inactive", 100},
		},
		{
			name: "should build OR where clause with subquery and multiple conditions",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "=", 1).
					OrWhereSub("id", "NOT IN", func(q QueryBuilder) {
						q.Select("product_id").
							From("order_items").
							Where("quantity", ">", 5)
					})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" = $1 OR "id" NOT IN (SELECT "product_id" FROM "order_items" WHERE "quantity" > $2)`,
			expectedArgs: []any{1, 5},
		},
		{
			name: "should build OR where clause with subquery and EXISTS operator",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereSub("", "EXISTS", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR EXISTS (SELECT "user_id" FROM "orders" WHERE orders.user_id = users.id)`,
			expectedArgs: []any{"inactive"},
		},
		{
			name: "should treat leading OrWhereSub as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereSub("id", "IN", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" IN (SELECT "user_id" FROM "orders" WHERE "amount" > $1)`,
			expectedArgs: []any{100},
		},
		{
			name: "should build deeply nested OR subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereSub("id", "IN", func(q1 QueryBuilder) {
						q1.Select("user_id").
							From("orders").
							OrWhereSub("order_id", "IN", func(q2 QueryBuilder) {
								q2.Select("id").
									From("order_items").
									Where("product_id", "=", 1).
									OrWhereSub("item_id", "IN", func(q3 QueryBuilder) {
										q3.Select("id").
											From("inventory").
											Where("location", "=", "warehouse")
									})
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR "id" IN (SELECT "user_id" FROM "orders" WHERE "order_id" IN (SELECT "id" FROM "order_items" WHERE "product_id" = $2 OR "item_id" IN (SELECT "id" FROM "inventory" WHERE "location" = $3)))`,
			expectedArgs: []any{"inactive", 1, "warehouse"},
		},
		{
			name: "should return error when subquery builder is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereSub("id", "IN", nil)
			},
			expectedError: "WHERE SUB clause cannot be empty",
		},
		{
			name: "should return error when subquery is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereSub("id", "IN", func(q QueryBuilder) {
						// Empty subquery
					})
			},
			expectedError: "WHERE SUB clause cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				limit:   -1,
				offset:  -1,
			}
			tt.build(b)

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_Select_Where_Combined(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		table         string
		wheres        []where
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name:  "should combine basic, between, in, null, raw, sub query, and nested where conditions",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryBetween, column: "price", operator: "BETWEEN", args: []any{100, 500}},
				{conj: "OR", queryType: QueryBasic, column: "status", operator: "IN", args: []any{[]any{"available", "backorder"}}},
				{conj: "AND", queryType: QueryNull, column: "description", operator: "IS NOT NULL", args: []any{}},
				{conj: "AND", queryType: QueryRaw, expr: "stock > ? AND warehouse_id = ?", args: []any{10, 5}},
				{conj: "OR", queryType: QueryNested, nested: []where{
					{queryType: QueryBasic, column: "manufacturer", operator: "=", args: []any{"Apple"}},
					{conj: "AND", queryType: QueryBasic, column: "warranty_years", operator: ">", args: []any{1}},
				}},
				{conj: "AND", queryType: QuerySub, column: "id", operator: "NOT IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "discontinued_products",
					columns: []column{{queryType: QueryBasic, name: "product_id"}},
					limit:   -1,
					offset:  -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND "price" BETWEEN $2 AND $3 OR "status" IN ($4, $5) AND "description" IS NOT NULL AND stock > $6 AND warehouse_id = $7 OR ("manufacturer" = $8 AND "warranty_years" > $9) AND "id" NOT IN (SELECT "product_id" FROM "discontinued_products")`,
			expectedArgs: []any{"electronics", 100, 500, "available", "backorder", 10, 5, "Apple", 1},
		},
		{
			name:  "should handle complex query with deep nesting, multiple subqueries, and all operators",
			table: "orders",
			wheres: []where{
				// top-level simple where
				{conj: "AND", queryType: QueryBasic, column: "customer_id", operator: "=", args: []any{123}},
				{conj: "AND", queryType: QueryBasic, column: "region", operator: "=", args: []any{"EU"}},

				// first-level nested group
				{conj: "AND", queryType: QueryNested, nested: []where{
					// IN condition
					{queryType: QueryBasic, column: "status", operator: "IN", args: []any{[]any{"completed", "shipped"}}},
					// OR BETWEEN condition
					{conj: "OR", queryType: QueryBetween, column: "order_date", operator: "BETWEEN", args: []any{"2023-01-01", "2023-06-30"}},

					// second-level nested group
					{conj: "AND", queryType: QueryNested, nested: []where{
						// RAW expression
						{queryType: QueryRaw, expr: "total_amount > ? AND currency = ?", args: []any{500, "USD"}},
						{conj: "OR", queryType: QueryNull, column: "tracking_number", operator: "IS NULL"},
						{conj: "OR", queryType: QueryNull, column: "updated_at", operator: "IS NOT NULL"},
						// subquery inside second-level group
						{conj: "AND", queryType: QuerySub, column: "id", operator: "IN", sub: &builder{
							dialect: PostgresDialect{},
							action:  "select",
							table:   "priority_orders",
							columns: []column{{queryType: QueryBasic, name: "order_id"}},
							wheres: []where{
								{queryType: QueryBasic, column: "priority_level", operator: "=", args: []any{"high"}},
								{conj: "AND", queryType: QueryBetween, column: "created_at", operator: "BETWEEN", args: []any{"2023-01-01", "2023-12-31"}},
							},
							orderBys: []orderBy{{queryType: QueryBasic, column: "created_at", dir: "DESC"}},
							limit:    10,
							offset:   5,
						}},
					}},
				}},

				// top-level NOT IN subquery
				{conj: "AND", queryType: QuerySub, column: "id", operator: "NOT IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "refunded_orders",
					columns: []column{{queryType: QueryBasic, name: "order_id"}},
					wheres: []where{
						{queryType: QueryBasic, column: "refund_date", operator: ">", args: []any{"2023-01-01"}},
						{conj: "AND", queryType: QueryBasic, column: "reason", operator: "=", args: []any{"fraud"}},
						{conj: "OR", queryType: QueryBasic, column: "store_id", operator: "IN", args: []any{[]any{11, 12, 13}}},
					},
					limit:  20,
					offset: 0,
				}},
			},
			expectedSQL: `SELECT * FROM "orders" WHERE "customer_id" = $1 AND "region" = $2 AND ("status" IN ($3, $4) OR "order_date" BETWEEN $5 AND $6 AND (total_amount > $7 AND currency = $8 OR "tracking_number" IS NULL OR "updated_at" IS NOT NULL AND "id" IN (SELECT "order_id" FROM "priority_orders" WHERE "priority_level" = $9 AND "created_at" BETWEEN $10 AND $11 ORDER BY "created_at" DESC LIMIT 10 OFFSET 5))) AND "id" NOT IN (SELECT "order_id" FROM "refunded_orders" WHERE "refund_date" > $12 AND "reason" = $13 OR "store_id" IN ($14, $15, $16) LIMIT 20 OFFSET 0)`,
			expectedArgs: []any{
				123, "EU", // $1, $2
				"completed", "shipped", // $3, $4
				"2023-01-01", "2023-06-30", // $5, $6
				500, "USD", // $7, $8
				"high",                     // $9
				"2023-01-01", "2023-12-31", // $10, $11
				"2023-01-01", "fraud", // $12, $13
				11, 12, 13, // $14, $15, $16
			},
		},
		{
			name:  "should return error when nested where clause compilation fails",
			table: "orders",
			wheres: []where{
				{queryType: QueryNested, nested: []where{
					{queryType: QuerySub, column: "id", operator: "IN", sub: &builder{
						// no dialect — will cause ToSQL() to fail
						action:  "select",
						table:   "broken_table",
						columns: []column{{queryType: QueryBasic, name: "some_col"}},
						limit:   -1,
						offset:  -1,
					}},
				}},
			},
			expectedError: "no dialect specified", // matches builder.ToSQL() error
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
				limit:   -1,
				offset:  -1,
			}

			// Act
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_Select_OrderBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		table         string
		orderBys      []orderBy
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name:  "should build select with single basic order by",
			table: "users",
			orderBys: []orderBy{
				{queryType: QueryBasic, column: "id", dir: "ASC"},
			},
			expectedSQL:  `SELECT * FROM "users" ORDER BY "id" ASC`,
			expectedArgs: []any{},
		},
		{
			name:  "should build select with multiple basic order by",
			table: "users",
			orderBys: []orderBy{
				{queryType: QueryBasic, column: "name", dir: "DESC"},
				{queryType: QueryBasic, column: "created_at", dir: "ASC"},
			},
			expectedSQL:  `SELECT * FROM "users" ORDER BY "name" DESC, "created_at" ASC`,
			expectedArgs: []any{},
		},
		{
			name:  "should build select with raw order by",
			table: "products",
			orderBys: []orderBy{
				{queryType: QueryRaw, expr: "LENGTH(name) DESC"},
			},
			expectedSQL:  `SELECT * FROM "products" ORDER BY LENGTH(name) DESC`,
			expectedArgs: []any{},
		},
		{
			name:  "should build select with raw order by with args",
			table: "products",
			orderBys: []orderBy{
				{queryType: QueryRaw, expr: "CASE WHEN price > ? THEN 1 ELSE 0 END DESC", args: []any{100}},
			},
			expectedSQL:  `SELECT * FROM "products" ORDER BY CASE WHEN price > $1 THEN 1 ELSE 0 END DESC`,
			expectedArgs: []any{100},
		},
		{
			name:  "should build select with mixed order by",
			table: "orders",
			orderBys: []orderBy{
				{queryType: QueryBasic, column: "status", dir: "ASC"},
				{queryType: QueryRaw, expr: "CASE WHEN amount > ? THEN 1 ELSE 0 END DESC", args: []any{100}},
			},
			expectedSQL:  `SELECT * FROM "orders" ORDER BY "status" ASC, CASE WHEN amount > $1 THEN 1 ELSE 0 END DESC`,
			expectedArgs: []any{100},
		},
		{
			name:         "should not add order by clause if empty",
			table:        "items",
			orderBys:     []orderBy{},
			expectedSQL:  `SELECT * FROM "items"`,
			expectedArgs: []any{},
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
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

func TestPostgresDialect_CompileSelect_Select_LimitOffset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		table         string
		limit         int
		offset        int
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name:         "should build select with limit",
			table:        "users",
			limit:        10,
			offset:       -1, // default
			expectedSQL:  `SELECT * FROM "users" LIMIT 10`,
			expectedArgs: []any{},
		},
		{
			name:         "should build select with offset",
			table:        "users",
			limit:        -1, // default
			offset:       5,
			expectedSQL:  `SELECT * FROM "users" OFFSET 5`,
			expectedArgs: []any{},
		},
		{
			name:         "should build select with limit and offset",
			table:        "users",
			limit:        10,
			offset:       5,
			expectedSQL:  `SELECT * FROM "users" LIMIT 10 OFFSET 5`,
			expectedArgs: []any{},
		},
		{
			name:         "should ignore negative limit and offset",
			table:        "users",
			limit:        -10,
			offset:       -5,
			expectedSQL:  `SELECT * FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name:         "should handle zero limit and offset",
			table:        "users",
			limit:        0,
			offset:       0,
			expectedSQL:  `SELECT * FROM "users" LIMIT 0 OFFSET 0`,
			expectedArgs: []any{},
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
			sql, args, err := b.dialect.CompileSelect(b)

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
			assert.Equal(t, tt.expectedArgs, args, "expected args to match output")
		})
	}
}

// -----------------
// --- BENCHMARK ---
// -----------------

func BenchmarkPostgresDialect_Capabilities(b *testing.B) {
	d := PostgresDialect{}

	for b.Loop() {
		d.Capabilities()
	}
}

func BenchmarkPostgresDialect_Placeholder(b *testing.B) {
	d := PostgresDialect{}
	n := 10

	for b.Loop() {
		d.Placeholder(n)
	}
}

func BenchmarkPostgresDialect_WrapColumn(b *testing.B) {
	d := PostgresDialect{}
	column := "users.id AS user_id"

	for b.Loop() {
		d.WrapColumn(column)
	}
}

func BenchmarkPostgresDialect_WrapIdentifier(b *testing.B) {
	d := PostgresDialect{}
	identifier := "public.users.id"

	for b.Loop() {
		d.WrapIdentifier(identifier)
	}
}

func BenchmarkPostgresDialect_WrapTable(b *testing.B) {
	d := PostgresDialect{}
	table := "users u"

	for b.Loop() {
		d.WrapTable(table)
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_Simple(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "users",
		columns: []column{
			{queryType: QueryBasic, name: "id"},
			{queryType: QueryBasic, name: "name"},
			{queryType: QueryBasic, name: "email"},
			{queryType: QueryRaw, expr: "MAX(created_at) AS last_created"},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_Where_Simple(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "users",
		wheres: []where{
			{conj: "AND", queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
			{conj: "AND", queryType: QueryBasic, column: "name", operator: "LIKE", args: []any{"%John%"}},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}

func BenchmarkPostgresDialect_WhereBetween(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereBetween simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("products").
					WhereBetween("price", 100, 200)
			},
		},
		{
			name: "OrWhereBetween simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("products").
					Where("category_id", "=", 10).
					OrWhereBetween("quantity", 1, 5)
			},
		},
		{
			name: "Multiple WhereBetween",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("orders").
					WhereBetween("amount", 100, 500).
					WhereBetween("discount", 5, 15).
					OrWhereBetween("created_at", "2023-01-01", "2023-12-31")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}
				bm.build(bd)
				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_WhereNotBetween(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereNotBetween simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("products").
					WhereNotBetween("price", 100, 200)
			},
		},
		{
			name: "OrWhereNotBetween simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("products").
					Where("category_id", "=", 10).
					OrWhereNotBetween("quantity", 1, 5)
			},
		},
		{
			name: "Multiple WhereNotBetween",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("orders").
					WhereNotBetween("amount", 100, 500).
					WhereNotBetween("discount", 5, 15).
					OrWhereNotBetween("created_at", "2023-01-01", "2023-12-31")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}
				bm.build(bd)
				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_WhereIn(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereIn single value",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("customers").
					WhereIn("id", 1)
			},
		},
		{
			name: "WhereIn multiple values",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("orders").
					WhereIn("id", 101, 102, 103)
			},
		},
		{
			name: "OrWhereIn multiple values",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("products").
					Where("category_id", "=", 1).
					OrWhereIn("unit_id", 1, 2)
			},
		},
		{
			name: "WhereIn from slice",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("products").
					WhereIn("category_id", []int{11, 12, 13})
			},
		},
		{
			name: "WhereIn empty slice",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("shipments").
					WhereIn("tracking_number", []any{})
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}
				bm.build(bd)
				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_WhereNotIn(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereNotIn single value",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("customers").
					WhereNotIn("id", 1)
			},
		},
		{
			name: "WhereNotIn multiple values",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("orders").
					WhereNotIn("id", 101, 102, 103)
			},
		},
		{
			name: "OrWhereNotIn multiple values",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("products").
					Where("category_id", "=", 1).
					OrWhereNotIn("unit_id", 1, 2)
			},
		},
		{
			name: "WhereNotIn from slice",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("products").
					WhereNotIn("category_id", []int{11, 12, 13})
			},
		},
		{
			name: "WhereNotIn empty slice",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("shipments").
					WhereNotIn("tracking_number", []any{})
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}
				bm.build(bd)
				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_WhereNull(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereNull simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereNull("email")
			},
		},
		{
			name: "OrWhereNull simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					Where("status", "=", "pending").
					OrWhereNull("email")
			},
		},
		{
			name: "Multiple WhereNull",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereNull("email").
					WhereNull("phone")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(bd)

				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_WhereNotNull(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereNotNull simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereNotNull("email")
			},
		},
		{
			name: "OrWhereNotNull simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					Where("status", "=", "pending").
					OrWhereNotNull("email")
			},
		},
		{
			name: "Multiple WhereNotNull",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereNotNull("email").
					WhereNotNull("phone")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(bd)

				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_WhereRaw(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereRaw simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereRaw("id = 1")
			},
		},
		{
			name: "WhereRaw with args",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereRaw("name = ?", "John Doe")
			},
		},
		{
			name: "OrWhereRaw with args",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereRaw("age BETWEEN ? AND ?", 20, 30)
			},
		},
		{
			name: "Multiple WhereRaw",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereRaw("status = 'active'").
					WhereRaw("created_at > ?", "2023-01-01")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(bd)

				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_WhereGroup(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereGroup simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18).
							OrWhere("age", "<", 65)
					})
			},
		},
		{
			name: "OrWhereGroup simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					Where("status", "=", "active").
					OrWhereGroup(func(q QueryBuilder) {
						q.Where("age", ">", 18).
							Where("age", "<", 65)
					})
			},
		},
		{
			name: "WhereGroup deep nesting",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereGroup(func(q1 QueryBuilder) {
						q1.Where("level1", "=", 1).
							OrWhereGroup(func(q2 QueryBuilder) {
								q2.Where("level2", "=", 2).
									WhereGroup(func(q3 QueryBuilder) {
										q3.Where("level3", "=", 3)
									})
							})
					})
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(bd)

				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_WhereSub(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereSub simple IN",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereSub("id", "IN", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							Where("amount", ">", 100)
					})
			},
		},
		{
			name: "OrWhereSub simple EXISTS",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereSub("", "EXISTS", func(q QueryBuilder) {
						q.Select("user_id").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
		},
		{
			name: "WhereSub nested",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereSub("id", "IN", func(q1 QueryBuilder) {
						q1.Select("user_id").
							From("orders").
							WhereSub("order_id", "IN", func(q2 QueryBuilder) {
								q2.Select("id").
									From("order_items").
									Where("product_id", "=", 1)
							})
					})
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				bd := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(bd)

				_, _, _ = bd.dialect.CompileSelect(bd)
			}
		})
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_Where_Combined(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "products",
		wheres: []where{
			{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
			{conj: "AND", queryType: QueryBetween, column: "price", operator: "BETWEEN", args: []any{100, 500}},
			{conj: "OR", queryType: QueryBasic, column: "status", operator: "IN", args: []any{[]any{"available", "backorder"}}},
			{conj: "AND", queryType: QueryNull, column: "description", operator: "IS NOT NULL", args: []any{}},
			{conj: "AND", queryType: QueryRaw, expr: "stock > ? AND warehouse_id = ?", args: []any{10, 5}},
			{conj: "OR", queryType: QueryNested, nested: []where{
				{queryType: QueryBasic, column: "manufacturer", operator: "=", args: []any{"Apple"}},
				{conj: "AND", queryType: QueryBasic, column: "warranty_years", operator: ">", args: []any{1}},
				{conj: "OR", queryType: QueryNested, nested: []where{
					{queryType: QueryBasic, column: "rating", operator: ">=", args: []any{4}},
					{conj: "AND", queryType: QueryBasic, column: "reviews_count", operator: ">", args: []any{50}},
				}},
			}},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}

func BenchmarkPostgresDialect_CompileSelect_Where_Complex(b *testing.B) {
	d := PostgresDialect{}

	// Build a very complex query once, reuse for each benchmark iteration
	subInner := &builder{
		dialect: d,
		action:  "select",
		table:   "priority_orders",
		columns: []column{{queryType: QueryBasic, name: "order_id"}},
		wheres: []where{
			{queryType: QueryBasic, column: "priority_level", operator: "=", args: []any{"high"}},
			{conj: "AND", queryType: QueryBetween, column: "created_at", operator: "BETWEEN", args: []any{"2023-01-01", "2023-12-31"}},
		},
		orderBys: []orderBy{{queryType: QueryBasic, column: "created_at", dir: "DESC"}},
		limit:    10,
		offset:   5,
	}

	subOuter := &builder{
		dialect: d,
		action:  "select",
		table:   "refunded_orders",
		columns: []column{{queryType: QueryBasic, name: "order_id"}},
		wheres: []where{
			{queryType: QueryBasic, column: "refund_date", operator: ">", args: []any{"2023-01-01"}},
			{conj: "AND", queryType: QueryBasic, column: "reason", operator: "=", args: []any{"fraud"}},
			{conj: "OR", queryType: QueryBasic, column: "store_id", operator: "IN", args: []any{[]any{11, 12, 13}}},
		},
		limit:  20,
		offset: 0,
	}

	mainBuilder := &builder{
		dialect: d,
		action:  "select",
		table:   "orders",
		wheres: []where{
			{queryType: QueryBasic, column: "customer_id", operator: "=", args: []any{123}},
			{conj: "AND", queryType: QueryBasic, column: "region", operator: "=", args: []any{"EU"}},
			{conj: "AND", queryType: QueryNested, nested: []where{
				// IN condition
				{queryType: QueryBasic, column: "status", operator: "IN", args: []any{[]any{"completed", "shipped"}}},
				// OR BETWEEN condition
				{conj: "OR", queryType: QueryBetween, column: "order_date", operator: "BETWEEN", args: []any{"2023-01-01", "2023-06-30"}},

				// second-level nested group
				{conj: "AND", queryType: QueryNested, nested: []where{
					// RAW expression
					{queryType: QueryRaw, expr: "total_amount > ? AND currency = ?", args: []any{500, "USD"}},
					{conj: "OR", queryType: QueryNull, column: "tracking_number", operator: "IS NULL"},
					{conj: "OR", queryType: QueryNull, column: "updated_at", operator: "IS NOT NULL"},
					// subquery inside second-level group
					{conj: "AND", queryType: QuerySub, column: "id", operator: "IN", sub: subInner},
				}},
			}},

			// top-level NOT IN subquery
			{conj: "AND", queryType: QuerySub, column: "id", operator: "NOT IN", sub: subOuter},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, err := d.CompileSelect(mainBuilder)
		if err != nil {
			b.Fatalf("CompileSelect failed: %v", err)
		}
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_OrderBy(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "users",
		orderBys: []orderBy{
			{queryType: QueryBasic, column: "name", dir: "DESC"},
			{queryType: QueryBasic, column: "created_at", dir: "ASC"},
			{queryType: QueryRaw, expr: "LENGTH(email) ASC"},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_LimitOffset(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "users",
		limit:   100,
		offset:  50,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}
