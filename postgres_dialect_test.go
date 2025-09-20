package sequel

import (
	"fmt"
	"strings"
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
			assert.Equal(t, tt.expected, result, "expected quoted column with alias to match")
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

func TestPostgresDialect_CompileSelect_Select_Simple(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		table         string
		columns       []column
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name:         "should build select all query when columns are empty",
			table:        "users",
			columns:      []column{},
			expectedSQL:  `SELECT * FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name:         "should build select with single basic column",
			table:        "users",
			columns:      []column{{queryType: QueryBasic, name: "id"}},
			expectedSQL:  `SELECT "id" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name:  "should build select with multiple basic columns",
			table: "users",
			columns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
				{queryType: QueryBasic, name: "email"},
			},
			expectedSQL:  `SELECT "id", "name", "email" FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name:  "should build select with table alias and qualified columns",
			table: "users u",
			columns: []column{
				{queryType: QueryBasic, name: "u.id"},
				{queryType: QueryBasic, name: "u.name"},
			},
			expectedSQL:  `SELECT "u"."id", "u"."name" FROM "users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name:  "should build select with raw column expression",
			table: "products",
			columns: []column{
				{queryType: QueryRaw, expr: "COUNT(*) AS total_products"},
			},
			expectedSQL:  `SELECT COUNT(*) AS total_products FROM "products"`,
			expectedArgs: []any{},
		},
		{
			name:  "should build select with raw column expression and arguments",
			table: "logs",
			columns: []column{
				{queryType: QueryRaw, expr: "DATE(timestamp) AS log_date", args: []any{}},
			},
			expectedSQL:  `SELECT DATE(timestamp) AS log_date FROM "logs"`,
			expectedArgs: []any{},
		},
		{
			name:  "should build select with multiple raw columns",
			table: "orders",
			columns: []column{
				{queryType: QueryRaw, expr: "id"},
				{queryType: QueryRaw, expr: "user_id"},
				{queryType: QueryRaw, expr: "total_amount"},
			},
			expectedSQL:  `SELECT id, user_id, total_amount FROM "orders"`,
			expectedArgs: []any{},
		},
		{
			name:          "should return error when table is empty",
			table:         "",
			columns:       []column{},
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
				action:  "select",
				table:   tt.table,
				columns: tt.columns,
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

func TestPostgresDialect_CompileSelect_Select_Where_Between(t *testing.T) {
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
			name:  "should handle where conditions with BETWEEN operator",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBetween, column: "price", operator: "BETWEEN", args: []any{10, 100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" BETWEEN $1 AND $2`,
			expectedArgs: []any{10, 100},
		},
		{
			name:  "should handle where conditions with NOT BETWEEN operator",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBetween, column: "price", operator: "NOT BETWEEN", args: []any{10, 100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" NOT BETWEEN $1 AND $2`,
			expectedArgs: []any{10, 100},
		},
		{
			name:  "should handle BETWEEN after previous WHERE condition",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryBetween, column: "price", operator: "BETWEEN", args: []any{10, 100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND "price" BETWEEN $2 AND $3`,
			expectedArgs: []any{"electronics", 10, 100},
		},
		{
			name:  "should handle NOT BETWEEN after previous WHERE condition",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryBetween, column: "price", operator: "NOT BETWEEN", args: []any{10, 100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND "price" NOT BETWEEN $2 AND $3`,
			expectedArgs: []any{"electronics", 10, 100},
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

func TestPostgresDialect_CompileSelect_Select_Where_In(t *testing.T) {
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
			name:  "should handle where conditions with IN operator",
			table: "orders",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "status", operator: "IN", args: []any{[]any{"pending", "processing"}}},
			},
			expectedSQL:  `SELECT * FROM "orders" WHERE "status" IN ($1, $2)`,
			expectedArgs: []any{"pending", "processing"},
		},
		{
			name:  "should handle where conditions with NOT IN operator",
			table: "orders",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "status", operator: "NOT IN", args: []any{[]any{"cancelled", "failed"}}},
			},
			expectedSQL:  `SELECT * FROM "orders" WHERE "status" NOT IN ($1, $2)`,
			expectedArgs: []any{"cancelled", "failed"},
		},
		{
			name:  "should handle IN after previous WHERE condition",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryBasic, column: "id", operator: "IN", args: []any{[]any{1, 2, 3}}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND "id" IN ($2, $3, $4)`,
			expectedArgs: []any{"electronics", 1, 2, 3},
		},
		{
			name:  "should handle NOT IN after previous WHERE condition",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryBasic, column: "id", operator: "NOT IN", args: []any{[]any{4, 5, 6}}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND "id" NOT IN ($2, $3, $4)`,
			expectedArgs: []any{"electronics", 4, 5, 6},
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

func TestPostgresDialect_CompileSelect_Select_Where_Null(t *testing.T) {
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
			name:  "should handle where conditions with IS NULL operator",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QueryNull, column: "email", operator: "IS NULL", args: []any{}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NULL`,
			expectedArgs: []any{},
		},
		{
			name:  "should handle where conditions with IS NOT NULL operator",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QueryNull, column: "email", operator: "IS NOT NULL", args: []any{}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NOT NULL`,
			expectedArgs: []any{},
		},
		{
			name:  "should handle IS NULL after previous WHERE condition",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryNull, column: "description", operator: "IS NULL", args: []any{}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND "description" IS NULL`,
			expectedArgs: []any{"electronics"},
		},
		{
			name:  "should handle IS NOT NULL after previous WHERE condition",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryNull, column: "description", operator: "IS NOT NULL", args: []any{}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND "description" IS NOT NULL`,
			expectedArgs: []any{"electronics"},
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

func TestPostgresDialect_CompileSelect_Select_Where_Raw(t *testing.T) {
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
			name:  "should handle raw where conditions",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryRaw, expr: "price > ? AND stock < ?", args: []any{50, 100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE price > $1 AND stock < $2`,
			expectedArgs: []any{50, 100},
		},
		{
			name:  "should handle raw AND after previous WHERE condition",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryRaw, expr: "price > ? AND stock < ?", args: []any{50, 100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND price > $2 AND stock < $3`,
			expectedArgs: []any{"electronics", 50, 100},
		},
		{
			name:  "should handle raw OR after previous WHERE condition",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "OR", queryType: QueryRaw, expr: "price > ? OR stock < ?", args: []any{50, 100}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 OR price > $2 OR stock < $3`,
			expectedArgs: []any{"electronics", 50, 100},
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

func TestPostgresDialect_CompileSelect_Select_Where_Group(t *testing.T) {
	t.Parallel()

	var generateDeepNestedWhere func(level int, startValue int) where
	generateDeepNestedWhere = func(level int, startValue int) where {
		if level == 1 {
			return where{queryType: QueryBasic, column: fmt.Sprintf("col_%d", level), operator: "=", args: []any{startValue}}
		}
		return where{
			queryType: QueryNested,
			nested: []where{
				generateDeepNestedWhere(level-1, startValue),
				{conj: "AND", queryType: QueryBasic, column: fmt.Sprintf("col_%d", level), operator: "=", args: []any{startValue + level - 1}},
			},
		}
	}

	generateExpectedSQL := func(level int) string {
		sql := ""
		for i := 1; i < level; i++ {
			sql += "("
		}
		sql += `"col_1" = $1`
		for i := 2; i <= level; i++ {
			sql += fmt.Sprintf(` AND "col_%d" = $%d)`, i, i)
		}
		return fmt.Sprintf(`SELECT * FROM "items" WHERE %s`, sql)
	}

	generateExpectedArgs := func(level int) []any {
		args := make([]any, level)
		for i := 1; i <= level; i++ {
			args[i-1] = i
		}
		return args
	}

	// For 15-level mix of IN and BETWEEN
	var generateDeepNestedWhereMix func(level int) where
	generateDeepNestedWhereMix = func(level int) where {
		if level == 1 {
			return where{queryType: QueryBasic, column: "base", operator: "=", args: []any{1}}
		}
		return where{
			queryType: QueryNested,
			nested: []where{
				generateDeepNestedWhereMix(level - 1), // now it works
				{conj: "OR", queryType: QueryBetween, column: fmt.Sprintf("range_%d", level), operator: "BETWEEN", args: []any{level * 10, level*10 + 5}},
			},
		}
	}

	generateExpectedSQLMix := func(level int) string {
		var sb strings.Builder
		placeholder := 1

		// Open parentheses for each nesting level
		for i := 0; i < level-1; i++ {
			sb.WriteString("(")
		}

		// Base condition
		sb.WriteString(fmt.Sprintf(`"base" = $%d`, placeholder))
		placeholder++

		// Each deeper level adds OR + BETWEEN condition and closes one parenthesis
		for i := 2; i <= level; i++ {
			sb.WriteString(fmt.Sprintf(` OR "range_%d" BETWEEN $%d AND $%d)`, i, placeholder, placeholder+1))
			placeholder += 2
		}

		return fmt.Sprintf(`SELECT * FROM "inventory" WHERE %s`, sb.String())
	}

	generateExpectedArgsMix := func(level int) []any {
		args := []any{1} // base arg
		for i := 2; i <= level; i++ {
			start := i * 10
			args = append(args, start, start+5)
		}
		return args
	}

	tests := []struct {
		name          string
		table         string
		wheres        []where
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name:  "should handle nested where group (simple)",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
				{conj: "AND", queryType: QueryNested, nested: []where{
					{queryType: QueryBasic, column: "name", operator: "=", args: []any{"John"}},
					{conj: "OR", queryType: QueryBasic, column: "age", operator: ">", args: []any{25}},
				}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 AND ("name" = $2 OR "age" > $3)`,
			expectedArgs: []any{1, "John", 25},
		},
		{
			name:  "should handle nested where group with IN operator",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "category", operator: "=", args: []any{"electronics"}},
				{conj: "AND", queryType: QueryNested, nested: []where{
					{queryType: QueryBasic, column: "id", operator: "IN", args: []any{[]any{1, 2, 3}}},
					{conj: "OR", queryType: QueryBasic, column: "price", operator: "<", args: []any{100}},
				}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category" = $1 AND ("id" IN ($2, $3, $4) OR "price" < $5)`,
			expectedArgs: []any{"electronics", 1, 2, 3, 100},
		},
		{
			name:  "should handle deeply nested where group",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
				{conj: "AND", queryType: QueryNested, nested: []where{
					{queryType: QueryBasic, column: "name", operator: "=", args: []any{"John"}},
					{conj: "OR", queryType: QueryNested, nested: []where{
						{queryType: QueryBasic, column: "age", operator: ">", args: []any{25}},
						{conj: "AND", queryType: QueryBasic, column: "status", operator: "=", args: []any{"active"}},
					}},
				}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "id" = $1 AND ("name" = $2 OR ("age" > $3 AND "status" = $4))`,
			expectedArgs: []any{1, "John", 25, "active"},
		},
		{
			name:  "should handle 5-level nested where group",
			table: "orders",
			wheres: []where{
				{queryType: QueryNested, nested: []where{
					{queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
					{conj: "AND", queryType: QueryNested, nested: []where{
						{queryType: QueryBasic, column: "status", operator: "=", args: []any{"open"}},
						{conj: "OR", queryType: QueryNested, nested: []where{
							{queryType: QueryBasic, column: "price", operator: ">", args: []any{50}},
							{conj: "AND", queryType: QueryNested, nested: []where{
								{queryType: QueryBasic, column: "qty", operator: ">", args: []any{10}},
								{conj: "OR", queryType: QueryBasic, column: "qty", operator: "<", args: []any{2}},
							}},
						}},
					}},
				}},
			},
			expectedSQL:  `SELECT * FROM "orders" WHERE ("id" = $1 AND ("status" = $2 OR ("price" > $3 AND ("qty" > $4 OR "qty" < $5))))`,
			expectedArgs: []any{1, "open", 50, 10, 2},
		},
		{
			name:  "should handle 10-level nested where group (stress test)",
			table: "items",
			wheres: []where{
				generateDeepNestedWhere(10, 1), // helper function below
			},
			expectedSQL:  generateExpectedSQL(10),  // helper below
			expectedArgs: generateExpectedArgs(10), // helper below
		},
		{
			name:  "should handle 15-level nested where group with BETWEEN and IN (stress test)",
			table: "inventory",
			wheres: []where{
				generateDeepNestedWhereMix(15),
			},
			expectedSQL:  generateExpectedSQLMix(15),
			expectedArgs: generateExpectedArgsMix(15),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &builder{
				dialect: PostgresDialect{},
				action:  "select",
				table:   tt.table,
				wheres:  tt.wheres,
				limit:   -1,
				offset:  -1,
			}

			sql, args, err := b.dialect.CompileSelect(b)

			if tt.expectedError != "" {
				assert.Error(t, err, "expected an error")
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Empty(t, sql)
				assert.Empty(t, args)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSQL, sql)
			assert.Equal(t, tt.expectedArgs, args)
		})
	}
}

func TestPostgresDialect_CompileSelect_Where_Sub(t *testing.T) {
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
			name:  "should handle where conditions with subquery (IN)",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "id", operator: "IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "active_users",
					columns: []column{{queryType: QueryBasic, name: "user_id"}},
					limit:   -1,
					offset:  -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" IN (SELECT "user_id" FROM "active_users")`,
			expectedArgs: []any{},
		},
		{
			name:  "should handle where conditions with subquery (NOT IN)",
			table: "products",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "id", operator: "NOT IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "discontinued_products",
					columns: []column{{queryType: QueryBasic, name: "product_id"}},
					limit:   -1,
					offset:  -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "id" NOT IN (SELECT "product_id" FROM "discontinued_products")`,
			expectedArgs: []any{},
		},
		{
			name:  "should handle where conditions with subquery (EXISTS)",
			table: "orders",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "", operator: "EXISTS", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "order_items",
					columns: []column{{queryType: QueryRaw, expr: "1"}},
					limit:   -1,
					offset:  -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "orders" WHERE EXISTS (SELECT 1 FROM "order_items")`,
			expectedArgs: []any{},
		},
		{
			name:  "should handle where conditions with subquery (NOT EXISTS)",
			table: "customers",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "", operator: "NOT EXISTS", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "inactive_accounts",
					columns: []column{{queryType: QueryRaw, expr: "1"}},
					limit:   -1,
					offset:  -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "customers" WHERE NOT EXISTS (SELECT 1 FROM "inactive_accounts")`,
			expectedArgs: []any{},
		},
		{
			name:  "should handle subquery with arguments",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "id", operator: "IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "posts",
					columns: []column{{queryType: QueryBasic, name: "author_id"}},
					wheres: []where{
						{queryType: QueryBasic, column: "status", operator: "=", args: []any{"published"}},
						{conj: "AND", queryType: QueryBasic, column: "views", operator: ">", args: []any{100}},
					},
					limit:  -1,
					offset: -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" IN (SELECT "author_id" FROM "posts" WHERE "status" = $1 AND "views" > $2)`,
			expectedArgs: []any{"published", 100},
		},
		{
			name:  "should handle subquery with arguments and limit - offset",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "id", operator: "IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "posts",
					columns: []column{{queryType: QueryBasic, name: "author_id"}},
					wheres: []where{
						{queryType: QueryBasic, column: "status", operator: "=", args: []any{"published"}},
					},
					limit:  5,
					offset: 10,
				}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" IN (SELECT "author_id" FROM "posts" WHERE "status" = $1 LIMIT 5 OFFSET 10)`,
			expectedArgs: []any{"published"},
		},
		{
			name:  "should handle subquery with multiple arguments and renumbering",
			table: "main_table",
			wheres: []where{
				{conj: "AND", queryType: QueryBasic, column: "main_id", operator: "=", args: []any{99}},
				{conj: "AND", queryType: QuerySub, column: "sub_id", operator: "IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "sub_table",
					columns: []column{{queryType: QueryBasic, name: "id"}},
					wheres: []where{
						{queryType: QueryBasic, column: "value1", operator: ">", args: []any{100}},
						{conj: "AND", queryType: QueryBasic, column: "value2", operator: "<", args: []any{200}},
					},
					limit:  -1,
					offset: -1,
				}},
				{conj: "AND", queryType: QueryBasic, column: "another_col", operator: "=", args: []any{"test"}},
			},
			expectedSQL:  `SELECT * FROM "main_table" WHERE "main_id" = $1 AND "sub_id" IN (SELECT "id" FROM "sub_table" WHERE "value1" > $2 AND "value2" < $3) AND "another_col" = $4`,
			expectedArgs: []any{99, 100, 200, "test"},
		},
		{
			name:  "should handle subquery with no arguments",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "id", operator: "IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "active_users",
					columns: []column{{queryType: QueryBasic, name: "user_id"}},
					limit:   -1,
					offset:  -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" IN (SELECT "user_id" FROM "active_users")`,
			expectedArgs: []any{},
		},
		{
			name:  "should handle subquery with custom operator and no column",
			table: "logs",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "", operator: "SOME", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "log_entries",
					columns: []column{{queryType: QueryBasic, name: "id"}},
					limit:   -1,
					offset:  -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "logs" WHERE SOME (SELECT "id" FROM "log_entries")`,
			expectedArgs: []any{},
		},
		{
			name:  "should handle subquery with raw column expression and args",
			table: "metrics",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "metric_id", operator: "IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "measurements",
					columns: []column{{queryType: QueryRaw, expr: "MAX(value)", args: []any{}}},
					wheres: []where{
						{queryType: QueryRaw, expr: "created_at > ?", args: []any{"2025-01-01"}},
					},
					limit:  -1,
					offset: -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "metrics" WHERE "metric_id" IN (SELECT MAX(value) FROM "measurements" WHERE created_at > $1)`,
			expectedArgs: []any{"2025-01-01"},
		},
		{
			name:  "should handle nested subquery inside subquery",
			table: "users",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "id", operator: "IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "teams",
					columns: []column{{queryType: QueryBasic, name: "user_id"}},
					wheres: []where{
						{queryType: QuerySub, column: "team_id", operator: "IN", sub: &builder{
							dialect: PostgresDialect{},
							action:  "select",
							table:   "departments",
							columns: []column{{queryType: QueryBasic, name: "team_id"}},
							wheres: []where{
								{queryType: QueryBasic, column: "active", operator: "=", args: []any{true}},
							},
							limit:  -1,
							offset: -1,
						}},
					},
					limit:  -1,
					offset: -1,
				}},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" IN (SELECT "user_id" FROM "teams" WHERE "team_id" IN (SELECT "team_id" FROM "departments" WHERE "active" = $1))`,
			expectedArgs: []any{true},
		},
		{
			name:  "should handle five-level deep nested subqueries with multiple args per level",
			table: "root_table",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "root_id", operator: "IN", sub: &builder{
					dialect: PostgresDialect{},
					action:  "select",
					table:   "level1",
					columns: []column{{queryType: QueryBasic, name: "l1_id"}},
					wheres: []where{
						{queryType: QueryBasic, column: "l1_col1", operator: "=", args: []any{"l1_val1"}},
						{conj: "AND", queryType: QueryBasic, column: "l1_col2", operator: ">", args: []any{10}},
						{conj: "AND", queryType: QueryBasic, column: "l1_col3", operator: "<", args: []any{20}},
						{queryType: QuerySub, column: "l1_id", operator: "IN", sub: &builder{
							dialect: PostgresDialect{},
							action:  "select",
							table:   "level2",
							columns: []column{{queryType: QueryBasic, name: "l2_id"}},
							wheres: []where{
								{queryType: QueryBasic, column: "l2_col1", operator: "=", args: []any{"l2_val1"}},
								{conj: "AND", queryType: QueryBasic, column: "l2_col2", operator: ">", args: []any{30}},
								{conj: "AND", queryType: QueryBasic, column: "l2_col3", operator: "<", args: []any{40}},
								{queryType: QuerySub, column: "l2_id", operator: "IN", sub: &builder{
									dialect: PostgresDialect{},
									action:  "select",
									table:   "level3",
									columns: []column{{queryType: QueryBasic, name: "l3_id"}},
									wheres: []where{
										{queryType: QueryBasic, column: "l3_col1", operator: "=", args: []any{"l3_val1"}},
										{conj: "AND", queryType: QueryBasic, column: "l3_col2", operator: ">", args: []any{50}},
										{conj: "AND", queryType: QueryBasic, column: "l3_col3", operator: "<", args: []any{60}},
										{queryType: QuerySub, column: "l3_id", operator: "IN", sub: &builder{
											dialect: PostgresDialect{},
											action:  "select",
											table:   "level4",
											columns: []column{{queryType: QueryBasic, name: "l4_id"}},
											wheres: []where{
												{queryType: QueryBasic, column: "l4_col1", operator: "=", args: []any{"l4_val1"}},
												{conj: "AND", queryType: QueryBasic, column: "l4_col2", operator: ">", args: []any{70}},
												{conj: "AND", queryType: QueryBasic, column: "l4_col3", operator: "<", args: []any{80}},
												{queryType: QuerySub, column: "l4_id", operator: "IN", sub: &builder{
													dialect: PostgresDialect{},
													action:  "select",
													table:   "level5",
													columns: []column{{queryType: QueryBasic, name: "l5_id"}},
													wheres: []where{
														{queryType: QueryBasic, column: "l5_col1", operator: "=", args: []any{"l5_val1"}},
														{conj: "AND", queryType: QueryBasic, column: "l5_col2", operator: ">", args: []any{90}},
														{conj: "AND", queryType: QueryBasic, column: "l5_col3", operator: "<", args: []any{100}},
													},
													limit:  -1,
													offset: -1,
												}},
											},
											limit:  -1,
											offset: -1,
										}},
									},
									limit:  -1,
									offset: -1,
								}},
							},
							limit:  -1,
							offset: -1,
						}},
					},
					limit:  -1,
					offset: -1,
				}},
			},
			expectedSQL: `SELECT * FROM "root_table" WHERE "root_id" IN (SELECT "l1_id" FROM "level1" WHERE "l1_col1" = $1 AND "l1_col2" > $2 AND "l1_col3" < $3 AND "l1_id" IN (SELECT "l2_id" FROM "level2" WHERE "l2_col1" = $4 AND "l2_col2" > $5 AND "l2_col3" < $6 AND "l2_id" IN (SELECT "l3_id" FROM "level3" WHERE "l3_col1" = $7 AND "l3_col2" > $8 AND "l3_col3" < $9 AND "l3_id" IN (SELECT "l4_id" FROM "level4" WHERE "l4_col1" = $10 AND "l4_col2" > $11 AND "l4_col3" < $12 AND "l4_id" IN (SELECT "l5_id" FROM "level5" WHERE "l5_col1" = $13 AND "l5_col2" > $14 AND "l5_col3" < $15)))))`,
			expectedArgs: []any{
				"l1_val1", 10, 20,
				"l2_val1", 30, 40,
				"l3_val1", 50, 60,
				"l4_val1", 70, 80,
				"l5_val1", 90, 100,
			},
		},
		{
			name:  "should return error when compileWhereClause fails due to invalid subquery",
			table: "orders",
			wheres: []where{
				{conj: "AND", queryType: QuerySub, column: "id", operator: "IN", sub: &builder{
					// missing dialect, will cause ToSQL() to fail
					action:  "select",
					table:   "broken_subquery",
					columns: []column{{queryType: QueryBasic, name: "col"}},
					limit:   -1,
					offset:  -1,
				}},
			},
			expectedError: "no dialect specified", // match the error returned by ToSQL()
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

func BenchmarkPostgresDialect_CompileSelect_Select_Where_Between(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "products",
		wheres: []where{
			{conj: "AND", queryType: QueryBetween, column: "price", operator: "BETWEEN", args: []any{10, 100}},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_Where_In(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "orders",
		wheres: []where{
			{conj: "AND", queryType: QueryBasic, column: "status", operator: "IN", args: []any{[]any{"pending", "processing", "completed"}}},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_Where_Null(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "users",
		wheres: []where{
			{conj: "AND", queryType: QueryNull, column: "email", operator: "IS NULL", args: []any{}},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_Where_Raw(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "products",
		wheres: []where{
			{conj: "AND", queryType: QueryRaw, expr: "price > ? AND stock < ?", args: []any{50, 100}},
		},
		limit:  -1,
		offset: -1,
	}

	for b.Loop() {
		_, _, _ = d.CompileSelect(builder)
	}
}

func BenchmarkPostgresDialect_CompileSelect_Select_Where_Group(b *testing.B) {
	d := PostgresDialect{}
	builder := &builder{
		dialect: d,
		action:  "select",
		table:   "users",
		wheres: []where{
			{conj: "AND", queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
			{conj: "AND", queryType: QueryNested, nested: []where{
				{queryType: QueryBasic, column: "name", operator: "=", args: []any{"John"}},
				{conj: "OR", queryType: QueryBasic, column: "age", operator: ">", args: []any{25}},
				{conj: "AND", queryType: QueryNested, nested: []where{
					{queryType: QueryBasic, column: "city", operator: "=", args: []any{"New York"}},
					{conj: "OR", queryType: QueryBasic, column: "country", operator: "=", args: []any{"USA"}},
					{conj: "AND", queryType: QueryNested, nested: []where{
						{queryType: QueryBasic, column: "zip", operator: "=", args: []any{"10001"}},
						{conj: "AND", queryType: QueryBasic, column: "active", operator: "=", args: []any{true}},
					}},
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
