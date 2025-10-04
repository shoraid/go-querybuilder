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

func TestPostgresDialect_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
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

func TestPostgresDialect_SelectRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
		expectedErr  error
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
			name: "should build raw select expression with mixed args",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectRaw("id, name, ? AS status, ? AS type", "active", 1).
					From("users")
			},
			expectedSQL:  `SELECT id, name, $1 AS status, $2 AS type FROM "users"`,
			expectedArgs: []any{"active", 1},
		},
		{
			name: "should return error when raw expression is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectRaw("").
					From("users")
			},
			expectedErr: ErrEmptyExpression,
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
			if tt.expectedErr != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedErr, "expected error to match")
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

func TestPostgresDialect_SelectSafe(t *testing.T) {
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

func TestPostgresDialect_SelectSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
		expectedErr  error
	}{
		{
			name: "should build select with subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectSub(func(qb QueryBuilder) {
						qb.
							Select("id").
							From("users").
							Where("status", "=", "active")
					}, "active_user_ids").
					From("posts")
			},
			expectedSQL:  `SELECT (SELECT "id" FROM "users" WHERE "status" = $1) AS "active_user_ids" FROM "posts"`,
			expectedArgs: []any{"active"},
		},
		{
			name: "should return error when subquery is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectSub(nil, "alias").
					From("posts")
			},
			expectedErr: ErrNilFunc,
		},
		{
			name: "should return error when alias is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectSub(func(qb QueryBuilder) {
						qb.Select("id").From("users")
					}, "").
					From("posts")
			},
			expectedErr: ErrEmptyAlias,
		},
		{
			name: "should return error when subquery compilation returns error",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectSub(func(qb QueryBuilder) {
						qb.Select("id").From("") // This will cause an error
					}, "alias").
					From("posts")
			},
			expectedErr: ErrEmptyTable,
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
			if tt.expectedErr != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedErr, "expected error to match")
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

func TestPostgresDialect_AddSelect(t *testing.T) {
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

func TestPostgresDialect_AddSelectRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
		expectedErr  error
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
		{
			name: "should return error when raw expression is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectRaw("").
					From("users")
			},
			expectedErr: ErrEmptyExpression,
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
			if tt.expectedErr != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedErr, "expected error to match")
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

func TestPostgresDialect_AddSelectSafe(t *testing.T) {
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

func TestPostgresDialect_AddSelectSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		build        func(*builder) QueryBuilder
		expectedSQL  string
		expectedArgs []any
		expectedErr  error
	}{
		{
			name: "should add select with subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("posts.id").
					AddSelectSub(func(qb QueryBuilder) {
						qb.
							SelectRaw("COUNT(*)").
							From("comments").
							WhereRaw(`"post_id" = "posts"."id"`)
					}, "comment_count").
					From("posts")
			},
			expectedSQL:  `SELECT "posts"."id", (SELECT COUNT(*) FROM "comments" WHERE "post_id" = "posts"."id") AS "comment_count" FROM "posts"`,
			expectedArgs: []any{},
		},
		{
			name: "should add select with subquery to an empty select",
			build: func(b *builder) QueryBuilder {
				return b.
					AddSelectSub(func(qb QueryBuilder) {
						qb.
							Select("id").
							From("users").
							Where("status", "=", "active")
					}, "active_user_ids").
					From("posts")
			},
			expectedSQL:  `SELECT (SELECT "id" FROM "users" WHERE "status" = $1) AS "active_user_ids" FROM "posts"`,
			expectedArgs: []any{"active"},
		},
		{
			name: "should return error when subquery is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectSub(nil, "alias").
					From("posts")
			},
			expectedErr: ErrNilFunc,
		},
		{
			name: "should return error when alias is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectSub(func(qb QueryBuilder) {
						qb.Select("id").From("users")
					}, "").
					From("posts")
			},
			expectedErr: ErrEmptyAlias,
		},
		{
			name: "should return error when subquery compilation returns error",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectSub(func(qb QueryBuilder) {
						qb.Select("id").From("") // This will cause an error
					}, "alias").
					From("posts")
			},
			expectedErr: ErrEmptyTable,
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
			if tt.expectedErr != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedErr, "expected error to match")
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

func TestPostgresDialect_From(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		{
			name: "should build from clause with single table",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users")
			},
			expectedSQL:  `SELECT * FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build from clause with table and alias",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("u.id").
					From("users u")
			},
			expectedSQL:  `SELECT "u"."id" FROM "users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name: "should build from clause with schema qualified table",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("public.users")
			},
			expectedSQL:  `SELECT * FROM "public"."users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build from clause with schema qualified table and alias",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("u.id").
					From("public.users u")
			},
			expectedSQL:  `SELECT "u"."id" FROM "public"."users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name: "should return error when table name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("")
			},
			expectedError: ErrEmptyTable,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match")
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

func TestPostgresDialect_FromRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		{
			name: "should build from raw clause with single table",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					FromRaw("users")
			},
			expectedSQL:  `SELECT * FROM users`,
			expectedArgs: []any{},
		},
		{
			name: "should build from raw clause with table and alias",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					FromRaw("users u")
			},
			expectedSQL:  `SELECT * FROM users u`,
			expectedArgs: []any{},
		},
		{
			name: "should build from raw clause with multiple args",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					FromRaw("(SELECT id, name FROM users WHERE status = ? AND created_at > ?) AS recent_active_users", "active", "2023-01-01")
			},
			expectedSQL:  `SELECT * FROM (SELECT id, name FROM users WHERE status = $1 AND created_at > $2) AS recent_active_users`,
			expectedArgs: []any{"active", "2023-01-01"},
		},
		{
			name: "should return error when table name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					FromRaw("")
			},
			expectedError: ErrEmptyExpression,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match")
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

func TestPostgresDialect_FromSafe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		{
			name: "should build from safe clause with single table",
			build: func(b *builder) QueryBuilder {
				userInput := "users"
				whitelist := map[string]string{
					"users": "users",
				}
				return b.
					Select().
					FromSafe(userInput, whitelist)
			},
			expectedSQL:  `SELECT * FROM "users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build from safe clause with table and alias",
			build: func(b *builder) QueryBuilder {
				userInput := "users"
				whitelist := map[string]string{
					"users": "users u",
				}
				return b.
					Select().
					FromSafe(userInput, whitelist)
			},
			expectedSQL:  `SELECT * FROM "users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name: "should build from safe clause with schema qualified table",
			build: func(b *builder) QueryBuilder {
				userInput := "public.users"
				whitelist := map[string]string{
					"public.users": "public.users",
				}
				return b.
					Select().
					FromSafe(userInput, whitelist)
			},
			expectedSQL:  `SELECT * FROM "public"."users"`,
			expectedArgs: []any{},
		},
		{
			name: "should build from safe clause with schema qualified table and alias",
			build: func(b *builder) QueryBuilder {
				userInput := "public.users u"
				whitelist := map[string]string{
					"public.users u": "public.users u",
				}
				return b.
					Select().
					FromSafe(userInput, whitelist)
			},
			expectedSQL:  `SELECT * FROM "public"."users" AS "u"`,
			expectedArgs: []any{},
		},
		{
			name: "should return error when table name is not in whitelist",
			build: func(b *builder) QueryBuilder {
				userInput := "not_allowed_table"
				whitelist := map[string]string{
					"users": "users",
				}
				return b.
					Select().
					FromSafe(userInput, whitelist)
			},
			expectedError: ErrInvalidTableInput,
		},
		{
			name: "should return error when table name is empty",
			build: func(b *builder) QueryBuilder {
				userInput := ""
				whitelist := map[string]string{
					"users": "users",
				}
				return b.
					Select().
					FromSafe(userInput, whitelist)
			},
			expectedError: ErrInvalidTableInput,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match")
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

func TestPostgresDialect_FromSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		{
			name: "should build from subquery with alias",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("t.id", "t.name").
					FromSub(func(qb QueryBuilder) {
						qb.
							Select("id", "name").
							From("users").
							Where("status", "=", "active")
					}, "t")
			},
			expectedSQL:  `SELECT "t"."id", "t"."name" FROM (SELECT "id", "name" FROM "users" WHERE "status" = $1) AS "t"`,
			expectedArgs: []any{"active"},
		},
		{
			name: "should build from subquery with alias and additional where clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("t.id", "t.name").
					FromSub(func(qb QueryBuilder) {
						qb.
							Select("id", "name").
							From("users").
							Where("status", "=", "active")
					}, "t").
					Where("t.id", ">", 10)
			},
			expectedSQL:  `SELECT "t"."id", "t"."name" FROM (SELECT "id", "name" FROM "users" WHERE "status" = $1) AS "t" WHERE "t"."id" > $2`,
			expectedArgs: []any{"active", 10},
		},
		{
			name: "should return error when subquery is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					FromSub(nil, "t")
			},
			expectedError: ErrNilFunc,
		},
		{
			name: "should return error when alias is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					FromSub(func(qb QueryBuilder) {
						qb.Select("id").From("users")
					}, "")
			},
			expectedError: ErrEmptyAlias,
		},
		{
			name: "should return error when subquery compilation fails",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					FromSub(func(qb QueryBuilder) {
						qb.
							Select("id").
							From("") // This will cause an error
					}, "t")
			},
			expectedError: ErrEmptyTable,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match")
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

func TestPostgresDialect_Where(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		// -------------------------------------------
		// --------------- Where Basic ---------------
		// -------------------------------------------
		{
			name: "should build query with a single basic WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("id", "=", 1)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1`,
			expectedArgs: []any{1},
		},
		{
			name: "should build query with multiple basic where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					Where("email", "LIKE", "%example.com%")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 AND "email" LIKE $2`,
			expectedArgs: []any{"active", "%example.com%"},
		},

		// -------------------------------------------
		// -------------- Where Between --------------
		// -------------------------------------------
		{
			name: "should build query with BETWEEN operator and multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "BETWEEN", 10, 50)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" BETWEEN $1 AND $2)`,
			expectedArgs: []any{10, 50},
		},
		{
			name: "should build query with BETWEEN operator and a slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "BETWEEN", []int{10, 50})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" BETWEEN $1 AND $2)`,
			expectedArgs: []any{10, 50},
		},
		{
			name: "should error when BETWEEN operator has a nil 'from' value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "BETWEEN", nil, 100)
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should error when BETWEEN operator has a nil 'to' value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "BETWEEN", 50, nil)
			},
			expectedError: ErrNilNotAllowed,
		},

		// -------------------------------------------
		// ------------ Where Not Between ------------
		// -------------------------------------------
		{
			name: "should build query with NOT BETWEEN operator and multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "NOT BETWEEN", 10, 50)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" NOT BETWEEN $1 AND $2)`,
			expectedArgs: []any{10, 50},
		},
		{
			name: "should build query with NOT BETWEEN operator and a slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "NOT BETWEEN", []int{10, 50})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE ("price" NOT BETWEEN $1 AND $2)`,
			expectedArgs: []any{10, 50},
		},
		{
			name: "should error when NOT BETWEEN operator has a nil 'from' value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "NOT BETWEEN", nil, 100)
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should error when NOT BETWEEN operator has a nil 'to' value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "NOT BETWEEN", 50, nil)
			},
			expectedError: ErrNilNotAllowed,
		},

		// -------------------------------------------
		// ---------------- Where In -----------------
		// -------------------------------------------
		{
			name: "should build query with IN operator and a single value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "IN", 1)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" IN ($1)`,
			expectedArgs: []any{1},
		},
		{
			name: "should build query with IN operator and multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "IN", "a", "b", "c")
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" IN ($1, $2, $3)`,
			expectedArgs: []any{"a", "b", "c"},
		},
		{
			name: "should build query with IN operator and a slice of values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "IN", []int{1, 2, 3})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" IN ($1, $2, $3)`,
			expectedArgs: []any{1, 2, 3},
		},
		{
			name: "should build query with IN operator and mixed slices",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "IN", []int{1, 2, 3}, []string{"a", "b", "c"})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" IN ($1, $2, $3, $4, $5, $6)`,
			expectedArgs: []any{1, 2, 3, "a", "b", "c"},
		},

		// -------------------------------------------
		// -------------- Where Not In ---------------
		// -------------------------------------------
		{
			name: "should build query with NOT IN operator and a single value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "NOT IN", 1)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" NOT IN ($1)`,
			expectedArgs: []any{1},
		},
		{
			name: "should build query with NOT IN operator and multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "NOT IN", "a", "b", "c")
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" NOT IN ($1, $2, $3)`,
			expectedArgs: []any{"a", "b", "c"},
		},
		{
			name: "should build query with NOT IN operator and a slice of values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "NOT IN", []int{1, 2, 3})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" NOT IN ($1, $2, $3)`,
			expectedArgs: []any{1, 2, 3},
		},
		{
			name: "should build query with NOT IN operator and mixed slices",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("category_id", "NOT IN", []int{1, 2, 3}, []string{"a", "b", "c"})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" NOT IN ($1, $2, $3, $4, $5, $6)`,
			expectedArgs: []any{1, 2, 3, "a", "b", "c"},
		},

		// -------------------------------------------
		// --------------- Where Null ----------------
		// -------------------------------------------
		{
			name: "should build query with IS NULL operator",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("email", "IS NULL")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NULL`,
			expectedArgs: []any{},
		},
		{
			name: "should build query with IS NULL operator and ignore extra value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("email", "IS NULL", 123)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NULL`,
			expectedArgs: []any{},
		},

		// -------------------------------------------
		// ------------- Where Not Null --------------
		// -------------------------------------------
		{
			name: "should build query with IS NOT NULL operator",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("email", "IS NOT NULL")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NOT NULL`,
			expectedArgs: []any{},
		},
		{
			name: "should build query with IS NOT NULL operator and ignore extra value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("email", "IS NOT NULL", 123)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "email" IS NOT NULL`,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match")
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

func TestPostgresDialect_OrWhere(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		// -------------------------------------------
		// ------------- Or Where Basic --------------
		// -------------------------------------------
		{
			name: "should build query with a single OR WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("id", "=", 1).
					OrWhere("name", "=", "John")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 OR "name" = $2`,
			expectedArgs: []any{1, "John"},
		},
		{
			name: "should build query with multiple OR WHERE clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					OrWhere("name", "=", "John").
					OrWhere("email", "LIKE", "%example.com%")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR "name" = $2 OR "email" LIKE $3`,
			expectedArgs: []any{"active", "John", "%example.com%"},
		},
		{
			name: "should treat leading OrWhere as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhere("name", "=", "John")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "name" = $1`,
			expectedArgs: []any{"John"},
		},

		// -------------------------------------------
		// ------------ Or Where Between -------------
		// -------------------------------------------
		{
			name: "should build query with OR BETWEEN operator and multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "<", 10).
					OrWhere("price", "BETWEEN", 10, 50)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" < $1 OR ("price" BETWEEN $2 AND $3)`,
			expectedArgs: []any{10, 10, 50},
		},
		{
			name: "should build query with OR BETWEEN operator and a slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "<", 10).
					OrWhere("price", "BETWEEN", []int{10, 50})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" < $1 OR ("price" BETWEEN $2 AND $3)`,
			expectedArgs: []any{10, 10, 50},
		},
		{
			name: "should error when OR BETWEEN operator has a nil 'from' value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("id", "=", 1).
					OrWhere("price", "BETWEEN", nil, 100)
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should error when OR BETWEEN operator has a nil 'to' value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("id", "=", 1).
					OrWhere("price", "BETWEEN", 50, nil)
			},
			expectedError: ErrNilNotAllowed,
		},

		// -------------------------------------------
		// ---------- Or Where Not Between -----------
		// -------------------------------------------
		{
			name: "should build query with OR NOT BETWEEN operator and multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "<", 10).
					OrWhere("price", "NOT BETWEEN", 10, 50)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" < $1 OR ("price" NOT BETWEEN $2 AND $3)`,
			expectedArgs: []any{10, 10, 50},
		},
		{
			name: "should build query with OR NOT BETWEEN operator and a slice",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", "<", 10).
					OrWhere("price", "NOT BETWEEN", []int{10, 50})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" < $1 OR ("price" NOT BETWEEN $2 AND $3)`,
			expectedArgs: []any{10, 10, 50},
		},
		{
			name: "should error when OR NOT BETWEEN operator has a nil 'from' value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("id", "=", 1).
					OrWhere("price", "NOT BETWEEN", nil, 100)
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should error when OR NOT BETWEEN operator has a nil 'to' value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("id", "=", 1).
					OrWhere("price", "NOT BETWEEN", 50, nil)
			},
			expectedError: ErrNilNotAllowed,
		},

		// -------------------------------------------
		// -------------- Or Where In ----------------
		// -------------------------------------------
		{
			name: "should build query with OR IN operator and a single value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhere("category_id", "IN", 1)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" IN ($2)`,
			expectedArgs: []any{100, 1},
		},
		{
			name: "should build query with OR IN operator and multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhere("category_id", "IN", "a", "b", "c")
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" IN ($2, $3, $4)`,
			expectedArgs: []any{100, "a", "b", "c"},
		},
		{
			name: "should build query with OR IN operator and a slice of values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhere("category_id", "IN", []int{1, 2, 3})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" IN ($2, $3, $4)`,
			expectedArgs: []any{100, 1, 2, 3},
		},
		{
			name: "should build query with OR IN operator and mixed slices",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhere("category_id", "NOT IN", []int{1, 2, 3}, []string{"a", "b", "c"})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" NOT IN ($2, $3, $4, $5, $6, $7)`,
			expectedArgs: []any{100, 1, 2, 3, "a", "b", "c"},
		},

		// -------------------------------------------
		// ------------ Or Where Not In --------------
		// -------------------------------------------
		{
			name: "should build query with OR NOT IN operator and a single value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhere("category_id", "NOT IN", 1)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" NOT IN ($2)`,
			expectedArgs: []any{100, 1},
		},
		{
			name: "should build query with OR NOT IN operator and multiple values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhere("category_id", "NOT IN", "a", "b", "c")
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" NOT IN ($2, $3, $4)`,
			expectedArgs: []any{100, "a", "b", "c"},
		},
		{
			name: "should build query with OR NOT IN operator and a slice of values",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhere("category_id", "NOT IN", []int{1, 2, 3})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" NOT IN ($2, $3, $4)`,
			expectedArgs: []any{100, 1, 2, 3},
		},
		{
			name: "should build query with OR NOT IN operator and mixed slices",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					OrWhere("category_id", "NOT IN", []int{1, 2, 3}, []string{"a", "b", "c"})
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "price" > $1 OR "category_id" NOT IN ($2, $3, $4, $5, $6, $7)`,
			expectedArgs: []any{100, 1, 2, 3, "a", "b", "c"},
		},

		// -------------------------------------------
		// ------------- Or Where Null ---------------
		// -------------------------------------------
		{
			name: "should build query with OR IS NULL operator",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("id", "=", 1).
					OrWhere("email", "IS NULL")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 OR "email" IS NULL`,
			expectedArgs: []any{1},
		},
		{
			name: "should build query with OR IS NULL operator and ignore extra value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("id", "=", 1).
					OrWhere("email", "IS NULL", 123)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 OR "email" IS NULL`,
			expectedArgs: []any{1},
		},

		// -------------------------------------------
		// ----------- Or Where Not Null -------------
		// -------------------------------------------
		{
			name: "should build query with OR IS NOT NULL operator",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("id", "=", 1).
					OrWhere("email", "IS NOT NULL")
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 OR "email" IS NOT NULL`,
			expectedArgs: []any{1},
		},
		{
			name: "should build query with OR IS NOT NULL operator and ignore extra value",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("id", "=", 1).
					OrWhere("email", "IS NOT NULL", 123)
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1 OR "email" IS NOT NULL`,
			expectedArgs: []any{1},
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			name: "should return error when 'from' value is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereBetween("category_id", nil, 100)
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should return error when 'to' value is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereBetween("category_id", 50, nil)
			},
			expectedError: ErrNilNotAllowed,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyColumn,
		},
		{
			name: "should return error when 'from' value is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereBetween("category_id", nil, 100)
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should return error when 'to' value is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereBetween("category_id", 50, nil)
			},
			expectedError: ErrNilNotAllowed,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyColumn,
		},
		{
			name: "should return error when 'from' value is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereNotBetween("category_id", nil, 100)
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should return error when 'to' value is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					WhereNotBetween("category_id", 50, nil)
			},
			expectedError: ErrNilNotAllowed,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyColumn,
		},
		{
			name: "should return error when 'from' value is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereNotBetween("category_id", nil, 100)
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should return error when 'to' value is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereNotBetween("category_id", 50, nil)
			},
			expectedError: ErrNilNotAllowed,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
	}{
		{
			name: "should build IN clause with single value",
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
			name: "should build IN clause with multiple values",
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
			name: "should build IN clause from single slice",
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
			name: "should build IN clause from multiple slices",
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
			name: "should build IN clause from mixed values and slice",
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
			name: "should build IN clause with boolean values",
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
			name: "should replace empty slice with 1=0",
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
			name: "should return error when column name is empty",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("warehouses").
					WhereIn("", 1, 2)
			},
			expectedError: ErrEmptyColumn,
		},
		{
			name: "should return error when nil is passed directly",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					Where("price", ">", 100).
					WhereIn("status", nil, "active")
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should return error when slice contains nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					Where("country", "=", "US").
					WhereIn("segment", []any{"premium", nil})
			},
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should return error when nested slice is passed",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("suppliers").
					WhereIn("id", [][]int{{1, 2}})
			},
			expectedError: ErrNestedSlice,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			name: "should treat leading OrWhereIn as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("customers").
					OrWhereIn("id", 1)
			},
			expectedSQL:  `SELECT * FROM "customers" WHERE "id" IN ($1)`,
			expectedArgs: []any{1},
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
			expectedError: ErrEmptyColumn,
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
			expectedError: ErrNilNotAllowed,
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
			expectedError: ErrNilNotAllowed,
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
			expectedError: ErrNestedSlice,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyColumn,
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
			expectedError: ErrNilNotAllowed,
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
			expectedError: ErrNilNotAllowed,
		},
		{
			name: "should return error when nested slice is passed",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("suppliers").
					WhereNotIn("id", [][]int{{1, 2}})
			},
			expectedError: ErrNestedSlice,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			name: "should treat leading OrWhereNotIn as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("products").
					OrWhereNotIn("category_id", 1, 2)
			},
			expectedSQL:  `SELECT * FROM "products" WHERE "category_id" NOT IN ($1, $2)`,
			expectedArgs: []any{1, 2},
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
			expectedError: ErrEmptyColumn,
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
			expectedError: ErrNilNotAllowed,
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
			expectedError: ErrNilNotAllowed,
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
			expectedError: ErrNestedSlice,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyColumn,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyColumn,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyColumn,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyColumn,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyExpression,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrEmptyExpression,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
		{
			name: "should return error when nil passed as group",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereGroup(nil)
			},
			expectedError: ErrNilFunc,
		},
		{
			name: "should return error when child query error",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereGroup(func(qb QueryBuilder) {
						qb.WhereIn("", 1, 2, 3) // it should be an error empty column
					})
			},
			expectedError: ErrEmptyColumn,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrNilFunc,
		},
		{
			name: "should return error when child query error",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereSub("id", "IN", func(q QueryBuilder) {
						q.Select("user_id").From("")
					})
			},
			expectedError: ErrEmptyTable,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
		expectedError error
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
			expectedError: ErrNilFunc,
		},
		{
			name: "should return error when child query error",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					OrWhereSub("id", "IN", func(q QueryBuilder) {
						q.Select("user_id").From("")
					})
			},
			expectedError: ErrEmptyTable,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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

func TestPostgresDialect_WhereExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		{
			name: "should build EXISTS clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id)`,
			expectedArgs: []any{},
		},
		{
			name: "should build EXISTS clause with other conditions",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 AND EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id AND "amount" > $2)`,
			expectedArgs: []any{"active", 100},
		},
		{
			name: "should build deeply nested EXISTS subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereExists(func(q1 QueryBuilder) {
						q1.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							WhereExists(func(q2 QueryBuilder) {
								q2.SelectRaw("1").
									From("order_items").
									WhereRaw("order_items.order_id = orders.id").
									Where("product_id", "=", 1)
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id AND EXISTS (SELECT 1 FROM "order_items" WHERE order_items.order_id = orders.id AND "product_id" = $1))`,
			expectedArgs: []any{1},
		},
		{
			name: "should return error when subquery builder is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereExists(nil)
			},
			expectedError: ErrNilFunc,
		},
		{
			name: "should return error when child query error",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereExists(func(q QueryBuilder) {
						q.Select("user_id").From("")
					})
			},
			expectedError: ErrEmptyTable,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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

func TestPostgresDialect_OrWhereExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		{
			name: "should build OR EXISTS clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id)`,
			expectedArgs: []any{"inactive"},
		},
		{
			name: "should build OR EXISTS clause with other conditions",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id AND "amount" > $2)`,
			expectedArgs: []any{"inactive", 100},
		},
		{
			name: "should treat leading OrWhereExists as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id)`,
			expectedArgs: []any{},
		},
		{
			name: "should build deeply nested OR EXISTS subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereExists(func(q1 QueryBuilder) {
						q1.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							OrWhereExists(func(q2 QueryBuilder) {
								q2.SelectRaw("1").
									From("order_items").
									WhereRaw("order_items.order_id = orders.id").
									Where("product_id", "=", 1)
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id OR EXISTS (SELECT 1 FROM "order_items" WHERE order_items.order_id = orders.id AND "product_id" = $2))`,
			expectedArgs: []any{"inactive", 1},
		},
		{
			name: "should return error when subquery builder is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereExists(nil)
			},
			expectedError: ErrNilFunc,
		},
		{
			name: "should return error when child query error",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereExists(func(q QueryBuilder) {
						q.Select("user_id").From("")
					})
			},
			expectedError: ErrEmptyTable,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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

func TestPostgresDialect_WhereNotExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		{
			name: "should build NOT EXISTS clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNotExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE NOT EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id)`,
			expectedArgs: []any{},
		},
		{
			name: "should build NOT EXISTS clause with other conditions",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "active").
					WhereNotExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 AND NOT EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id AND "amount" > $2)`,
			expectedArgs: []any{"active", 100},
		},
		{
			name: "should build deeply nested NOT EXISTS subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNotExists(func(q1 QueryBuilder) {
						q1.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							WhereNotExists(func(q2 QueryBuilder) {
								q2.SelectRaw("1").
									From("order_items").
									WhereRaw("order_items.order_id = orders.id").
									Where("product_id", "=", 1)
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE NOT EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id AND NOT EXISTS (SELECT 1 FROM "order_items" WHERE order_items.order_id = orders.id AND "product_id" = $1))`,
			expectedArgs: []any{1},
		},
		{
			name: "should return error when subquery builder is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNotExists(nil)
			},
			expectedError: ErrNilFunc,
		},
		{
			name: "should return error when child query error",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					WhereNotExists(func(q QueryBuilder) {
						q.Select("user_id").From("")
					})
			},
			expectedError: ErrEmptyTable,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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

func TestPostgresDialect_OrWhereNotExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		build         func(*builder) QueryBuilder
		expectedSQL   string
		expectedArgs  []any
		expectedError error
	}{
		{
			name: "should build OR NOT EXISTS clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereNotExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR NOT EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id)`,
			expectedArgs: []any{"inactive"},
		},
		{
			name: "should build OR NOT EXISTS clause with other conditions",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereNotExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							Where("amount", ">", 100)
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR NOT EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id AND "amount" > $2)`,
			expectedArgs: []any{"inactive", 100},
		},
		{
			name: "should treat leading OrWhereNotExists as first WHERE clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					OrWhereNotExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE NOT EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id)`,
			expectedArgs: []any{},
		},
		{
			name: "should build deeply nested OR NOT EXISTS subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereNotExists(func(q1 QueryBuilder) {
						q1.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							OrWhereNotExists(func(q2 QueryBuilder) {
								q2.SelectRaw("1").
									From("order_items").
									WhereRaw("order_items.order_id = orders.id").
									Where("product_id", "=", 1)
							})
					})
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "status" = $1 OR NOT EXISTS (SELECT 1 FROM "orders" WHERE orders.user_id = users.id OR NOT EXISTS (SELECT 1 FROM "order_items" WHERE order_items.order_id = orders.id AND "product_id" = $2))`,
			expectedArgs: []any{"inactive", 1},
		},
		{
			name: "should return error when subquery builder is nil",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereNotExists(nil)
			},
			expectedError: ErrNilFunc,
		},
		{
			name: "should return error when child query error",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereNotExists(func(q QueryBuilder) {
						q.Select("user_id").From("")
					})
			},
			expectedError: ErrEmptyTable,
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
			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error to match output")
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
				dialect: PostgresDialect{},
				action:  "select",
				table: table{
					queryType: QueryBasic,
					name:      tt.table,
				},
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
				table: table{
					queryType: QueryBasic,
					name:      tt.table,
				},
				limit:  tt.limit,
				offset: tt.offset,
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

func BenchmarkPostgresDialect_Select(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "select all",
			build: func(b *builder) QueryBuilder {
				return b.
					Select().
					From("users")
			},
		},
		{
			name: "select one column",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					From("users")
			},
		},
		{
			name: "select two columns",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id", "name").
					From("users")
			},
		},
		{
			name: "select multiple columns",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id", "name", "email", "email_verified_at", "phone_number", "age", "image", "created_at", "updated_at").
					From("users")
			},
		},
		{
			name: "select with alias",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("u.id", "u.name AS user_name").
					From("users u")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				builder := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(builder)
				_, _, _ = builder.dialect.CompileSelect(builder)
			}
		})
	}
}

func BenchmarkPostgresDialect_SelectRaw(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "select raw column",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectRaw("COUNT(*)").
					From("users")
			},
		},
		{
			name: "select raw with args",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectRaw("SUM(CASE WHEN status = ? THEN 1 ELSE 0 END)", "active").
					From("users")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				builder := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(builder)
				_, _, _ = builder.dialect.CompileSelect(builder)
			}
		})
	}
}

func BenchmarkPostgresDialect_SelectSafe(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "simple select safe",
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
		},
		{
			name: "select safe with alias",
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
		},
		{
			name: "select safe with ignore inputs",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"id", "name", "not_allowed"}
				whitelist := map[string]string{
					"id":   "id",
					"name": "u.name",
				}

				return b.
					SelectSafe(userInput, whitelist).
					From("users")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				builder := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(builder)
				_, _, _ = builder.dialect.CompileSelect(builder)
			}
		})
	}
}

func BenchmarkPostgresDialect_SelectSub(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "simple select subquery",
			build: func(b *builder) QueryBuilder {
				return b.
					SelectSub(func(qb QueryBuilder) {
						qb.SelectRaw("COUNT(*)").From("orders").WhereRaw("orders.user_id = users.id")
					}, "order_count").
					From("users")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				builder := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(builder)
				_, _, _ = builder.dialect.CompileSelect(builder)
			}
		})
	}
}

func BenchmarkPostgresDialect_AddSelect(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "add one column to existing column",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelect("email").
					From("users")
			},
		},
		{
			name: "add two columns to existing column",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelect("name", "email").
					From("users")
			},
		},
		{
			name: "add multiple columns to existing column",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelect("name", "email", "email_verified_at", "phone_number", "age", "image", "created_at", "updated_at").
					From("users")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				builder := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(builder)
				_, _, _ = builder.dialect.CompileSelect(builder)
			}
		})
	}
}

func BenchmarkPostgresDialect_AddSelectRaw(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "add select raw to existing select",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectRaw("COUNT(*) as total_users").
					From("users")
			},
		},
		{
			name: "add select raw with args to existing select",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectRaw("SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS active_users", "active").
					From("users")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				builder := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(builder)
				_, _, _ = builder.dialect.CompileSelect(builder)
			}
		})
	}
}

func BenchmarkPostgresDialect_AddSelectSafe(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "add select safe to existing select",
			build: func(b *builder) QueryBuilder {
				userInput := []string{"name", "email"}
				whitelist := map[string]string{
					"name":  "name",
					"email": "email",
				}

				return b.
					Select("id").
					AddSelectSafe(userInput, whitelist).
					From("users")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				builder := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(builder)
				_, _, _ = builder.dialect.CompileSelect(builder)
			}
		})
	}
}

func BenchmarkPostgresDialect_AddSelectSub(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "add select subquery to existing select",
			build: func(b *builder) QueryBuilder {
				return b.
					Select("id").
					AddSelectSub(func(qb QueryBuilder) {
						qb.SelectRaw("COUNT(*)").From("orders").WhereRaw("orders.user_id = users.id")
					}, "order_count").
					From("users")
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for b.Loop() {
				builder := &builder{
					dialect: PostgresDialect{},
					limit:   -1,
					offset:  -1,
				}

				bm.build(builder)
				_, _, _ = builder.dialect.CompileSelect(builder)
			}
		})
	}
}

func BenchmarkPostgresDialect_Where(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "single where clause",
			build: func(b *builder) QueryBuilder {
				return b.Where("id", "=", 1)
			},
		},
		{
			name: "multiple where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Where("id", "=", 1).
					Where("name", "LIKE", "test")
			},
		},
		{
			name: "single OR where clause",
			build: func(b *builder) QueryBuilder {
				return b.
					Where("id", "=", 1).
					OrWhere("name", "LIKE", "test")
			},
		},
		{
			name: "multiple OR where clauses",
			build: func(b *builder) QueryBuilder {
				return b.
					Where("id", "=", 1).
					OrWhere("name", "LIKE", "test").
					OrWhere("email", "LIKE", "test@example.com")
			},
		},
		{
			name: "between clause with multiple values",
			build: func(b *builder) QueryBuilder {
				return b.Where("age", "BETWEEN", 18, 65)
			},
		},
		{
			name: "between clause with a slice",
			build: func(b *builder) QueryBuilder {
				return b.Where("age", "BETWEEN", []int{18, 65})
			},
		},
		{
			name: "not between clause with multiple values",
			build: func(b *builder) QueryBuilder {
				return b.Where("age", "NOT BETWEEN", 18, 65)
			},
		},
		{
			name: "not between clause with a slice",
			build: func(b *builder) QueryBuilder {
				return b.Where("age", "NOT BETWEEN", []int{18, 65})
			},
		},
		{
			name: "in clause with multiple values",
			build: func(b *builder) QueryBuilder {
				return b.Where("id", "IN", 1, 2, 3)
			},
		},
		{
			name: "in clause with a slice",
			build: func(b *builder) QueryBuilder {
				return b.Where("id", "IN", []int{1, 2, 3})
			},
		},
		{
			name: "in clause with mixed slices",
			build: func(b *builder) QueryBuilder {
				return b.Where("id", "IN", []int{1, 2, 3}, []string{"a", "b", "c"})
			},
		},
		{
			name: "not in clause with multiple values",
			build: func(b *builder) QueryBuilder {
				return b.Where("id", "NOT IN", 1, 2, 3)
			},
		},
		{
			name: "not in clause with a slice",
			build: func(b *builder) QueryBuilder {
				return b.Where("id", "NOT IN", []int{1, 2, 3})
			},
		},
		{
			name: "not in clause with mixed slices",
			build: func(b *builder) QueryBuilder {
				return b.Where("id", "NOT IN", []int{1, 2, 3}, []string{"a", "b", "c"})
			},
		},
		{
			name: "null clause",
			build: func(b *builder) QueryBuilder {
				return b.Where("email", "NULL")
			},
		},
		{
			name: "not null clause",
			build: func(b *builder) QueryBuilder {
				return b.Where("email", "NOT NULL")
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

func BenchmarkPostgresDialect_WhereExists(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereExists simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
		},
		{
			name: "OrWhereExists simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
		},
		{
			name: "WhereExists nested",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereExists(func(q1 QueryBuilder) {
						q1.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							WhereExists(func(q2 QueryBuilder) {
								q2.SelectRaw("1").
									From("order_items").
									WhereRaw("order_items.order_id = orders.id")
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

func BenchmarkPostgresDialect_WhereNotExists(b *testing.B) {
	benchmarks := []struct {
		name  string
		build func(*builder) QueryBuilder
	}{
		{
			name: "WhereNotExists simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereNotExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
		},
		{
			name: "OrWhereNotExists simple",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					Where("status", "=", "inactive").
					OrWhereNotExists(func(q QueryBuilder) {
						q.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id")
					})
			},
		},
		{
			name: "WhereNotExists nested",
			build: func(bd *builder) QueryBuilder {
				return bd.
					Select().
					From("users").
					WhereNotExists(func(q1 QueryBuilder) {
						q1.SelectRaw("1").
							From("orders").
							WhereRaw("orders.user_id = users.id").
							WhereNotExists(func(q2 QueryBuilder) {
								q2.SelectRaw("1").
									From("order_items").
									WhereRaw("order_items.order_id = orders.id")
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
