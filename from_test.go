package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_From(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialTable  table
		tableName     string
		expectedTable table
		expectedError error
	}{
		{
			name:          "should set table name correctly",
			tableName:     "users",
			expectedTable: table{queryType: QueryBasic, name: "users"},
		},
		{
			name:          "should set table name with schema",
			tableName:     "public.users",
			expectedTable: table{queryType: QueryBasic, name: "public.users"},
		},
		{
			name:          "should set table name with alias",
			tableName:     "users as u",
			expectedTable: table{queryType: QueryBasic, name: "users as u"},
		},
		{
			name:          "should overwrite existing table name",
			initialTable:  table{queryType: QueryBasic, name: "old_users"},
			tableName:     "new_users",
			expectedTable: table{queryType: QueryBasic, name: "new_users"},
		},
		{
			name:          "should return error for empty table name",
			tableName:     "",
			expectedTable: table{},
			expectedError: ErrEmptyTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{table: tt.initialTable}

			// Act
			result := b.From(tt.tableName)

			// Assert
			if tt.expectedError != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedError, "expected error message to match")
				return
			}

			assert.NoError(t, b.err)
			assert.Equal(t, tt.expectedTable, b.table, "expected table to be updated correctly")
			assert.Equal(t, b, result, "expected From() to return the same builder instance")
		})
	}
}

func TestBuilder_FromRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialTable  table
		expression    string
		args          []any
		expectedTable table
		expectedError error
	}{
		{
			name:          "should set raw table expression",
			expression:    "(SELECT id, name FROM users WHERE status = ?) as active_users",
			args:          []any{"active"},
			expectedTable: table{queryType: QueryRaw, expr: "(SELECT id, name FROM users WHERE status = ?) as active_users", args: []any{"active"}},
		},
		{
			name:          "should overwrite existing table with raw expression",
			initialTable:  table{queryType: QueryBasic, name: "old_table"},
			expression:    "my_function(?, ?) as result",
			args:          []any{1, "test"},
			expectedTable: table{queryType: QueryRaw, expr: "my_function(?, ?) as result", args: []any{1, "test"}},
		},
		{
			name:          "should handle raw expression without arguments",
			expression:    "generate_series(1, 10) as s(id)",
			args:          []any{},
			expectedTable: table{queryType: QueryRaw, expr: "generate_series(1, 10) as s(id)", args: []any{}},
		},
		{
			name:          "should return error for empty raw expression",
			expression:    "",
			args:          []any{},
			expectedTable: table{},
			expectedError: ErrEmptyExpression,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{table: tt.initialTable}

			// Act
			result := b.FromRaw(tt.expression, tt.args...)

			// Assert
			if tt.expectedError != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedError, "expected error message to match")
				return
			}

			assert.NoError(t, b.err)
			assert.Equal(t, tt.expectedTable, b.table, "expected table to be updated correctly")
			assert.Equal(t, b, result, "expected FromRaw() to return the same builder instance")
		})
	}
}

func TestBuilder_FromSafe(t *testing.T) {
	t.Parallel()

	whitelist := map[string]string{
		"users":    "users",
		"orders":   "orders o",
		"products": "public.products",
	}

	tests := []struct {
		name          string
		initialTable  table
		userInput     string
		whitelist     map[string]string
		expectedTable table
		expectedError error
	}{
		{
			name:          "should set valid table name from whitelist",
			userInput:     "users",
			whitelist:     whitelist,
			expectedTable: table{queryType: QueryBasic, name: "users"},
		},
		{
			name:          "should set valid table name with schema from whitelist",
			userInput:     "products",
			whitelist:     whitelist,
			expectedTable: table{queryType: QueryBasic, name: "public.products"},
		},
		{
			name:          "should overwrite existing table with valid table from whitelist",
			initialTable:  table{queryType: QueryBasic, name: "old_table"},
			userInput:     "orders",
			whitelist:     whitelist,
			expectedTable: table{queryType: QueryBasic, name: "orders o"},
		},
		{
			name:          "should return error for invalid table name not in whitelist",
			userInput:     "invalid_table",
			whitelist:     whitelist,
			expectedTable: table{},
			expectedError: ErrInvalidTableInput,
		},
		{
			name:          "should return error for empty user input",
			userInput:     "",
			whitelist:     whitelist,
			expectedTable: table{},
			expectedError: ErrInvalidTableInput,
		},
		{
			name:          "should return error for empty whitelist",
			userInput:     "users",
			whitelist:     map[string]string{},
			expectedTable: table{},
			expectedError: ErrInvalidTableInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{table: tt.initialTable}

			// Act
			result := b.FromSafe(tt.userInput, tt.whitelist)

			// Assert
			if tt.expectedError != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedError, "expected error message to match")
				return
			}

			assert.NoError(t, b.err)
			assert.Equal(t, tt.expectedTable, b.table, "expected table to be updated correctly")
			assert.Equal(t, b, result, "expected FromRaw() to return the same builder instance")
		})
	}
}

func TestBuilder_FromSub(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialTable  table
		subQueryFn    func(QueryBuilder)
		alias         string
		expectedTable table
		expectedError error
	}{
		{
			name: "should set subquery table correctly",
			subQueryFn: func(qb QueryBuilder) {
				qb.Select("id", "name").From("users").Where("status", "=", "active")
			},
			alias: "active_users",
			expectedTable: table{
				queryType: QuerySub,
				name:      "active_users",
				sub: &builder{
					columns: []column{
						{queryType: QueryBasic, name: "id"},
						{queryType: QueryBasic, name: "name"},
					},
					table: table{queryType: QueryBasic, name: "users"},
					wheres: []where{
						{queryType: QueryBasic, conj: "AND", column: "status", operator: "=", args: []any{"active"}},
					},
				},
			},
		},
		{
			name:         "should overwrite existing table with subquery",
			initialTable: table{queryType: QueryBasic, name: "old_table"},
			subQueryFn: func(qb QueryBuilder) {
				qb.Select("name").From("products")
			},
			alias: "product_count",
			expectedTable: table{
				queryType: QuerySub,
				name:      "product_count",
				sub: &builder{
					columns: []column{
						{queryType: QueryBasic, name: "name"},
					},
					table: table{queryType: QueryBasic, name: "products"},
				},
			},
		},
		{
			name:          "should return error for nil subquery function",
			subQueryFn:    nil,
			alias:         "sub",
			expectedTable: table{},
			expectedError: ErrNilFunc,
		},
		{
			name:          "should return error for empty alias",
			subQueryFn:    func(qb QueryBuilder) {},
			alias:         "",
			expectedTable: table{},
			expectedError: ErrEmptyAlias,
		},
		{
			name: "should propagate error from subquery builder",
			subQueryFn: func(qb QueryBuilder) {
				qb.From("") // This will cause an error in the sub-builder
			},
			alias:         "error_sub",
			expectedTable: table{},
			expectedError: ErrEmptyTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: PostgresDialect{},
				table:   tt.initialTable,
			}

			// Act
			result := b.FromSub(tt.subQueryFn, tt.alias)

			// Assert
			if tt.expectedError != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedError, "expected error message to match")
				return
			}

			assert.NoError(t, b.err, "expected no error")

			if tt.expectedTable.sub != nil {
				// Compare sub-builder's internal state
				expectedSubBuilder := tt.expectedTable.sub.(*builder)
				actualSubBuilder := b.table.sub.(*builder)

				assert.Equal(t, expectedSubBuilder.columns, actualSubBuilder.columns, "sub-builder columns mismatch")
				assert.Equal(t, expectedSubBuilder.table, actualSubBuilder.table, "sub-builder table mismatch")
				assert.Equal(t, expectedSubBuilder.wheres, actualSubBuilder.wheres, "sub-builder wheres mismatch")
			} else {
				assert.Nil(t, b.table.sub, "expected sub-builder to be nil")
			}

			assert.Equal(t, b, result, "expected FromSub() to return the same builder instance")
		})
	}
}

// -----------------
// --- BENCHMARK ---
// -----------------

func BenchmarkBuilder_From(b *testing.B) {
	builder := &builder{}
	tableName := "users"

	for b.Loop() {
		builder.From(tableName)
	}
}

func BenchmarkBuilder_FromRaw(b *testing.B) {
	builder := &builder{}
	expression := "(SELECT id, name FROM users WHERE status = ?) as active_users"
	args := []any{"active"}

	for b.Loop() {
		builder.FromRaw(expression, args...)
	}
}

func BenchmarkBuilder_FromSafe(b *testing.B) {
	builder := &builder{}
	userInput := "active_users"
	whitelist := map[string]string{
		"users":        "users",
		"orders":       "orders",
		"products":     "public.products",
		"active_users": "(SELECT id, name FROM users WHERE status = 'active')",
	}

	for b.Loop() {
		builder.FromSafe(userInput, whitelist)
	}
}

func BenchmarkBuilder_FromSub(b *testing.B) {
	builder := &builder{dialect: PostgresDialect{}}

	for b.Loop() {
		builder.FromSub(func(qb QueryBuilder) {
			qb.Select("id", "name").From("users").Where("status", "=", "active")
		}, "active_users")
	}
}
