package goquerybuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresDialect_Placeholder(t *testing.T) {
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
			// Arrange
			d := PostgresDialect{}

			// Act
			result := d.Placeholder(tt.n)

			// Assert
			assert.Equal(t, tt.expected, result, "expected placeholder to match")
		})
	}
}

func TestPostgresDialect_QuoteIdentifier(t *testing.T) {
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
			name:     "should quote empty identifier",
			input:    "",
			expected: `""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			d := PostgresDialect{}

			// Act
			result := d.QuoteIdentifier(tt.input)

			// Assert
			assert.Equal(t, tt.expected, result, "expected quoted identifier to match")
		})
	}
}

func TestPostgresDialect_QuoteTableWithAlias(t *testing.T) {
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
			expected: `"public.users"`,
		},
		{
			name:     "should quote table with alias",
			input:    "users u",
			expected: `"users" AS u`,
		},
		{
			name:     "should quote table with alias containing number",
			input:    "orders o1",
			expected: `"orders" AS o1`,
		},
		{
			name:     "should handle extra spaces between table and alias",
			input:    "users     u",
			expected: `"users" AS u`,
		},
		{
			name:     "should quote table name with underscore",
			input:    "user_profile up",
			expected: `"user_profile" AS up`,
		},
		{
			name:     "should quote table name with hyphen",
			input:    "order-items oi",
			expected: `"order-items" AS oi`,
		},
		{
			name:     "should quote table name without alias but with mixed case",
			input:    "UserTable",
			expected: `"UserTable"`,
		},
		{
			name:     "should quote table name with alias and mixed case",
			input:    "UserTable ut",
			expected: `"UserTable" AS ut`,
		},
		{
			name:     "should quote empty string (edge case)",
			input:    "",
			expected: `""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			d := PostgresDialect{}

			// Act
			result := d.QuoteTableWithAlias(tt.input)

			// Assert
			assert.Equal(t, tt.expected, result, "expected quoted table with alias to match")
		})
	}
}

func TestPostgresDialect_QuoteColumnWithAlias(t *testing.T) {
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
			expected: `"name" AS username`,
		},
		{
			name:     "should quote column with alias using mixed case",
			input:    "Name AS UserName",
			expected: `"Name" AS UserName`,
		},
		{
			name:     "should quote table.column without alias",
			input:    "users.id",
			expected: `"users"."id"`,
		},
		{
			name:     "should quote table.column with alias",
			input:    "users.id AS user_id",
			expected: `"users"."id" AS user_id`,
		},
		{
			name:     "should quote column with SQL function and alias",
			input:    "COUNT(id) AS total",
			expected: `"COUNT(id)" AS total`, // function treated as identifier
		},
		{
			name:     "should handle extra spaces before alias",
			input:    "email     AS    email_address",
			expected: `"email" AS email_address`,
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
			name:     "should quote empty string",
			input:    "",
			expected: `""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			d := PostgresDialect{}

			// Act
			result := d.QuoteColumnWithAlias(tt.input)

			// Assert
			assert.Equal(t, tt.expected, result, "expected quoted column with alias to match")
		})
	}
}

func TestPostgresDialect_Capabilities(t *testing.T) {
	tests := []struct {
		name              string
		expectedReturning bool
		expectedFullJoin  bool
		expectedIntersect bool
		expectedExcept    bool
	}{
		{
			name:              "should return correct capabilities for Postgres",
			expectedReturning: true,
			expectedFullJoin:  true,
			expectedIntersect: true,
			expectedExcept:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			d := PostgresDialect{}

			// Act
			caps := d.Capabilities()

			// Assert
			assert.Equal(t, tt.expectedReturning, caps.SupportsReturning, "expected SupportsReturning to match")
			assert.Equal(t, tt.expectedFullJoin, caps.SupportsFullJoin, "expected SupportsFullJoin to match")
			assert.Equal(t, tt.expectedIntersect, caps.SupportsIntersect, "expected SupportsIntersect to match")
			assert.Equal(t, tt.expectedExcept, caps.SupportsExcept, "expected SupportsExcept to match")
		})
	}
}
