package goquerybuilder

import (
	"testing"

	"github.com/shoraid/go-querybuilder/dialect"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_OrderBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		column      string
		direction   string
		expectedSQL []string
	}{
		{
			name:        "should add single ASC order by clause",
			column:      "id",
			direction:   "asc",
			expectedSQL: []string{`"id" ASC`},
		},
		{
			name:        "should add single DESC order by clause",
			column:      "name",
			direction:   "DESC",
			expectedSQL: []string{`"name" DESC`},
		},
		{
			name:        "should default to ASC for invalid direction",
			column:      "created_at",
			direction:   "invalid",
			expectedSQL: []string{`"created_at" ASC`},
		},
		{
			name:        "should handle multiple order by calls",
			column:      "email",
			direction:   "ASC",
			expectedSQL: []string{`"id" ASC`, `"email" ASC`}, // Assuming a previous call added "id" ASC
		},
		{
			name:        "should quote column name correctly",
			column:      "user_id",
			direction:   "DESC",
			expectedSQL: []string{`"user_id" DESC`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: dialect.PostgresDialect{}}
			if tt.name == "should handle multiple order by calls" {
				b.OrderBy("id", "ASC")
			}

			// Act
			result := b.OrderBy(tt.column, tt.direction)

			// Assert
			assert.Contains(t, b.orderBys, tt.expectedSQL[len(tt.expectedSQL)-1], "expected order by clause to be added")
			assert.Equal(t, b, result, "expected OrderBy() to return the same builder instance")
		})
	}
}

func TestBuilder_OrderByRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		expression  string
		expectedSQL []string
	}{
		{
			name:        "should add raw order by expression",
			expression:  "LENGTH(name) DESC",
			expectedSQL: []string{"LENGTH(name) DESC"},
		},
		{
			name:        "should add another raw order by expression",
			expression:  "RANDOM()",
			expectedSQL: []string{"id ASC", "RANDOM()"}, // Assuming a previous call added "id ASC"
		},
		{
			name:        "should handle complex raw expression",
			expression:  "CASE WHEN status = 'active' THEN 1 ELSE 0 END DESC",
			expectedSQL: []string{"CASE WHEN status = 'active' THEN 1 ELSE 0 END DESC"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: dialect.PostgresDialect{}}
			if tt.name == "should add another raw order by expression" {
				b.OrderBy("id", "ASC")
			}

			// Act
			result := b.OrderByRaw(tt.expression)

			// Assert
			assert.Contains(t, b.orderBys, tt.expectedSQL[len(tt.expectedSQL)-1], "expected raw order by expression to be added")
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
		name          string
		userInput     string
		dir           string
		whitelist     map[string]string
		expectedOrder []string
		expectedError string
	}{
		{
			name:          "should add valid column and direction",
			userInput:     "id",
			dir:           "asc",
			whitelist:     whitelist,
			expectedOrder: []string{`"id" ASC`},
		},
		{
			name:          "should add valid column with DESC direction",
			userInput:     "name",
			dir:           "DESC",
			whitelist:     whitelist,
			expectedOrder: []string{`"name" DESC`},
		},
		{
			name:          "should default to ASC for invalid direction",
			userInput:     "created_at",
			dir:           "invalid",
			whitelist:     whitelist,
			expectedOrder: []string{`"created_at" ASC`},
		},
		{
			name:          "should handle column with alias from whitelist",
			userInput:     "email",
			dir:           "desc",
			whitelist:     whitelist,
			expectedOrder: []string{`"u"."email" DESC`},
		},
		{
			name:          "should return error for invalid column",
			userInput:     "invalid_col",
			dir:           "asc",
			whitelist:     whitelist,
			expectedOrder: nil,
			expectedError: "invalid order by column: invalid_col",
		},
		{
			name:          "should handle multiple safe order by calls",
			userInput:     "name",
			dir:           "ASC",
			whitelist:     whitelist,
			expectedOrder: []string{`"id" DESC`, `"name" ASC`}, // Assuming a previous call added "id" DESC
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: dialect.PostgresDialect{}}
			if tt.name == "should handle multiple safe order by calls" {
				_, _ = b.OrderBySafe("id", "desc", whitelist)
			}

			// Act
			result, err := b.OrderBySafe(tt.userInput, tt.dir, tt.whitelist)

			// Assert
			if tt.expectedError != "" {
				assert.Error(t, err, "expected an error")
				assert.Contains(t, err.Error(), tt.expectedError, "error message should match")
				assert.Nil(t, result, "expected nil builder on error")
				assert.Empty(t, b.orderBys, "expected order to be empty on error")
				return
			}

			assert.NoError(t, err, "expected no error")
			assert.Equal(t, b, result, "expected OrderBySafe() to return the same builder instance")
			assert.Equal(t, tt.expectedOrder, b.orderBys, "expected order by clauses to be updated correctly")
		})
	}
}
