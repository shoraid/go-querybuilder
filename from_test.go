package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_From(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table string
	}{
		{"should set table name correctly", "users"},
		{"should accept table with alias", "users u"},
		{"should accept schema.table", "public.users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{}

			// Act
			result := b.From(tt.table)

			// Assert
			assert.Equal(t, tt.table, b.table, "expected table to be set correctly")
			assert.Equal(t, b, result, "expected From() to return the same builder instance")
		})
	}
}

func TestBuilder_FromSafe(t *testing.T) {
	t.Parallel()

	whitelist := map[string]string{
		"users":    "users",
		"products": "products",
		"orders":   "public.orders",
		"items":    "items i",
	}

	tests := []struct {
		name          string
		userInput     string
		whitelist     map[string]string
		expectedTable string
		expectedError string
	}{
		{
			name:          "should set table from whitelist",
			userInput:     "users",
			whitelist:     whitelist,
			expectedTable: "users",
			expectedError: "",
		},
		{
			name:          "should set table with schema from whitelist",
			userInput:     "orders",
			whitelist:     whitelist,
			expectedTable: "public.orders",
			expectedError: "",
		},
		{
			name:          "should set table with alias from whitelist",
			userInput:     "items",
			whitelist:     whitelist,
			expectedTable: "items i",
			expectedError: "",
		},
		{
			name:          "should return error for invalid table",
			userInput:     "invalid_table",
			whitelist:     whitelist,
			expectedTable: "",
			expectedError: "invalid table: invalid_table",
		},
		{
			name:          "should return error for empty whitelist",
			userInput:     "users",
			whitelist:     map[string]string{},
			expectedTable: "",
			expectedError: "invalid table: users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{}

			// Act
			result, err := b.FromSafe(tt.userInput, tt.whitelist)

			// Assert
			if tt.expectedError != "" {
				assert.Error(t, err, "expected an error")
				assert.Contains(t, err.Error(), tt.expectedError, "error message should match")
				assert.Nil(t, result, "expected nil builder on error")
				assert.Empty(t, b.table, "expected table to be empty on error")
				return
			}

			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedTable, b.table, "expected table to be set correctly")
			assert.Equal(t, b, result, "expected FromSafe() to return the same builder instance")
		})
	}
}
