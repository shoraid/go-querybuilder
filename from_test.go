package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_From(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		table         string
		expectedTable string
		expectedError error
	}{
		{
			name:          "should set table name",
			table:         "users",
			expectedTable: "users",
			expectedError: nil,
		},
		{
			name:          "should set table name with schema",
			table:         "public.users",
			expectedTable: "public.users",
			expectedError: nil,
		},
		{
			name:          "should set table name with alias",
			table:         "users u",
			expectedTable: "users u",
			expectedError: nil,
		},
		{
			name:          "should return error for empty table name",
			table:         "",
			expectedTable: "",
			expectedError: ErrEmptyTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{}

			// Act
			result := b.From(tt.table)

			// Assert
			if tt.expectedError != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedError, "expected error message to match")
				return
			}

			assert.NoError(t, b.err, "expected no error")
			assert.Equal(t, tt.expectedTable, b.table, "expected table to be set correctly")
			assert.Equal(t, b, result, "expected From() to return the same builder instance")
		})
	}
}

func TestBuilder_FromSafe(t *testing.T) {
	t.Parallel()

	whitelist := map[string]string{
		"users":          "users",
		"orders":         "public.orders",
		"products_alias": "products p",
	}

	tests := []struct {
		name          string
		userInput     string
		whitelist     map[string]string
		expectedTable string
		expectedError error
	}{
		{
			name:          "should set table from whitelist",
			userInput:     "users",
			whitelist:     whitelist,
			expectedTable: "users",
			expectedError: nil,
		},
		{
			name:          "should set table with schema from whitelist",
			userInput:     "orders",
			whitelist:     whitelist,
			expectedTable: "public.orders",
			expectedError: nil,
		},
		{
			name:          "should set table with alias from whitelist",
			userInput:     "products_alias",
			whitelist:     whitelist,
			expectedTable: "products p",
			expectedError: nil,
		},
		{
			name:          "should return error for invalid user input",
			userInput:     "invalid_table",
			whitelist:     whitelist,
			expectedTable: "",
			expectedError: ErrInvalidTableInput,
		},
		{
			name:          "should return error for empty user input",
			userInput:     "",
			whitelist:     whitelist,
			expectedTable: "",
			expectedError: ErrInvalidTableInput,
		},
		{
			name:          "should return error for empty whitelist",
			userInput:     "users",
			whitelist:     map[string]string{},
			expectedTable: "",
			expectedError: ErrInvalidTableInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{}

			// Act
			result := b.FromSafe(tt.userInput, tt.whitelist)

			// Assert
			if tt.expectedError != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedError, "expected error message to match")
				return
			}

			assert.NoError(t, b.err, "expected no error")
			assert.Equal(t, tt.expectedTable, b.table, "expected table to be set correctly")
			assert.Equal(t, b, result, "expected FromSafe() to return the same builder instance")
		})
	}
}

// -----------------
// --- BENCHMARK ---
// -----------------

func BenchmarkBuilder_From(b *testing.B) {
	builder := &builder{}
	table := "users"

	for b.Loop() {
		builder.From(table)
	}
}

func BenchmarkBuilder_FromSafe(b *testing.B) {
	builder := &builder{}
	userInput := "users"
	whitelist := map[string]string{
		"users":  "users",
		"orders": "public.orders",
	}

	for b.Loop() {
		builder.FromSafe(userInput, whitelist)
	}
}
