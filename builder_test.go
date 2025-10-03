package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_New(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect Dialect
	}{
		{
			name:    "should create builder with Postgres dialect",
			dialect: PostgresDialect{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			q := New(tt.dialect)
			assert.NotNil(t, q, "expected New() to return non-nil")

			// Assert
			b, ok := q.(*builder) // Type assert back to *builder so we can inspect fields
			assert.True(t, ok, "expected New() to return *builder type")

			assert.Equal(t, tt.dialect, b.dialect, "expected dialect to be set correctly")
			assert.Empty(t, b.action, "expected action to be empty by default")
			assert.Empty(t, b.table, "expected table to be empty by default")
			assert.Empty(t, b.columns, "expected columns to be empty by default")
		})
	}
}

func TestBuilder_Dialect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		dialect         Dialect
		expectedDialect Dialect
	}{
		{
			name:            "should return Postgres dialect",
			dialect:         PostgresDialect{},
			expectedDialect: PostgresDialect{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: tt.dialect}

			// Act
			result := b.Dialect()

			// Assert
			assert.Equal(t, tt.expectedDialect, result, "expected dialect to match")
		})
	}
}
