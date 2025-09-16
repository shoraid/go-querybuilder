package goquerybuilder

import (
	"testing"

	"github.com/shoraid/go-querybuilder/dialect"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		limit    int
		expected int
	}{
		{
			name:     "should set limit correctly",
			limit:    10,
			expected: 10,
		},
		{
			name:     "should set limit to 0 if negative",
			limit:    -5,
			expected: 0,
		},
		{
			name:     "should set limit to 0",
			limit:    0,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: dialect.PostgresDialect{}}

			// Act
			result := b.Limit(tt.limit)

			// Assert
			assert.Equal(t, tt.expected, b.limit, "expected limit to be set correctly")
			assert.Equal(t, b, result, "expected Limit() to return the same builder instance")
		})
	}
}
