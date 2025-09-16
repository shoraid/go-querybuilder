package goquerybuilder

import (
	"testing"

	"github.com/shoraid/go-querybuilder/dialect"
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
			b := &builder{dialect: dialect.PostgresDialect{}}

			// Act
			result := b.From(tt.table)

			// Assert
			assert.Equal(t, tt.table, b.table, "expected table to be set correctly")
			assert.Equal(t, b, result, "expected From() to return the same builder instance")
		})
	}
}
