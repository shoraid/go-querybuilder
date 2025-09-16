package goquerybuilder

import (
	"testing"

	"github.com/shoraid/go-querybuilder/dialect"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		columns        []string
		expectedAction string
		expectedCols   []string
	}{
		{
			name:           "should set single column",
			columns:        []string{"id"},
			expectedAction: "select",
			expectedCols:   []string{"id"},
		},
		{
			name:           "should set multiple columns",
			columns:        []string{"id", "name", "email"},
			expectedAction: "select",
			expectedCols:   []string{"id", "name", "email"},
		},
		{
			name:           "should reset columns when called again",
			columns:        []string{"username"},
			expectedAction: "select",
			expectedCols:   []string{"username"},
		},
		{
			name:           "should handle empty column list (select all)",
			columns:        []string{},
			expectedAction: "select",
			expectedCols:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: dialect.PostgresDialect{}}
			// Pre-fill with some data to check reset behavior
			b.columns = []string{"dummy"}

			// Act
			result := b.Select(tt.columns...)

			// Assert
			assert.Equal(t, tt.expectedAction, b.action, "expected action to be set to select")
			assert.Equal(t, tt.expectedCols, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected Select to return the same builder instance")
		})
	}
}

func TestBuilder_AddSelect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialColumns []string
		addColumn      string
		expectedCols   []string
	}{
		{
			name:           "should add a single column to empty list",
			initialColumns: []string{},
			addColumn:      "id",
			expectedCols:   []string{"id"},
		},
		{
			name:           "should add a single column to existing list",
			initialColumns: []string{"id", "name"},
			addColumn:      "email",
			expectedCols:   []string{"id", "name", "email"},
		},
		{
			name:           "should not add duplicate column",
			initialColumns: []string{"id", "name"},
			addColumn:      "id",
			expectedCols:   []string{"id", "name"},
		},
		{
			name:           "should handle adding to nil column list",
			initialColumns: nil,
			addColumn:      "id",
			expectedCols:   []string{"id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: dialect.PostgresDialect{}, columns: tt.initialColumns}

			// Act
			result := b.AddSelect(tt.addColumn)

			// Assert
			assert.Equal(t, tt.expectedCols, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected AddSelect to return the same builder instance")
		})
	}
}
