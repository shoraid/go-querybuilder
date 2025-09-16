package goquerybuilder

import (
	"testing"

	"github.com/shoraid/go-querybuilder/dialect"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_New(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect dialect.Dialect
	}{
		{
			name:    "should create builder with Postgres dialect",
			dialect: dialect.PostgresDialect{},
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

func TestBuilder_GetDialect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		dialect         dialect.Dialect
		expectedDialect dialect.Dialect
	}{
		{
			name:            "should return Postgres dialect",
			dialect:         dialect.PostgresDialect{},
			expectedDialect: dialect.PostgresDialect{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: tt.dialect}

			// Act
			result := b.GetDialect()

			// Assert
			assert.Equal(t, tt.expectedDialect, result, "expected dialect to match")
		})
	}
}

func TestBuilder_GetAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		action         string
		expectedAction string
	}{
		{
			name:           "should return select action",
			action:         "select",
			expectedAction: "select",
		},
		{
			name:           "should return empty action",
			action:         "",
			expectedAction: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{action: tt.action}

			// Act
			result := b.GetAction()

			// Assert
			assert.Equal(t, tt.expectedAction, result, "expected action to match")
		})
	}
}

func TestBuilder_GetTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		table         string
		expectedTable string
	}{
		{
			name:          "should return table name",
			table:         "users",
			expectedTable: "users",
		},
		{
			name:          "should return empty table name",
			table:         "",
			expectedTable: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{table: tt.table}

			// Act
			result := b.GetTable()

			// Assert
			assert.Equal(t, tt.expectedTable, result, "expected table to match")
		})
	}
}

func TestBuilder_GetColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		columns         []string
		expectedColumns []string
	}{
		{
			name:            "should return columns",
			columns:         []string{"id", "name"},
			expectedColumns: []string{"id", "name"},
		},
		{
			name:            "should return empty columns",
			columns:         []string{},
			expectedColumns: []string{},
		}, {
			name:            "should return nil columns",
			columns:         nil,
			expectedColumns: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{columns: tt.columns}

			// Act
			result := b.GetColumns()

			// Assert
			assert.Equal(t, tt.expectedColumns, result, "expected columns to match")
		})
	}
}
