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

func TestBuilder_SelectSafe(t *testing.T) {
	t.Parallel()

	whitelist := map[string]string{
		"id":    "id",
		"name":  "name",
		"email": "u.email",
		"age":   "age",
	}

	tests := []struct {
		name          string
		userInput     []string
		whitelist     map[string]string
		expectedCols  []string
		expectedError string
	}{
		{
			name:          "should set valid columns from whitelist",
			userInput:     []string{"id", "name"},
			whitelist:     whitelist,
			expectedCols:  []string{"id", "name"},
			expectedError: "",
		},
		{
			name:          "should handle column with alias from whitelist",
			userInput:     []string{"id", "email"},
			whitelist:     whitelist,
			expectedCols:  []string{"id", "u.email"},
			expectedError: "",
		},
		{
			name:          "should handle empty user input (select all)",
			userInput:     []string{},
			whitelist:     whitelist,
			expectedCols:  []string{},
			expectedError: "",
		},
		{
			name:          "should return error for invalid column",
			userInput:     []string{"id", "invalid_col"},
			whitelist:     whitelist,
			expectedCols:  nil,
			expectedError: "invalid column: invalid_col",
		},
		{
			name:          "should return error for empty whitelist",
			userInput:     []string{"id"},
			whitelist:     map[string]string{},
			expectedCols:  nil,
			expectedError: "invalid column: id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: dialect.PostgresDialect{}}

			// Act
			result, err := b.SelectSafe(tt.userInput, tt.whitelist)

			// Assert
			if tt.expectedError != "" {
				assert.Error(t, err, "expected an error")
				assert.Contains(t, err.Error(), tt.expectedError, "error message should match")
				assert.Nil(t, result, "expected nil builder on error")
				assert.Empty(t, b.columns, "expected columns to be empty on error")
				return
			}

			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedCols, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected SelectSafe to return the same builder instance")
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

func TestBuilder_AddSelectSafe(t *testing.T) {
	t.Parallel()

	whitelist := map[string]string{
		"id":    "id",
		"name":  "name",
		"email": "u.email",
		"age":   "age",
	}

	tests := []struct {
		name           string
		initialColumns []string
		userInput      string
		whitelist      map[string]string
		expectedCols   []string
		expectedError  string
	}{
		{
			name:           "should add valid column from whitelist to empty list",
			initialColumns: []string{},
			userInput:      "id",
			whitelist:      whitelist,
			expectedCols:   []string{"id"},
			expectedError:  "",
		},
		{
			name:           "should add valid column from whitelist to existing list",
			initialColumns: []string{"id", "name"},
			userInput:      "email",
			whitelist:      whitelist,
			expectedCols:   []string{"id", "name", "u.email"},
			expectedError:  "",
		},
		{
			name:           "should not add duplicate column from whitelist",
			initialColumns: []string{"id", "name"},
			userInput:      "id",
			whitelist:      whitelist,
			expectedCols:   []string{"id", "name"},
			expectedError:  "",
		},
		{
			name:           "should return error for invalid column",
			initialColumns: []string{"id"},
			userInput:      "invalid_col",
			whitelist:      whitelist,
			expectedCols:   []string{"id"}, // Columns should remain unchanged on error
			expectedError:  "invalid column: invalid_col",
		},
		{
			name:           "should return error for empty whitelist",
			initialColumns: []string{"id"},
			userInput:      "name",
			whitelist:      map[string]string{},
			expectedCols:   []string{"id"}, // Columns should remain unchanged on error
			expectedError:  "invalid column: name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{dialect: dialect.PostgresDialect{}, columns: tt.initialColumns}

			// Act
			result, err := b.AddSelectSafe(tt.userInput, tt.whitelist)

			// Assert
			if tt.expectedError != "" {
				assert.Error(t, err, "expected an error")
				assert.Contains(t, err.Error(), tt.expectedError, "error message should match")
				assert.Nil(t, result, "expected nil builder on error")
				assert.Equal(t, tt.expectedCols, b.columns, "expected columns to remain unchanged on error")
				return
			}

			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedCols, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected AddSelectSafe to return the same builder instance")
		})
	}
}
