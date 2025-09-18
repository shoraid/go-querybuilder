package goquerybuilder

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
		initialColumns  []column
		expectedColumns []string
	}{
		{
			name: "should return basic columns",
			initialColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
			expectedColumns: []string{"id", "name"},
		},
		{
			name: "should return raw columns (expressions)",
			initialColumns: []column{
				{queryType: QueryRaw, expr: "COUNT(id) AS total"},
				{queryType: QueryRaw, expr: "MAX(created_at)"},
			},
			expectedColumns: []string{"COUNT(id) AS total", "MAX(created_at)"},
		},
		{
			name: "should return mixed basic and raw columns",
			initialColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryRaw, expr: "COUNT(id) AS total"},
				{queryType: QueryBasic, name: "name"},
			},
			expectedColumns: []string{"id", "COUNT(id) AS total", "name"},
		},
		{
			name:            "should return empty slice for empty columns",
			initialColumns:  []column{},
			expectedColumns: []string{},
		},
		{
			name:            "should return nil for nil columns",
			initialColumns:  nil,
			expectedColumns: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{columns: tt.initialColumns}

			// Act
			result := b.GetColumns()

			// Assert
			assert.Equal(t, tt.expectedColumns, result, "expected columns to match")
		})
	}
}

// func TestBuilder_GetColumns(t *testing.T) {
// 	t.Parallel()

// 	tests := []struct {
// 		name            string
// 		columns         []string
// 		expectedColumns []string
// 	}{
// 		{
// 			name:            "should return columns",
// 			columns:         []string{"id", "name"},
// 			expectedColumns: []string{"id", "name"},
// 		},
// 		{
// 			name:            "should return empty columns",
// 			columns:         []string{},
// 			expectedColumns: []string{},
// 		}, {
// 			name:            "should return nil columns",
// 			columns:         nil,
// 			expectedColumns: nil,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			t.Parallel()

// 			// Arrange
// 			b := &builder{columns: tt.columns}

// 			// Act
// 			result := b.GetColumns()

// 			// Assert
// 			assert.Equal(t, tt.expectedColumns, result, "expected columns to match")
// 		})
// 	}
// }

func TestBuilder_Args(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         []any
		expectedArgs []any
	}{
		{
			name:         "should return args",
			args:         []any{1, "test"},
			expectedArgs: []any{1, "test"},
		},
		{
			name:         "should return empty args",
			args:         []any{},
			expectedArgs: []any{},
		}, {
			name:         "should return nil args",
			args:         nil,
			expectedArgs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{args: tt.args}

			// Act
			result := b.Args()

			// Assert
			assert.Equal(t, tt.expectedArgs, result, "expected args to match")
		})
	}
}

func TestBuilder_ArgsByIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialArgs    []any
		indexes        []int
		expectedResult []any
	}{
		{
			name:           "should return args for valid single index",
			initialArgs:    []any{1, "test", true},
			indexes:        []int{1},
			expectedResult: []any{"test"},
		},
		{
			name:           "should return args for valid multiple indexes",
			initialArgs:    []any{1, "test", true, 3.14},
			indexes:        []int{0, 2},
			expectedResult: []any{1, true},
		},
		{
			name:           "should return empty slice for empty indexes",
			initialArgs:    []any{1, "test"},
			indexes:        []int{},
			expectedResult: []any{},
		},
		{
			name:           "should return empty slice for out of bounds index",
			initialArgs:    []any{1, "test"},
			indexes:        []int{2},
			expectedResult: []any{},
		},
		{
			name:           "should return empty slice for negative index",
			initialArgs:    []any{1, "test"},
			indexes:        []int{-1},
			expectedResult: []any{},
		},
		{
			name:           "should handle mixed valid and invalid indexes",
			initialArgs:    []any{1, "test", true},
			indexes:        []int{0, 3, 1, -1},
			expectedResult: []any{1, "test"},
		},
		{
			name:           "should return empty slice when initial args are empty",
			initialArgs:    []any{},
			indexes:        []int{0},
			expectedResult: []any{},
		},
		{
			name:           "should return empty slice when initial args are nil",
			initialArgs:    nil,
			indexes:        []int{0},
			expectedResult: []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{args: tt.initialArgs}

			// Act
			result := b.ArgsByIndexes(tt.indexes...)

			// Assert
			assert.Equal(t, tt.expectedResult, result, "expected args to match for given indexes")
		})
	}
}

func TestBuilder_AddArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialArgs  []any
		argsToAdd    []any
		expectedArgs []any
	}{
		{
			name:         "should add single arg to empty list",
			initialArgs:  []any{},
			argsToAdd:    []any{1},
			expectedArgs: []any{1},
		},
		{
			name:         "should add multiple args to empty list",
			initialArgs:  []any{},
			argsToAdd:    []any{1, "test"},
			expectedArgs: []any{1, "test"},
		},
		{
			name:         "should add single arg to existing list",
			initialArgs:  []any{10},
			argsToAdd:    []any{"hello"},
			expectedArgs: []any{10, "hello"},
		},
		{
			name:         "should add multiple args to existing list",
			initialArgs:  []any{10, "world"},
			argsToAdd:    []any{true, 3.14},
			expectedArgs: []any{10, "world", true, 3.14},
		},
		{
			name:         "should handle adding no args",
			initialArgs:  []any{1, 2},
			argsToAdd:    []any{},
			expectedArgs: []any{1, 2},
		},
		{
			name:         "should handle adding to nil list",
			initialArgs:  nil,
			argsToAdd:    []any{"first"},
			expectedArgs: []any{"first"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{args: tt.initialArgs}

			// Act
			b.AddArgs(tt.argsToAdd...)

			// Assert
			assert.Equal(t, tt.expectedArgs, b.args, "expected args to be updated correctly")
		})
	}
}
