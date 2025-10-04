package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		initialColumns  []column
		columns         []string
		expectedAction  string
		expectedColumns []column
	}{
		{
			name:            "should set single column",
			columns:         []string{"id"},
			expectedAction:  "select",
			expectedColumns: []column{{queryType: QueryBasic, name: "id"}},
		},
		{
			name:           "should set multiple columns",
			columns:        []string{"id", "name", "email"},
			expectedAction: "select",
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
				{queryType: QueryBasic, name: "email"},
			},
		},
		{
			name:            "should reset columns when called again",
			initialColumns:  []column{{queryType: QueryBasic, name: "dummy"}},
			columns:         []string{"username"},
			expectedAction:  "select",
			expectedColumns: []column{{queryType: QueryBasic, name: "username"}},
		},
		{
			name:            "should handle empty column list (select all)",
			columns:         []string{},
			expectedAction:  "select",
			expectedColumns: []column{},
		},
		{
			name:            "should handle nil column list",
			columns:         nil,
			expectedAction:  "select",
			expectedColumns: []column{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{columns: tt.initialColumns}

			// Act
			result := b.Select(tt.columns...)

			// Assert
			assert.Equal(t, tt.expectedAction, b.action, "expected action to be set to select")
			assert.Equal(t, tt.expectedColumns, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected Select to return the same builder instance")
		})
	}
}

func TestBuilder_SelectRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		initialColumns  []column
		expression      string
		args            []any
		expectedAction  string
		expectedColumns []column
		expectedErr     error
	}{
		{
			name:           "should set single raw expression",
			expression:     "COUNT(*) as total",
			args:           []any{},
			expectedAction: "select",
			expectedColumns: []column{{
				queryType: QueryRaw, expr: "COUNT(*) as total", args: []any{},
			}},
		},
		{
			name:           "should set raw expression with arguments",
			expression:     "CASE WHEN status = ? THEN 'Active' ELSE 'Inactive' END",
			args:           []any{"active"},
			expectedAction: "select",
			expectedColumns: []column{{
				queryType: QueryRaw, expr: "CASE WHEN status = ? THEN 'Active' ELSE 'Inactive' END", args: []any{"active"}},
			},
		},
		{
			name: "should reset columns when called again with raw expression",
			initialColumns: []column{
				{queryType: QueryBasic, name: "dummy"},
			},
			expression:     "SUM(amount)",
			args:           []any{},
			expectedAction: "select",
			expectedColumns: []column{{
				queryType: QueryRaw, expr: "SUM(amount)", args: []any{}},
			},
		},
		{
			name:            "should return error for empty expression",
			expression:      "",
			args:            []any{},
			expectedAction:  "select",
			expectedColumns: []column{},
			expectedErr:     ErrEmptyExpression,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{columns: tt.initialColumns}

			// Act
			result := b.SelectRaw(tt.expression, tt.args...)

			// Assert
			if tt.expectedErr != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedErr, "expected error message to match")
				return
			}

			assert.NoError(t, b.err)
			assert.Equal(t, tt.expectedAction, b.action, "expected action to be set to select")
			assert.Equal(t, tt.expectedColumns, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected SelectRaw to return the same builder instance")
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
		name            string
		initialColumns  []column
		userInput       []string
		whitelist       map[string]string
		expectedAction  string
		expectedColumns []column
	}{
		{
			name:           "should set valid columns from whitelist",
			userInput:      []string{"id", "name"},
			whitelist:      whitelist,
			expectedAction: "select",
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
		},
		{
			name:           "should handle column with alias from whitelist",
			userInput:      []string{"id", "email"},
			whitelist:      whitelist,
			expectedAction: "select",
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "u.email"},
			},
		},
		{
			name:            "should handle empty user input (select all)",
			userInput:       []string{},
			whitelist:       whitelist,
			expectedAction:  "select",
			expectedColumns: []column{},
		},
		{
			name:           "should filter out invalid column and include valid ones",
			userInput:      []string{"id", "invalid_col", "name"},
			whitelist:      whitelist,
			expectedAction: "select",
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
		},
		{
			name:      "should return empty columns for all invalid input",
			userInput: []string{"invalid_col1", "invalid_col2"},
			whitelist: whitelist, expectedAction: "select",
			expectedColumns: []column{},
		},
		{
			name: "should reset columns when called again",
			initialColumns: []column{
				{queryType: QueryBasic, name: "dummy"},
			},
			userInput:      []string{"id", "name"},
			whitelist:      whitelist,
			expectedAction: "select",
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{columns: tt.initialColumns}

			// Act
			result := b.SelectSafe(tt.userInput, tt.whitelist)

			// Assert
			assert.Equal(t, tt.expectedAction, b.action, "expected action to be set to select")
			assert.Equal(t, tt.expectedColumns, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected SelectSafe to return the same builder instance")
		})
	}
}

func TestBuilder_AddSelect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		initialColumns  []column
		addColumns      []string
		expectedColumns []column
	}{
		{
			name:            "should add a single column to empty list",
			initialColumns:  []column{},
			addColumns:      []string{"id"},
			expectedColumns: []column{{queryType: QueryBasic, name: "id"}},
		},
		{
			name:           "should add multiple columns to empty list",
			initialColumns: []column{},
			addColumns:     []string{"id", "name"},
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
		},
		{
			name:            "should not add any columns if addColumns is empty",
			initialColumns:  []column{{queryType: QueryBasic, name: "id"}},
			addColumns:      []string{},
			expectedColumns: []column{{queryType: QueryBasic, name: "id"}},
		},
		{
			name: "should add a single column to existing list",
			initialColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
			addColumns: []string{"email"},
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
				{queryType: QueryBasic, name: "email"},
			},
		},
		{
			name: "should not add duplicate column",
			initialColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
			addColumns: []string{"id"},
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
		},
		{
			name:            "should handle adding to nil column list",
			initialColumns:  nil,
			addColumns:      []string{"id"},
			expectedColumns: []column{{queryType: QueryBasic, name: "id"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{columns: tt.initialColumns}

			// Act
			result := b.AddSelect(tt.addColumns...)

			// Assert
			assert.Equal(t, tt.expectedColumns, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected AddSelect to return the same builder instance")
		})
	}
}

func TestBuilder_AddSelectRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		initialColumns  []column
		expression      string
		args            []any
		expectedColumns []column
		expectedErr     error
	}{
		{
			name:           "should add a single raw expression to empty list",
			initialColumns: []column{},
			expression:     "COUNT(*) as total",
			args:           []any{},
			expectedColumns: []column{{
				queryType: QueryRaw,
				expr:      "COUNT(*) as total",
				args:      []any{},
			}},
		},
		{
			name: "should add a raw expression to existing list",
			initialColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
			expression: "SUM(amount) as total_amount",
			args:       []any{},
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
				{queryType: QueryRaw, expr: "SUM(amount) as total_amount", args: []any{}},
			},
		},
		{
			name:           "should add raw expression with arguments",
			initialColumns: []column{},
			expression:     "CASE WHEN status = ? THEN 'Active' ELSE 'Inactive' END",
			args:           []any{"active"},
			expectedColumns: []column{{
				queryType: QueryRaw, expr: "CASE WHEN status = ? THEN 'Active' ELSE 'Inactive' END", args: []any{"active"}},
			},
		},
		{
			name:            "should handle adding to nil column list",
			initialColumns:  nil,
			expression:      "MAX(created_at)",
			args:            []any{},
			expectedColumns: []column{{queryType: QueryRaw, expr: "MAX(created_at)", args: []any{}}},
		},
		{
			name:            "should return error for empty expression",
			initialColumns:  []column{},
			expression:      "",
			args:            []any{},
			expectedColumns: []column{},
			expectedErr:     ErrEmptyExpression,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{columns: tt.initialColumns}

			// Act
			result := b.AddSelectRaw(tt.expression, tt.args...)

			// Assert
			if tt.expectedErr != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedErr, "expected error message to match")
				return
			}

			assert.NoError(t, b.err)
			assert.Equal(t, tt.expectedColumns, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected AddSelectRaw to return the same builder instance")
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
		name            string
		initialColumns  []column
		userInput       []string
		whitelist       map[string]string
		expectedColumns []column
	}{
		{
			name:            "should add valid column from whitelist to empty list",
			initialColumns:  []column{},
			userInput:       []string{"id"},
			whitelist:       whitelist,
			expectedColumns: []column{{queryType: QueryBasic, name: "id"}},
		},
		{
			name:           "should add multiple valid columns from whitelist to empty list",
			initialColumns: []column{},
			userInput:      []string{"id", "name"},
			whitelist:      whitelist,
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
		},
		{
			name: "should add valid column from whitelist to existing list",
			initialColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
			userInput: []string{"email"},
			whitelist: whitelist,
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
				{queryType: QueryBasic, name: "u.email"},
			},
		},
		{
			name: "should not add duplicate column from whitelist",
			initialColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
			userInput: []string{"id"},
			whitelist: whitelist,
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
		},
		{
			name:           "should filter out invalid column and add valid ones to existing list",
			initialColumns: []column{{queryType: QueryBasic, name: "id"}},
			userInput:      []string{"invalid_col", "name"},
			whitelist:      whitelist,
			expectedColumns: []column{
				{queryType: QueryBasic, name: "id"},
				{queryType: QueryBasic, name: "name"},
			},
		},
		{
			name:            "should handle adding to nil column list",
			initialColumns:  nil,
			userInput:       []string{"id"},
			whitelist:       whitelist,
			expectedColumns: []column{{queryType: QueryBasic, name: "id"}},
		},
		{
			name:            "should not add any columns if all user input is invalid",
			initialColumns:  []column{{queryType: QueryBasic, name: "id"}},
			userInput:       []string{"invalid1", "invalid2"},
			whitelist:       whitelist,
			expectedColumns: []column{{queryType: QueryBasic, name: "id"}},
		},
		{
			name:            "should handle empty user input",
			initialColumns:  []column{{queryType: QueryBasic, name: "id"}},
			userInput:       []string{},
			whitelist:       whitelist,
			expectedColumns: []column{{queryType: QueryBasic, name: "id"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{columns: tt.initialColumns}

			// Act
			result := b.AddSelectSafe(tt.userInput, tt.whitelist)

			// Assert
			assert.Equal(t, tt.expectedColumns, b.columns, "expected columns to be updated correctly")
			assert.Equal(t, b, result, "expected AddSelectSafe to return the same builder instance")
		})
	}
}

//------------------
// --- BENCHMARK ---
//------------------

func BenchmarkBuilder_Select(b *testing.B) {
	builder := &builder{}
	columns := []string{
		"id",
		"name",
		"email",
		"created_at",
		"updated_at",
		"last_login",
		"status",
		"role",
		"age",
		"address",
	}

	for b.Loop() {
		builder.Select(columns...)
	}
}

func BenchmarkBuilder_SelectRaw(b *testing.B) {
	builder := &builder{}
	expression := "COUNT(*) as total"
	args := []any{}

	for b.Loop() {
		builder.SelectRaw(expression, args...)
	}
}

func BenchmarkBuilder_SelectSafe(b *testing.B) {
	builder := &builder{}
	userInput := []string{
		"id",
		"name",
		"email",
		"created_at",
		"updated_at",
		"last_login",
		"status",
		"role",
		"age",
		"address",
		"invalid_col1",
		"invalid_col2",
	}
	whitelist := map[string]string{
		"id":         "id",
		"name":       "name",
		"email":      "email",
		"created_at": "created_at",
		"updated_at": "updated_at",
		"last_login": "last_login",
		"status":     "status",
		"role":       "role",
		"age":        "age",
		"address":    "address",
	}

	for b.Loop() {
		builder.SelectSafe(userInput, whitelist)
	}
}

func BenchmarkBuilder_AddSelect(b *testing.B) {
	initialColumns := []column{
		{queryType: QueryBasic, name: "id"},
		{queryType: QueryBasic, name: "name"},
		{queryType: QueryBasic, name: "email"},
	}
	builder := &builder{columns: initialColumns}

	addColumns := []string{
		"created_at",
		"updated_at",
		"last_login",
		"status",
		"role",
		"age",
		"address",
	}

	for b.Loop() {
		builder.AddSelect(addColumns...)
	}
}

func BenchmarkBuilder_AddSelectRaw(b *testing.B) {
	initialColumns := []column{
		{queryType: QueryBasic, name: "id"},
		{queryType: QueryBasic, name: "name"},
		{queryType: QueryBasic, name: "email"},
	}
	builder := &builder{columns: initialColumns}

	expression := "SUM(amount) as total_amount"
	args := []any{}

	for b.Loop() {
		builder.AddSelectRaw(expression, args...)
	}
}

func BenchmarkBuilder_AddSelectSafe(b *testing.B) {
	initialColumns := []column{
		{queryType: QueryBasic, name: "id"},
		{queryType: QueryBasic, name: "name"},
		{queryType: QueryBasic, name: "email"},
	}
	builder := &builder{columns: initialColumns}

	userInput := []string{
		"created_at",
		"updated_at",
		"last_login",
		"status",
		"role",
		"age",
		"address",
		"invalid_col1",
		"invalid_col2",
	}
	whitelist := map[string]string{
		"id":         "id",
		"name":       "name",
		"email":      "email",
		"created_at": "created_at",
		"updated_at": "updated_at",
		"last_login": "last_login",
		"status":     "status",
		"role":       "role",
		"age":        "age",
		"address":    "address",
	}

	for b.Loop() {
		builder.AddSelectSafe(userInput, whitelist)
	}
}
