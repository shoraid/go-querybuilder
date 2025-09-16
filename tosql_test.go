package goquerybuilder

import (
	"testing"

	"github.com/shoraid/go-querybuilder/dialect"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_ToSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		action        string
		table         string
		columns       []string
		expectedSQL   string
		expectedError string
	}{
		{
			name:          "should return error when table is empty",
			action:        "select",
			table:         "",
			expectedSQL:   "",
			expectedError: "no table specified",
		},
		{
			name:        "should build select all query when columns are empty",
			action:      "select",
			table:       "users",
			columns:     []string{},
			expectedSQL: `SELECT * FROM "users"`,
		},
		{
			name:        "should build select with single column",
			action:      "select",
			table:       "users",
			columns:     []string{"id"},
			expectedSQL: `SELECT "id" FROM "users"`,
		},
		{
			name:        "should build select with multiple columns",
			action:      "select",
			table:       "users",
			columns:     []string{"id", "name", "email"},
			expectedSQL: `SELECT "id", "name", "email" FROM "users"`,
		},
		{
			name:        "should build select with table alias",
			action:      "select",
			table:       "users u",
			columns:     []string{"u.id", "u.name"},
			expectedSQL: `SELECT "u"."id", "u"."name" FROM "users" AS u`,
		},
		{
			name:          "should return error on unsupported action",
			action:        "drop",
			table:         "users",
			expectedSQL:   "",
			expectedError: "unsupported action: drop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{
				dialect: dialect.PostgresDialect{}, // use Postgres for quoting
				action:  tt.action,
				table:   tt.table,
				columns: tt.columns,
			}

			// Act
			sql, args, err := b.ToSQL()

			// Assert
			if tt.expectedError != "" {
				assert.Error(t, err, "expected an error")
				assert.Contains(t, err.Error(), tt.expectedError, "error message should match")
				assert.Empty(t, sql, "SQL should be empty on error")
				assert.Empty(t, args, "Args should be empty on error")
				return
			}

			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tt.expectedSQL, sql, "SQL output should match")
			assert.Empty(t, args, "expected no args for simple SELECT")
		})
	}
}
