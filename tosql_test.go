package goquerybuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_ToSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		builder       builder
		expectedSQL   string
		expectedArgs  []any
		expectedError string
	}{
		{
			name: "should return select SQL when action is select",
			builder: builder{
				dialect: PostgresDialect{},
				action:  "select",
				table:   "users",
				wheres: []where{
					{queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
				},
				limit:  -1,
				offset: -1,
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1`,
			expectedArgs: []any{1},
		},
		{
			name: "should return error when action is unsupported",
			builder: builder{
				dialect: PostgresDialect{},
				action:  "update", // unsupported
				table:   "users",
			},
			expectedError: "unsupported action: update",
		},
		{
			name: "should return error when dialect is nil",
			builder: builder{
				dialect: nil, // nil dialect
				action:  "select",
				table:   "users",
			},
			expectedError: "no dialect specified for builder",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sql, args, err := tt.builder.ToSQL()

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Empty(t, sql)
				assert.Empty(t, args)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match")
			assert.Equal(t, tt.expectedArgs, args, "expected args to match")
		})
	}
}
