package sequel

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
		expectedError error
	}{
		{
			name: "should return select SQL when action is select",
			builder: builder{
				dialect: PostgresDialect{},
				action:  "select",
				table: table{
					queryType: QueryBasic,
					name:      "users",
				},
				limit:  -1,
				offset: -1,
				wheres: []where{
					{queryType: QueryBasic, column: "id", operator: "=", args: []any{1}},
				},
			},
			expectedSQL:  `SELECT * FROM "users" WHERE "id" = $1`,
			expectedArgs: []any{1},
		},
		{
			name: "should return error when action is unsupported",
			builder: builder{
				dialect: PostgresDialect{},
				action:  "update", // unsupported
				table: table{
					queryType: QueryBasic,
					name:      "users",
				},
			},
			expectedError: ErrUnsupportedAction,
		},
		{
			name: "should return error when dialect is nil",
			builder: builder{
				dialect: nil, // nil dialect
				action:  "select",
				table: table{
					queryType: QueryBasic,
					name:      "users",
				},
			},
			expectedError: ErrNoDialect,
		},
		{
			name: "should return error when nested where has an error",
			builder: builder{
				dialect: PostgresDialect{},
				action:  "select",
				table: table{
					queryType: QueryBasic,
					name:      "users",
				},
				limit:  -1,
				offset: -1,
				wheres: []where{
					{
						queryType: QueryNested,
						conj:      "AND",
						nested: []where{
							{queryType: QueryBasic, conj: "AND", column: "status", operator: "=", args: []any{"active"}},
							{
								queryType: QuerySub, conj: "AND", column: "", operator: "EXISTS", args: []any{}, sub: &builder{
									dialect: PostgresDialect{},
									action:  "select",
									table: table{
										queryType: QueryBasic,
										name:      "orders",
									},
									limit:  -1,
									offset: -1,
									err:    ErrEmptyColumn, // pre-existing error
									wheres: []where{
										{queryType: QueryIn, column: "", operator: "IN", args: []any{1, 2, 3}}, // This will cause ErrEmptyColumn
									},
								},
							},
						},
					},
				},
			},
			expectedError: ErrEmptyColumn,
		},
		{
			name: "should return error from select subquery if present",
			builder: builder{
				dialect: PostgresDialect{},
				action:  "select",
				columns: []column{
					{
						queryType: QuerySub, name: "order_count", args: []any{}, sub: &builder{
							dialect: PostgresDialect{},
							action:  "select",
							table:   table{}, // This will cause ErrEmptyTable
							limit:   -1,
							offset:  -1,
							err:     ErrEmptyTable, // pre-existing error
						},
					},
				},
			},
			expectedError: ErrEmptyTable,
		},
		{
			name: "should return error from table subquery if present",
			builder: builder{
				dialect: PostgresDialect{},
				action:  "select",
				table: table{
					queryType: QuerySub,
					name:      "users",
					sub: &builder{
						dialect: PostgresDialect{},
						action:  "select",
						table:   table{}, // This will cause ErrEmptyTable
						limit:   -1,
						offset:  -1,
						err:     ErrEmptyTable, // pre-existing error
					},
				},
				limit:  -1,
				offset: -1,
			},
			expectedError: ErrEmptyTable,
		},
		{
			name: "should return error from where subquery if present",
			builder: builder{
				dialect: PostgresDialect{},
				action:  "select",
				wheres: []where{
					{
						queryType: QuerySub, column: "", operator: "EXISTS", args: []any{}, sub: &builder{
							dialect: PostgresDialect{},
							action:  "select",
							table:   table{}, // This will cause ErrEmptyTable
							limit:   -1,
							offset:  -1,
							err:     ErrEmptyTable, // pre-existing error
						},
					},
				},
			},
			expectedError: ErrEmptyTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sql, args, err := tt.builder.ToSQL()

			if tt.expectedError != nil {
				assert.Error(t, err, "expected an error")
				assert.ErrorIs(t, err, tt.expectedError, "expected error message to match")
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSQL, sql, "expected SQL to match")
			assert.Equal(t, tt.expectedArgs, args, "expected args to match")
		})
	}
}
