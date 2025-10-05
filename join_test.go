package sequel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Join(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialJoins  []join
		table         string
		leftCol       string
		operator      string
		rightCol      string
		expectedJoins []join
		expectedErr   error
	}{
		{
			name:     "should add a simple JOIN clause",
			table:    "orders",
			leftCol:  "users.id",
			operator: "=",
			rightCol: "orders.user_id",
			expectedJoins: []join{
				{queryType: QueryBasic, joinType: "INNER JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
			},
		},
		{
			name: "should add multiple JOIN clauses",
			initialJoins: []join{
				{queryType: QueryBasic, joinType: "INNER JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
			},
			table:    "products",
			leftCol:  "orders.product_id",
			operator: "=",
			rightCol: "products.id",
			expectedJoins: []join{
				{queryType: QueryBasic, joinType: "INNER JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
				{queryType: QueryBasic, joinType: "INNER JOIN", table: "products", leftCol: "orders.product_id", operator: "=", rightCol: "products.id"},
			},
		},
		{
			name:        "should return an error if table is empty",
			table:       "",
			leftCol:     "users.id",
			operator:    "=",
			rightCol:    "orders.user_id",
			expectedErr: ErrEmptyTable,
		},
		{
			name:        "should return an error if left column is empty",
			table:       "orders",
			leftCol:     "",
			operator:    "=",
			rightCol:    "orders.user_id",
			expectedErr: ErrInvalidJoinCondition,
		},
		{
			name:        "should return an error if operator is empty",
			table:       "orders",
			leftCol:     "users.id",
			operator:    "",
			rightCol:    "orders.user_id",
			expectedErr: ErrInvalidJoinCondition,
		},
		{
			name:        "should return an error if right column is empty",
			table:       "orders",
			leftCol:     "users.id",
			operator:    "=",
			rightCol:    "",
			expectedErr: ErrInvalidJoinCondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &builder{joins: tt.initialJoins}
			result := b.Join(tt.table, tt.leftCol, tt.operator, tt.rightCol)

			if tt.expectedErr != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedErr, "expected error message to match")
			} else {
				assert.NoError(t, b.err, "expected no error")
			}

			assert.Equal(t, tt.expectedJoins, b.joins, "expected joins to be updated correctly")
			assert.Equal(t, b, result, "expected Join() to return the same builder instance")
		})
	}
}

func TestBuilder_LeftJoin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialJoins  []join
		table         string
		leftCol       string
		operator      string
		rightCol      string
		expectedJoins []join
		expectedErr   error
	}{
		{
			name:     "should add a simple LEFT JOIN clause",
			table:    "orders",
			leftCol:  "users.id",
			operator: "=",
			rightCol: "orders.user_id",
			expectedJoins: []join{
				{queryType: QueryBasic, joinType: "LEFT JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
			},
		},
		{
			name: "should add multiple LEFT JOIN clauses",
			initialJoins: []join{
				{queryType: QueryBasic, joinType: "LEFT JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
			},
			table:    "products",
			leftCol:  "orders.product_id",
			operator: "=",
			rightCol: "products.id",
			expectedJoins: []join{
				{queryType: QueryBasic, joinType: "LEFT JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
				{queryType: QueryBasic, joinType: "LEFT JOIN", table: "products", leftCol: "orders.product_id", operator: "=", rightCol: "products.id"},
			},
		},
		{
			name:        "should return an error if table is empty",
			table:       "",
			leftCol:     "users.id",
			operator:    "=",
			rightCol:    "orders.user_id",
			expectedErr: ErrEmptyTable,
		},
		{
			name:        "should return an error if left column is empty",
			table:       "orders",
			leftCol:     "",
			operator:    "=",
			rightCol:    "orders.user_id",
			expectedErr: ErrInvalidJoinCondition,
		},
		{
			name:        "should return an error if operator is empty",
			table:       "orders",
			leftCol:     "users.id",
			operator:    "",
			rightCol:    "orders.user_id",
			expectedErr: ErrInvalidJoinCondition,
		},
		{
			name:        "should return an error if right column is empty",
			table:       "orders",
			leftCol:     "users.id",
			operator:    "=",
			rightCol:    "",
			expectedErr: ErrInvalidJoinCondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &builder{joins: tt.initialJoins}
			result := b.LeftJoin(tt.table, tt.leftCol, tt.operator, tt.rightCol)

			if tt.expectedErr != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedErr, "expected error message to match")
			} else {
				assert.NoError(t, b.err, "expected no error")
			}

			assert.Equal(t, tt.expectedJoins, b.joins, "expected joins to be updated correctly")
			assert.Equal(t, b, result, "expected LeftJoin() to return the same builder instance")
		})
	}
}

func TestBuilder_RightJoin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialJoins  []join
		table         string
		leftCol       string
		operator      string
		rightCol      string
		expectedJoins []join
		expectedErr   error
	}{
		{
			name:     "should add a simple RIGHT JOIN clause",
			table:    "orders",
			leftCol:  "users.id",
			operator: "=",
			rightCol: "orders.user_id",
			expectedJoins: []join{
				{queryType: QueryBasic, joinType: "RIGHT JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
			},
		},
		{
			name: "should add multiple RIGHT JOIN clauses",
			initialJoins: []join{
				{queryType: QueryBasic, joinType: "RIGHT JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
			},
			table:    "products",
			leftCol:  "orders.product_id",
			operator: "=",
			rightCol: "products.id",
			expectedJoins: []join{
				{queryType: QueryBasic, joinType: "RIGHT JOIN", table: "orders", leftCol: "users.id", operator: "=", rightCol: "orders.user_id"},
				{queryType: QueryBasic, joinType: "RIGHT JOIN", table: "products", leftCol: "orders.product_id", operator: "=", rightCol: "products.id"},
			},
		},
		{
			name:        "should return an error if table is empty",
			table:       "",
			leftCol:     "users.id",
			operator:    "=",
			rightCol:    "orders.user_id",
			expectedErr: ErrEmptyTable,
		},
		{
			name:        "should return an error if left column is empty",
			table:       "orders",
			leftCol:     "",
			operator:    "=",
			rightCol:    "orders.user_id",
			expectedErr: ErrInvalidJoinCondition,
		},
		{
			name:        "should return an error if operator is empty",
			table:       "orders",
			leftCol:     "users.id",
			operator:    "",
			rightCol:    "orders.user_id",
			expectedErr: ErrInvalidJoinCondition,
		},
		{
			name:        "should return an error if right column is empty",
			table:       "orders",
			leftCol:     "users.id",
			operator:    "=",
			rightCol:    "",
			expectedErr: ErrInvalidJoinCondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &builder{joins: tt.initialJoins}
			result := b.RightJoin(tt.table, tt.leftCol, tt.operator, tt.rightCol)

			if tt.expectedErr != nil {
				assert.Error(t, b.err, "expected an error")
				assert.ErrorIs(t, b.err, tt.expectedErr, "expected error message to match")
			} else {
				assert.NoError(t, b.err, "expected no error")
			}

			assert.Equal(t, tt.expectedJoins, b.joins, "expected joins to be updated correctly")
			assert.Equal(t, b, result, "expected RightJoin() to return the same builder instance")
		})
	}
}

// -----------------
// --- BENCHMARK ---
// -----------------

func BenchmarkBuilder_Join(b *testing.B) {
	builder := &builder{}
	table := "orders"
	leftCol := "users.id"
	operator := "="
	rightCol := "orders.user_id"

	for b.Loop() {
		builder.Join(table, leftCol, operator, rightCol)
	}
}

func BenchmarkBuilder_LeftJoin(b *testing.B) {
	builder := &builder{}
	table := "orders"
	leftCol := "users.id"
	operator := "="
	rightCol := "orders.user_id"

	for b.Loop() {
		builder.LeftJoin(table, leftCol, operator, rightCol)
	}
}

func BenchmarkBuilder_RightJoin(b *testing.B) {
	builder := &builder{}
	table := "orders"
	leftCol := "users.id"
	operator := "="
	rightCol := "orders.user_id"

	for b.Loop() {
		builder.RightJoin(table, leftCol, operator, rightCol)
	}
}
