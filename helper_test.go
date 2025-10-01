package sequel

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_addErr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialErr  error
		inputErr    error
		expectedErr error
	}{
		{
			name:        "should do nothing when input error is nil",
			initialErr:  nil,
			inputErr:    nil,
			expectedErr: nil,
		},
		{
			name:        "should set first error when no existing error",
			initialErr:  nil,
			inputErr:    errors.New("first"),
			expectedErr: errors.New("first"),
		},
		{
			name:        "should preserve existing error when new error is provided",
			initialErr:  errors.New("first"),
			inputErr:    errors.New("second"),
			expectedErr: errors.New("first"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{err: tt.initialErr}

			// Act
			b.addErr(tt.inputErr)

			// Assert
			if tt.expectedErr == nil {
				assert.Nil(t, b.err)
			} else {
				assert.NotNil(t, b.err)
				assert.EqualError(t, b.err, tt.expectedErr.Error())
			}
		})
	}
}

func TestBuilder_flattenArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		args          []any
		expectedArgs  []any
		expectedError error
	}{
		{
			name:         "should flatten a slice of interfaces",
			args:         []any{1, "test", []any{2, 3}, 4},
			expectedArgs: []any{1, "test", 2, 3, 4},
		},
		{
			name:         "should handle empty slices",
			args:         []any{1, []any{}, 2},
			expectedArgs: []any{1, 2},
		},
		{
			name:         "should return original slice if no nested slices",
			args:         []any{1, 2, 3},
			expectedArgs: []any{1, 2, 3},
		},
		{
			name:         "should return empty slice for nil input",
			args:         nil,
			expectedArgs: []any{},
		},
		{
			name:         "should return empty slice for empty input",
			args:         []any{},
			expectedArgs: []any{},
		},
		{
			name:         "should handle mixed types including slices of specific types",
			args:         []any{1, "hello", []int{2, 3}, true, []string{"a", "b"}},
			expectedArgs: []any{1, "hello", 2, 3, true, "a", "b"},
		},
		{
			name:          "should return error for nil value",
			args:          []any{1, nil, 2},
			expectedArgs:  []any{},
			expectedError: ErrNilNotAllowed,
		},
		{
			name:          "should handle nested slices",
			args:          []any{1, []any{2, []any{3, 4}}, 5},
			expectedArgs:  []any{},
			expectedError: ErrNestedSlice,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Act
			result, err := flattenArgs(tt.args)

			// Assert
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tt.expectedError)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedArgs, result)
		})
	}
}
