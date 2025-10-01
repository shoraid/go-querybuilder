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

			b := &builder{err: tt.initialErr}
			b.addErr(tt.inputErr)

			if tt.expectedErr == nil {
				assert.Nil(t, b.err)
			} else {
				assert.NotNil(t, b.err)
				assert.EqualError(t, b.err, tt.expectedErr.Error())
			}
		})
	}
}
