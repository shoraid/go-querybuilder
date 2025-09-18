package goquerybuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		limit    int
		expected int
	}{
		{
			name:     "should set limit correctly",
			limit:    10,
			expected: 10,
		},
		{
			name:     "should set limit to 0 if negative",
			limit:    -5,
			expected: 0,
		},
		{
			name:     "should set limit to 0",
			limit:    0,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{}

			// Act
			result := b.Limit(tt.limit)

			// Assert
			assert.Equal(t, tt.expected, b.limit, "expected limit to be set correctly")
			assert.Equal(t, b, result, "expected Limit() to return the same builder instance")
		})
	}
}

func TestBuilder_Offset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		offset   int
		expected int
	}{
		{
			name:     "should set offset correctly",
			offset:   10,
			expected: 10,
		},
		{
			name:     "should set offset to 0 if negative",
			offset:   -5,
			expected: 0,
		},
		{
			name:     "should set offset to 0",
			offset:   0,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			b := &builder{}

			// Act
			result := b.Offset(tt.offset)

			// Assert
			assert.Equal(t, tt.expected, b.offset, "expected offset to be set correctly")
			assert.Equal(t, b, result, "expected Offset() to return the same builder instance")
		})
	}
}

// -----------------
// --- BENCHMARK ---
// -----------------

func BenchmarkBuilder_Limit(b *testing.B) {
	builder := &builder{}
	limit := 100

	for b.Loop() {
		builder.Limit(limit)
	}
}

func BenchmarkBuilder_Offset(b *testing.B) {
	builder := &builder{}
	offset := 100

	for b.Loop() {
		builder.Offset(offset)
	}
}
