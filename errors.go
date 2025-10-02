package sequel

import "errors"

var (
	ErrEmptyColumn     = errors.New("empty column")
	ErrEmptyTable      = errors.New("empty table")
	ErrNilNotAllowed   = errors.New("nil values are not supported")
	ErrNestedSlice     = errors.New("nested slices are not supported")
	ErrEmptyExpression = errors.New("empty expression")
	ErrNilFunc         = errors.New("nil function")
	ErrTypeMismatch    = errors.New("type mismatch")
)
