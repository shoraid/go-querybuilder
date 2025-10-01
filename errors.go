package sequel

import "errors"

var (
	ErrEmptyColumn      = errors.New("empty column")
	ErrNilNotAllowed    = errors.New("nil values are not supported")
	ErrNestedSlice      = errors.New("nested slices are not supported")
	ErrEmptyExpression  = errors.New("empty expression")
	ErrNilFunc          = errors.New("nil function")
	ErrBetweenNilBounds = errors.New("between: nil bound")
)
