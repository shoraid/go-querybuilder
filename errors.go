package sequel

import "errors"

var (
	ErrEmptyColumn      = errors.New("empty column")
	ErrBetweenNilBounds = errors.New("between: nil bound")
)
