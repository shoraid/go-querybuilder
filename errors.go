package sequel

import "errors"

var (
	ErrEmptyColumn       = errors.New("empty column")
	ErrEmptyTable        = errors.New("empty table")
	ErrNoDialect         = errors.New("no dialect specified for builder")
	ErrUnsupportedAction = errors.New("unsupported action")
	ErrNilNotAllowed     = errors.New("nil values are not supported")
	ErrNestedSlice       = errors.New("nested slices are not supported")
	ErrEmptyExpression   = errors.New("empty expression")
	ErrNilFunc           = errors.New("nil function")
	ErrTypeMismatch      = errors.New("type mismatch")
)
