package goquerybuilder

import (
	"fmt"
)

func (b *builder) ToSQL() (string, []any, error) {
	switch b.action {
	case "select":
		return b.dialect.CompileSelect(b)

	default:
		return "", nil, fmt.Errorf("unsupported action: %s", b.action)
	}
}
