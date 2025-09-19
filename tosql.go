package goquerybuilder

import (
	"fmt"
)

func (b *builder) ToSQL() (string, []any, error) {
	if b.dialect == nil {
		return "", nil, fmt.Errorf("no dialect specified for builder")
	}

	switch b.action {
	case "select":
		return b.dialect.CompileSelect(b)

	default:
		return "", nil, fmt.Errorf("unsupported action: %s", b.action)
	}
}
