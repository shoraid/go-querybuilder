package sequel

func (b *builder) ToSQL() (string, []any, error) {
	if b.dialect == nil {
		return "", nil, ErrNoDialect
	}

	switch b.action {
	case "select":
		return b.dialect.CompileSelect(b)

	default:
		return "", nil, ErrUnsupportedAction
	}
}
