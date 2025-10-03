package sequel

func (b *builder) From(tbl string) QueryBuilder {
	if tbl == "" {
		b.addErr(ErrEmptyTable)
		return b
	}

	b.table = table{
		queryType: QueryBasic,
		name:      tbl,
	}

	return b
}

func (b *builder) FromRaw(expr string, args ...any) QueryBuilder {
	if expr == "" {
		b.addErr(ErrEmptyExpression)
		return b
	}

	b.table = table{
		queryType: QueryRaw,
		expr:      expr,
		args:      args,
	}

	return b
}

func (b *builder) FromSafe(userInput string, whitelist map[string]string) QueryBuilder {
	table, ok := whitelist[userInput]
	if !ok {
		b.addErr(ErrInvalidTableInput)
		return b
	}

	return b.From(table)
}
