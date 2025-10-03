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

func (b *builder) FromSub(fn func(QueryBuilder), alias string) QueryBuilder {
	if fn == nil {
		b.addErr(ErrNilFunc)
		return b
	}

	if alias == "" {
		b.addErr(ErrEmptyAlias)
		return b
	}

	subBuilder := New(b.dialect).(*builder)
	fn(subBuilder)

	// propagate child error
	if subBuilder.err != nil {
		b.addErr(subBuilder.err)
		return b
	}

	b.table = table{
		queryType: QuerySub,
		sub:       subBuilder,
		name:      alias,
	}

	return b
}
