package sequel

func (b *builder) From(table string) QueryBuilder {
	if table == "" {
		b.addErr(ErrEmptyTable)
		return b
	}

	b.table = table
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
