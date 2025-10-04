package sequel

func (b *builder) Select(columns ...string) QueryBuilder {
	b.action = "select"
	if len(columns) == 0 {
		b.columns = b.columns[:0] // reuse slice
	}

	b.columns = make([]column, len(columns)) // Reset columns
	for i, col := range columns {
		b.columns[i] = column{queryType: QueryBasic, name: col}
	}

	return b
}

func (b *builder) SelectRaw(expr string, args ...any) QueryBuilder {
	b.action = "select"

	if expr == "" {
		b.addErr(ErrEmptyExpression)
		return b
	}

	// Reset columns
	b.columns = []column{{
		queryType: QueryRaw,
		expr:      expr,
		args:      args,
	}}

	return b
}

func (b *builder) SelectSafe(userInput []string, whitelist map[string]string) QueryBuilder {
	b.action = "select"
	b.columns = make([]column, 0, len(userInput)) // Reset columns

	for _, in := range userInput {
		col, ok := whitelist[in]
		if ok {
			b.columns = append(b.columns, column{queryType: QueryBasic, name: col})
		}
	}

	return b
}

func (b *builder) AddSelect(columns ...string) QueryBuilder {
	if len(columns) == 0 {
		return b
	}

	existing := make(map[string]struct{}, len(b.columns))
	for _, col := range b.columns {
		if col.queryType == QueryBasic {
			existing[col.name] = struct{}{}
		}
	}

	for _, newCol := range columns {
		if _, found := existing[newCol]; !found {
			b.columns = append(b.columns, column{queryType: QueryBasic, name: newCol})
			existing[newCol] = struct{}{}
		}
	}

	return b
}

func (b *builder) AddSelectRaw(expr string, args ...any) QueryBuilder {
	if expr == "" {
		b.addErr(ErrEmptyExpression)
		return b
	}

	b.columns = append(b.columns, column{
		queryType: QueryRaw,
		expr:      expr,
		args:      args,
	})

	return b
}

func (b *builder) AddSelectSafe(userInput []string, whitelist map[string]string) QueryBuilder {
	if len(userInput) == 0 {
		return b
	}

	existing := make(map[string]struct{}, len(b.columns))
	for _, col := range b.columns {
		if col.queryType == QueryBasic {
			existing[col.name] = struct{}{}
		}
	}

	for _, in := range userInput {
		col, ok := whitelist[in]
		if !ok {
			continue // column not allowed
		}

		if _, found := existing[col]; !found {
			b.columns = append(b.columns, column{queryType: QueryBasic, name: col})
			existing[col] = struct{}{}
		}
	}

	return b
}
