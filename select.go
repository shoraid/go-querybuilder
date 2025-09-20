package sequel

func (b *builder) Select(columns ...string) QueryBuilder {
	b.action = "select"
	b.columns = make([]column, len(columns)) // Reset columns

	for i, col := range columns {
		b.columns[i] = column{queryType: QueryBasic, name: col}
	}

	return b
}

func (b *builder) SelectRaw(expr string, args ...any) QueryBuilder {
	b.action = "select"
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
	for _, newCol := range columns {
		found := false
		for _, existingCol := range b.columns {
			if existingCol.queryType == QueryBasic && existingCol.name == newCol {
				found = true
				break
			}
		}

		if !found {
			b.columns = append(b.columns, column{queryType: QueryBasic, name: newCol})
		}
	}

	return b
}

func (b *builder) AddSelectRaw(expr string, args ...any) QueryBuilder {
	b.columns = append(b.columns, column{
		queryType: QueryRaw,
		expr:      expr,
		args:      args,
	})

	return b
}

func (b *builder) AddSelectSafe(userInput []string, whitelist map[string]string) QueryBuilder {
	for _, in := range userInput {
		col, ok := whitelist[in]
		if !ok {
			continue
		}

		found := false
		for _, existingCol := range b.columns {
			if existingCol.queryType == QueryBasic && existingCol.name == col {
				found = true
				break
			}
		}

		if !found {
			b.columns = append(b.columns, column{queryType: QueryBasic, name: col})
		}
	}

	return b
}
