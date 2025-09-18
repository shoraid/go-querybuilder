package goquerybuilder

func (b *builder) Where(column string, operator string, value any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBasic,
		column:    column,
		operator:  operator,
		conj:      "AND",
		args:      []any{value},
	})

	return b
}

func (b *builder) OrWhere(column string, operator string, value any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBasic,
		column:    column,
		operator:  operator,
		conj:      "OR",
		args:      []any{value},
	})

	return b
}

func (b *builder) WhereBetween(column string, from, to any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBetween,
		column:    column,
		operator:  "BETWEEN",
		conj:      "AND",
		args:      []any{from, to},
	})

	return b
}

func (b *builder) OrWhereBetween(column string, from, to any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBetween,
		column:    column,
		operator:  "BETWEEN",
		conj:      "OR",
		args:      []any{from, to},
	})

	return b
}

func (b *builder) WhereNotBetween(column string, from, to any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBetween,
		column:    column,
		operator:  "NOT BETWEEN",
		conj:      "AND",
		args:      []any{from, to},
	})

	return b
}

func (b *builder) OrWhereNotBetween(column string, from, to any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBetween,
		column:    column,
		operator:  "NOT BETWEEN",
		conj:      "OR",
		args:      []any{from, to},
	})

	return b
}

func (b *builder) WhereIn(column string, values []any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBasic,
		column:    column,
		operator:  "IN",
		conj:      "AND",
		args:      []any{values},
	})

	return b
}

func (b *builder) OrWhereIn(column string, values []any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBasic,
		column:    column,
		operator:  "IN",
		conj:      "OR",
		args:      []any{values},
	})

	return b
}

func (b *builder) WhereNotIn(column string, values []any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBasic,
		column:    column,
		operator:  "NOT IN",
		conj:      "AND",
		args:      []any{values},
	})

	return b
}

func (b *builder) OrWhereNotIn(column string, values []any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryBasic,
		column:    column,
		operator:  "NOT IN",
		conj:      "OR",
		args:      []any{values},
	})

	return b
}

func (b *builder) WhereNull(column string) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryNull,
		column:    column,
		operator:  "IS NULL",
		conj:      "AND",
		args:      []any{},
	})

	return b
}

func (b *builder) OrWhereNull(column string) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryNull,
		column:    column,
		operator:  "IS NULL",
		conj:      "OR",
		args:      []any{},
	})

	return b
}

func (b *builder) WhereNotNull(column string) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryNull,
		column:    column,
		operator:  "IS NOT NULL",
		conj:      "AND",
		args:      []any{},
	})

	return b
}

func (b *builder) OrWhereNotNull(column string) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryNull,
		column:    column,
		operator:  "IS NOT NULL",
		conj:      "OR",
		args:      []any{},
	})

	return b
}

func (b *builder) WhereRaw(expr string, args ...any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryRaw,
		expr:      expr,
		conj:      "AND",
		args:      args,
	})

	return b
}

func (b *builder) OrWhereRaw(expr string, args ...any) QueryBuilder {
	b.wheres = append(b.wheres, where{
		queryType: QueryRaw,
		expr:      expr,
		conj:      "OR",
		args:      args,
	})

	return b
}
