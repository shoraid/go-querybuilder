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
