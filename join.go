package sequel

func (b *builder) Join(table, leftCol, operator, rightCol string) QueryBuilder {
	b.addJoin("INNER JOIN", table, leftCol, operator, rightCol)
	return b
}

func (b *builder) LeftJoin(table, leftCol, operator, rightCol string) QueryBuilder {
	b.addJoin("LEFT JOIN", table, leftCol, operator, rightCol)
	return b
}

func (b *builder) RightJoin(table, leftCol, operator, rightCol string) QueryBuilder {
	b.addJoin("RIGHT JOIN", table, leftCol, operator, rightCol)
	return b
}

func (b *builder) addJoin(joinType, table, leftCol, operator, rightCol string) {
	if table == "" {
		b.addErr(ErrEmptyTable)
		return
	}

	if leftCol == "" || operator == "" || rightCol == "" {
		b.addErr(ErrInvalidJoinCondition)
		return
	}

	b.joins = append(b.joins, join{
		queryType: QueryBasic,
		joinType:  joinType,
		table:     table,
		leftCol:   leftCol,
		operator:  operator,
		rightCol:  rightCol,
	})
}
