package goquerybuilder

import "slices"

func (b *builder) Select(columns ...string) QueryBuilder {
	b.action = "select"
	b.columns = columns

	return b
}

func (b *builder) AddSelect(column string) QueryBuilder {
	if !slices.Contains(b.columns, column) {
		b.columns = append(b.columns, column)
	}

	return b
}
