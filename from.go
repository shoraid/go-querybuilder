package goquerybuilder

func (b *builder) From(table string) QueryBuilder {
	b.table = table

	return b
}
