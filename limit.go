package goquerybuilder

func (b *builder) Limit(limit int) QueryBuilder {
	if limit < 0 {
		limit = 0
	}

	b.limit = limit

	return b
}
