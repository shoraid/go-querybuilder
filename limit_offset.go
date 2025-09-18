package goquerybuilder

func (b *builder) Limit(limit int) QueryBuilder {
	if limit < 0 {
		limit = 0
	}

	b.limit = limit

	return b
}

func (b *builder) Offset(offset int) QueryBuilder {
	if offset < 0 {
		offset = 0
	}

	b.offset = offset

	return b
}
