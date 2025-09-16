package goquerybuilder

func (b *builder) Offset(offset int) QueryBuilder {
	if offset < 0 {
		offset = 0
	}

	b.offset = offset

	return b
}
