package goquerybuilder

import (
	"fmt"
	"strings"
)

func (b *builder) OrderBy(column string, direction string) QueryBuilder {
	dir := strings.ToUpper(direction)
	if dir != "ASC" && dir != "DESC" {
		dir = "ASC"
	}

	b.orderBys = append(b.orderBys, fmt.Sprintf("%s %s", b.dialect.QuoteIdentifier(column), dir))

	return b
}

func (b *builder) OrderByRaw(expr string) QueryBuilder {
	b.orderBys = append(b.orderBys, expr)

	return b
}

func (b *builder) OrderBySafe(userInput string, dir string, whitelist map[string]string) (QueryBuilder, error) {
	col, ok := whitelist[userInput]
	if !ok {
		return nil, fmt.Errorf("invalid order by column: %s", userInput)
	}

	if strings.ToUpper(dir) != "ASC" && strings.ToUpper(dir) != "DESC" {
		dir = "ASC"
	}

	return b.OrderBy(col, dir), nil
}
