package goquerybuilder

import (
	"strings"
)

func (b *builder) OrderBy(column, dir string) QueryBuilder {
	dir = strings.ToUpper(dir)
	if dir != "ASC" && dir != "DESC" {
		dir = "ASC"
	}

	b.orderBys = append(b.orderBys, orderBy{
		queryType: QueryBasic,
		column:    column,
		dir:       dir,
	})

	return b
}

func (b *builder) OrderByRaw(expr string, args ...any) QueryBuilder {
	b.orderBys = append(b.orderBys, orderBy{
		queryType: QueryRaw,
		expr:      expr,
		args:      args,
	})

	return b
}

func (b *builder) OrderBySafe(userInput string, dir string, whitelist map[string]string) QueryBuilder {
	col, ok := whitelist[userInput]
	if !ok {
		return b
	}

	dir = strings.ToUpper(dir)
	if dir != "ASC" && dir != "DESC" {
		dir = "ASC"
	}

	return b.OrderBy(col, dir)
}
