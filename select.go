package goquerybuilder

import (
	"fmt"
	"slices"
)

func (b *builder) Select(columns ...string) QueryBuilder {
	b.action = "select"
	b.columns = columns

	return b
}

func (b *builder) SelectSafe(userInput []string, whitelist map[string]string) (QueryBuilder, error) {
	cols := []string{}
	for _, in := range userInput {
		col, ok := whitelist[in]
		if !ok {
			return nil, fmt.Errorf("invalid column: %s", in)
		}

		cols = append(cols, col)
	}

	return b.Select(cols...), nil
}

func (b *builder) AddSelect(column string) QueryBuilder {
	if !slices.Contains(b.columns, column) {
		b.columns = append(b.columns, column)
	}

	return b
}

func (b *builder) AddSelectSafe(userInput string, whitelist map[string]string) (QueryBuilder, error) {
	col, ok := whitelist[userInput]
	if !ok {
		return nil, fmt.Errorf("invalid column: %s", userInput)
	}

	return b.AddSelect(col), nil
}
