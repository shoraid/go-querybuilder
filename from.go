package sequel

import "fmt"

func (b *builder) From(table string) QueryBuilder {
	if table == "" {
		b.addErr(ErrEmptyTable)
		return b
	}

	b.table = table
	return b
}

func (b *builder) FromSafe(userInput string, whitelist map[string]string) (QueryBuilder, error) {
	tbl, ok := whitelist[userInput]
	if !ok {
		return nil, fmt.Errorf("invalid table: %s", userInput)
	}

	return b.From(tbl), nil
}
