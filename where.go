package goquerybuilder

import (
	"slices"
	"strings"
)

// ---------- WHERE ----------

var allowedOperators = []string{
	"=", "!=", "<", "<=", ">", ">=", "<>",
	"LIKE", "ILIKE", "IN", "NOT IN",
}

func (b *builder) Where(column string, operator string, value any) QueryBuilder {
	b.addWhere("AND", column, operator, value)

	return b
}

func (b *builder) OrWhere(column string, operator string, value any) QueryBuilder {
	b.addWhere("OR", column, operator, value)

	return b
}

func (b *builder) addWhere(conj, column, operator string, value any) {
	if !slices.Contains(allowedOperators, strings.ToUpper(operator)) {
		operator = "="
	}

	b.AddArgs(value)
	totalArgs := len(b.Args())
	ph := b.dialect.Placeholder(totalArgs)

	b.wheres = append(b.wheres, condition{
		conj:       conj,
		query:      b.dialect.QuoteIdentifier(column) + " " + operator + " " + ph,
		argIndexes: []int{totalArgs - 1},
	})
}
