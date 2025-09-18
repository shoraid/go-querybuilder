package goquerybuilder

import (
	"fmt"
	"strings"
)

type PostgresDialect struct {
	//
}

func (d PostgresDialect) Capabilities() DialectCapabilities {
	return DialectCapabilities{
		SupportsExcept:    true,
		SupportsFullJoin:  true,
		SupportsIntersect: true,
		SupportsReturning: true,
	}
}

func (d PostgresDialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

func (d PostgresDialect) WrapColumn(expr string) string {
	parts := strings.Fields(expr) // preserve original case but split cleanly
	if len(parts) == 3 && strings.EqualFold(parts[1], "as") {
		col := parts[0]   // just the first token (before AS)
		alias := parts[2] // after AS

		return d.WrapIdentifier(col) + " AS " + alias
	}

	return d.WrapIdentifier(expr)
}

func (d PostgresDialect) WrapIdentifier(id string) string {
	parts := strings.Split(id, ".")
	for i, p := range parts {
		parts[i] = `"` + p + `"`
	}

	return strings.Join(parts, ".")
}

func (d PostgresDialect) WrapTable(expr string) string {
	parts := strings.Fields(expr)
	if len(parts) == 2 {
		return `"` + parts[0] + `" AS ` + parts[1]
	}

	return `"` + expr + `"`
}
