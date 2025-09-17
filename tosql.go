package goquerybuilder

import (
	"fmt"
	"strconv"
	"strings"
)

func (b *builder) ToSQL() (string, []any, error) {
	switch b.action {
	case "select":
		return b.buildSelect()

	default:
		return "", nil, fmt.Errorf("unsupported action: %s", b.action)
	}
}

func (b *builder) buildSelect() (string, []any, error) {
	if b.table == "" {
		return "", nil, fmt.Errorf("no table specified")
	}

	var args []any
	var sb strings.Builder

	// --- SELECT clause ---
	cols := "*"
	if len(b.columns) > 0 {
		quoted := make([]string, len(b.columns))
		for i, col := range b.columns {
			quoted[i] = b.dialect.QuoteColumnWithAlias(col)
		}

		cols = strings.Join(quoted, ", ")
	}

	// build SQL
	sb.WriteString("SELECT ")
	sb.WriteString(cols)
	sb.WriteString(" FROM ")
	sb.WriteString(b.dialect.QuoteTableWithAlias(b.table))

	// --- ORDER BY clause ---
	if len(b.orderBys) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(b.orderBys, ", "))
	}

	// --- LIMIT / OFFSET ---
	if b.limit >= 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(b.limit))
	}
	if b.offset >= 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(b.offset))
	}

	return sb.String(), args, nil
}
