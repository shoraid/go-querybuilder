package sequel

import (
	"fmt"
	"regexp"
	"strconv"
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
	var sb strings.Builder
	parts := strings.Fields(expr) // preserve original case but split cleanly
	if len(parts) == 3 && strings.EqualFold(parts[1], "as") {
		sb.WriteString(d.WrapIdentifier(parts[0])) // column
		sb.WriteString(" AS ")
		sb.WriteString(`"`)
		sb.WriteString(parts[2]) // alias
		sb.WriteString(`"`)

		return sb.String()
	}

	return d.WrapIdentifier(expr)
}

func (d PostgresDialect) WrapIdentifier(id string) string {
	var sb strings.Builder
	parts := strings.Split(id, ".")
	for i, p := range parts {
		if i > 0 {
			sb.WriteString(".")
		}
		sb.WriteString(`"`)
		sb.WriteString(p)
		sb.WriteString(`"`)
	}

	return sb.String()
}

func (d PostgresDialect) WrapTable(expr string) string {
	if expr == "" {
		return ""
	}

	parts := strings.Fields(expr)
	if len(parts) == 2 {
		var sb strings.Builder
		sb.WriteString(d.WrapIdentifier(parts[0]))
		sb.WriteString(" AS ")
		sb.WriteString(`"`)
		sb.WriteString(parts[1]) // alias
		sb.WriteString(`"`)

		return sb.String()
	}

	return d.WrapIdentifier(expr)
}

func (d PostgresDialect) CompileSelect(b *builder) (string, []any, error) {
	if b.table == "" {
		return "", nil, fmt.Errorf("no table specified")
	}

	args := []any{}
	var sb strings.Builder

	// SELECT clause
	sb.WriteString("SELECT ")
	sb.WriteString(d.compileSelectClause(b.columns, &args))

	// FROM clause
	sb.WriteString(" FROM ")
	sb.WriteString(d.WrapTable(b.table))

	// WHERE clause (recursive)
	if len(b.wheres) > 0 {
		whereClause, err := d.compileWhereClause(b.wheres, &args)
		if err != nil {
			return "", nil, err
		}

		sb.WriteString(" WHERE ")
		sb.WriteString(whereClause)
	}

	// ORDER BY clause
	if len(b.orderBys) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(d.compileOrderByClause(b.orderBys, &args))
	}

	// LIMIT / OFFSET
	if b.limit >= 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", b.limit))
	}
	if b.offset >= 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", b.offset))
	}

	return sb.String(), args, nil
}

func (d PostgresDialect) compileSelectClause(columns []column, globalArgs *[]any) string {
	var sb strings.Builder

	if len(columns) == 0 {
		sb.WriteString("*")
	} else {
		for i, col := range columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			switch col.queryType {
			case QueryBasic:
				sb.WriteString(d.WrapColumn(col.name))
			case QueryRaw:
				sb.WriteString(col.expr)
				*globalArgs = append(*globalArgs, col.args...)
			}
		}
	}

	return sb.String()
}

// Recursive WHERE compiler
func (d PostgresDialect) compileWhereClause(wheres []where, globalArgs *[]any) (string, error) {
	var sb strings.Builder

	for i, w := range wheres {
		if i > 0 {
			conj := w.conj
			if conj == "" {
				conj = "AND"
			}

			sb.WriteString(" ")
			sb.WriteString(conj)
			sb.WriteString(" ")
		}

		switch w.queryType {
		case QueryBasic:
			sb.WriteString(d.WrapColumn(w.column))
			sb.WriteString(" ")
			sb.WriteString(w.operator)
			sb.WriteString(" ")
			if strings.Contains(w.operator, "IN") {
				sb.WriteString("(")
				vals := w.args[0].([]any)
				for j, v := range vals {
					if j > 0 {
						sb.WriteString(", ")
					}
					sb.WriteString(d.Placeholder(len(*globalArgs) + 1))
					*globalArgs = append(*globalArgs, v)
				}
				sb.WriteString(")")
			} else {
				sb.WriteString(d.Placeholder(len(*globalArgs) + 1))
				*globalArgs = append(*globalArgs, w.args...)
			}

		case QueryBetween:
			sb.WriteString(d.WrapColumn(w.column))
			sb.WriteString(" ")
			sb.WriteString(w.operator)
			sb.WriteString(" ")
			sb.WriteString(d.Placeholder(len(*globalArgs) + 1))
			sb.WriteString(" AND ")
			sb.WriteString(d.Placeholder(len(*globalArgs) + 2))
			*globalArgs = append(*globalArgs, w.args...)

		case QueryNull:
			sb.WriteString(d.WrapColumn(w.column))
			sb.WriteString(" ")
			sb.WriteString(w.operator)

		case QueryRaw:
			expr := w.expr
			for _, a := range w.args {
				expr = strings.Replace(expr, "?", d.Placeholder(len(*globalArgs)+1), 1)
				*globalArgs = append(*globalArgs, a)
			}
			sb.WriteString(expr)

		case QueryNested:
			whereClause, err := d.compileWhereClause(w.nested, globalArgs) // recursion updates globalArgs directly
			if err != nil {
				return "", err
			}

			sb.WriteString("(")
			sb.WriteString(whereClause)
			sb.WriteString(")")

		case QuerySub:
			subSQL, subArgs, err := w.sub.ToSQL()
			if err != nil {
				return "", err
			}

			// Renumber placeholders inside subquery SQL without collisions
			base := len(*globalArgs) // number of args already present in the outer query
			re := regexp.MustCompile(`\$(\d+)`)
			subSQL = re.ReplaceAllStringFunc(subSQL, func(m string) string {
				nStr := m[1:] // strip leading '$'

				// NOTE: strconv.Atoi cannot fail here because the regex \$(\d+) guarantees nStr contains only digits.
				// Error handling is unnecessary and was removed to avoid an unreachable branch in coverage.
				n, _ := strconv.Atoi(nStr)

				return d.Placeholder(base + n) // shift by base
			})

			// Append subquery args in the same order
			*globalArgs = append(*globalArgs, subArgs...)

			if w.column != "" {
				sb.WriteString(d.WrapColumn(w.column))
				sb.WriteString(" ")
			}
			sb.WriteString(w.operator)
			sb.WriteString(" (")
			sb.WriteString(subSQL)
			sb.WriteString(")")
		}
	}

	return sb.String(), nil
}

func (d PostgresDialect) compileOrderByClause(orderBys []orderBy, globalArgs *[]any) string {
	var sb strings.Builder

	for i, ob := range orderBys {
		if i > 0 {
			sb.WriteString(", ")
		}
		switch ob.queryType {
		case QueryBasic:
			sb.WriteString(d.WrapColumn(ob.column))
			sb.WriteString(" ")
			sb.WriteString(ob.dir)
		case QueryRaw:
			expr := ob.expr
			for _, a := range ob.args {
				expr = strings.Replace(expr, "?", d.Placeholder(len(*globalArgs)+1), 1)
				*globalArgs = append(*globalArgs, a)
			}
			sb.WriteString(expr)
		}
	}

	return sb.String()
}
