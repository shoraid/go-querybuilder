package sequel

import (
	"reflect"
	"strings"
)

func (b *builder) Where(column string, operator string, values ...any) QueryBuilder {
	b.addWhere("AND", column, operator, values...)
	return b
}

func (b *builder) OrWhere(column string, operator string, values ...any) QueryBuilder {
	b.addWhere("OR", column, operator, values...)
	return b
}

func (b *builder) addWhere(conj, column, operator string, values ...any) {
	switch operator {
	case "IN", "NOT IN":
		b.addWhereIn(conj, column, operator, values...)

	case "BETWEEN", "NOT BETWEEN":
		var from, to any

		// Handle slice values: []int{a,b}, []string{a,b}, etc.
		if len(values) == 1 {
			rv := reflect.ValueOf(values[0])
			if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
				if rv.Len() >= 2 {
					from = rv.Index(0).Interface()
					to = rv.Index(1).Interface()
				} else if rv.Len() == 1 {
					from = rv.Index(0).Interface()
				}
			} else {
				// Single non-slice value
				from = values[0]
			}
		} else if len(values) == 2 {
			from = values[0]
			to = values[1]
		}

		b.addWhereBetween(conj, column, operator, from, to)

	case "IS NULL", "IS NOT NULL":
		b.addWhereNull(conj, column, operator)

	default:
		if values == nil {
			values = []any{}
		}

		b.wheres = append(b.wheres, where{
			queryType: QueryBasic,
			conj:      conj,
			column:    column,
			operator:  operator,
			args:      values,
		})
	}
}

func (b *builder) WhereBetween(column string, from, to any) QueryBuilder {
	b.addWhereBetween("AND", column, "BETWEEN", from, to)
	return b
}

func (b *builder) OrWhereBetween(column string, from, to any) QueryBuilder {
	b.addWhereBetween("OR", column, "BETWEEN", from, to)
	return b
}

func (b *builder) WhereNotBetween(column string, from, to any) QueryBuilder {
	b.addWhereBetween("AND", column, "NOT BETWEEN", from, to)
	return b
}

func (b *builder) OrWhereNotBetween(column string, from, to any) QueryBuilder {
	b.addWhereBetween("OR", column, "NOT BETWEEN", from, to)
	return b
}

func (b *builder) addWhereBetween(conj, column, operator string, from, to any) {
	if column == "" {
		b.addErr(ErrEmptyColumn)
		return
	}

	if from == nil || to == nil {
		b.addErr(ErrBetweenNilBounds)
		return
	}

	b.wheres = append(b.wheres, where{
		queryType: QueryBetween,
		conj:      conj,
		column:    column,
		operator:  operator,
		args:      []any{from, to},
	})
}

func (b *builder) WhereIn(column string, values ...any) QueryBuilder {
	b.addWhereIn("AND", column, "IN", values...)
	return b
}

func (b *builder) OrWhereIn(column string, values ...any) QueryBuilder {
	b.addWhereIn("OR", column, "IN", values...)
	return b
}

func (b *builder) WhereNotIn(column string, values ...any) QueryBuilder {
	b.addWhereIn("AND", column, "NOT IN", values...)
	return b
}

func (b *builder) OrWhereNotIn(column string, values ...any) QueryBuilder {
	b.addWhereIn("OR", column, "NOT IN", values...)
	return b
}

func (b *builder) addWhereIn(conj, column, operator string, values ...any) {
	if column == "" {
		b.addErr(ErrEmptyColumn)
		return
	}

	args, err := flattenArgs(values)
	if err != nil {
		b.addErr(err)
		return
	}

	b.wheres = append(b.wheres, where{
		queryType: QueryIn,
		conj:      conj,
		column:    column,
		operator:  operator,
		args:      args,
	})
}

func (b *builder) WhereNull(column string) QueryBuilder {
	b.addWhereNull("AND", column, "IS NULL")
	return b
}

func (b *builder) OrWhereNull(column string) QueryBuilder {
	b.addWhereNull("OR", column, "IS NULL")
	return b
}

func (b *builder) WhereNotNull(column string) QueryBuilder {
	b.addWhereNull("AND", column, "IS NOT NULL")
	return b
}

func (b *builder) OrWhereNotNull(column string) QueryBuilder {
	b.addWhereNull("OR", column, "IS NOT NULL")
	return b
}

func (b *builder) addWhereNull(conj, column, operator string) {
	b.wheres = append(b.wheres, where{
		queryType: QueryNull,
		conj:      conj,
		column:    column,
		operator:  operator,
		args:      []any{},
	})
}

func (b *builder) WhereRaw(expr string, args ...any) QueryBuilder {
	b.addWhereRaw("AND", expr, args...)
	return b
}

func (b *builder) OrWhereRaw(expr string, args ...any) QueryBuilder {
	b.addWhereRaw("OR", expr, args...)
	return b
}

func (b *builder) addWhereRaw(conj, expr string, args ...any) {
	b.wheres = append(b.wheres, where{
		queryType: QueryRaw,
		conj:      conj,
		expr:      expr,
		args:      args,
	})
}

func (b *builder) WhereGroup(fn func(QueryBuilder)) QueryBuilder {
	b.addWhereGroup("AND", fn)
	return b
}

func (b *builder) OrWhereGroup(fn func(QueryBuilder)) QueryBuilder {
	b.addWhereGroup("OR", fn)
	return b
}

func (b *builder) addWhereGroup(conj string, fn func(QueryBuilder)) {
	nestedBuilder := New(b.dialect).(*builder)
	fn(nestedBuilder)

	if len(nestedBuilder.wheres) > 0 {
		b.wheres = append(b.wheres, where{
			queryType: QueryNested,
			conj:      conj,
			nested:    nestedBuilder.wheres,
		})
	}
}

func (b *builder) WhereSub(column, operator string, fn func(QueryBuilder)) QueryBuilder {
	b.addWhereSub("AND", column, operator, fn)
	return b
}

func (b *builder) OrWhereSub(column, operator string, fn func(QueryBuilder)) QueryBuilder {
	b.addWhereSub("OR", column, operator, fn)
	return b
}

func (b *builder) WhereExists(sub func(QueryBuilder)) QueryBuilder {
	b.addWhereSub("AND", "", "EXISTS", sub)
	return b
}

func (b *builder) OrWhereExists(sub func(QueryBuilder)) QueryBuilder {
	b.addWhereSub("OR", "", "EXISTS", sub)
	return b
}

func (b *builder) WhereNotExists(sub func(QueryBuilder)) QueryBuilder {
	b.addWhereSub("AND", "", "NOT EXISTS", sub)
	return b
}

func (b *builder) OrWhereNotExists(sub func(QueryBuilder)) QueryBuilder {
	b.addWhereSub("OR", "", "NOT EXISTS", sub)
	return b
}

func (b *builder) addWhereSub(conj, column, operator string, fn func(QueryBuilder)) {
	if fn == nil {
		b.wheres = append(b.wheres, where{
			queryType: QuerySub,
			conj:      conj,
			column:    column,
			operator:  strings.ToUpper(operator),
			sub:       nil,
		})
		return
	}

	subBuilder := New(b.dialect).(*builder)
	subBuilder.action = "select"
	fn(subBuilder)

	b.wheres = append(b.wheres, where{
		queryType: QuerySub,
		conj:      conj,
		column:    column,
		operator:  strings.ToUpper(operator),
		sub:       subBuilder,
	})
}
