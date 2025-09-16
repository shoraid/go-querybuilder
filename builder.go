package goquerybuilder

import "github.com/shoraid/go-querybuilder/dialect"

type QueryBuilder interface {
	// Core
	Select(columns ...string) QueryBuilder
	AddSelect(column string) QueryBuilder
	From(table string) QueryBuilder
	ToSQL() (string, []any, error)

	// Pagination
	Limit(limit int) QueryBuilder
	Offset(offset int) QueryBuilder

	// Getter
	GetTable() string
	GetColumns() []string
	GetAction() string
	GetDialect() dialect.Dialect
}

type builder struct {
	dialect dialect.Dialect
	action  string
	table   string
	columns []string
	limit   int
	offset  int
}

func New(d dialect.Dialect) QueryBuilder {
	return &builder{
		dialect: d,
		limit:   -1,
		offset:  -1,
	}
}

func (b *builder) GetDialect() dialect.Dialect {
	return b.dialect
}

func (b *builder) GetAction() string {
	return b.action
}

func (b *builder) GetTable() string {
	return b.table
}

func (b *builder) GetColumns() []string {
	return b.columns
}
