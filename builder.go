package goquerybuilder

type QueryBuilder interface {
	// Select
	Select(columns ...string) QueryBuilder
	SelectRaw(expr string, args ...any) QueryBuilder
	SelectSafe(userInput []string, whitelist map[string]string) QueryBuilder
	AddSelect(columns ...string) QueryBuilder
	AddSelectRaw(expr string, args ...any) QueryBuilder
	AddSelectSafe(userInput []string, whitelist map[string]string) QueryBuilder

	From(table string) QueryBuilder
	FromSafe(userInput string, whitelist map[string]string) (QueryBuilder, error)
	ToSQL() (string, []any, error)

	// Where
	Where(column string, operator string, value any) QueryBuilder
	OrWhere(column string, operator string, value any) QueryBuilder

	WhereBetween(column string, from, to any) QueryBuilder
	OrWhereBetween(column string, from, to any) QueryBuilder
	WhereNotBetween(column string, from, to any) QueryBuilder
	OrWhereNotBetween(column string, from, to any) QueryBuilder

	WhereIn(column string, values []any) QueryBuilder
	OrWhereIn(column string, values []any) QueryBuilder
	WhereNotIn(column string, values []any) QueryBuilder
	OrWhereNotIn(column string, values []any) QueryBuilder

	WhereNull(column string) QueryBuilder
	OrWhereNull(column string) QueryBuilder
	WhereNotNull(column string) QueryBuilder
	OrWhereNotNull(column string) QueryBuilder

	WhereRaw(expr string, args ...any) QueryBuilder
	OrWhereRaw(expr string, args ...any) QueryBuilder

	WhereGroup(fn func(QueryBuilder)) QueryBuilder
	OrWhereGroup(fn func(QueryBuilder)) QueryBuilder

	// Order By
	OrderBy(column, direction string) QueryBuilder
	OrderByRaw(expr string, args ...any) QueryBuilder
	OrderBySafe(userInput, dir string, whitelist map[string]string) QueryBuilder

	// Pagination
	Limit(limit int) QueryBuilder
	Offset(offset int) QueryBuilder

	// Getter
	GetTable() string
	GetColumns() []string
	GetAction() string
	Dialect() Dialect
	Args() []any
	ArgsByIndexes(indexes ...int) []any
	AddArgs(args ...any)
}

type QueryType string

const (
	QueryBasic   QueryType = "Basic"
	QueryBetween QueryType = "Between"
	QueryNested  QueryType = "Nested"
	QueryNull    QueryType = "Null"
	QueryRaw     QueryType = "Raw"
)

type column struct {
	queryType QueryType
	name      string
	expr      string
	args      []any
}

type orderBy struct {
	queryType QueryType
	column    string
	dir       string
	expr      string
	args      []any
}

type where struct {
	queryType QueryType
	column    string
	operator  string
	conj      string
	expr      string
	args      []any
	nested    []where
}

type builder struct {
	dialect  Dialect
	action   string
	table    string
	columns  []column
	wheres   []where
	orderBys []orderBy
	limit    int
	offset   int
	args     []any
}

func New(d Dialect) QueryBuilder {
	return &builder{
		dialect: d,
		limit:   -1,
		offset:  -1,
	}
}

func (b *builder) Dialect() Dialect {
	return b.dialect
}

func (b *builder) GetAction() string {
	return b.action
}

func (b *builder) GetTable() string {
	return b.table
}

func (b *builder) GetColumns() []string {
	if b.columns == nil {
		return nil
	}

	res := make([]string, len(b.columns))
	for i, col := range b.columns {
		if col.queryType == QueryBasic {
			res[i] = col.name
		} else {
			res[i] = col.expr
		}
	}

	return res
}

func (b *builder) Args() []any {
	return b.args
}

func (b *builder) ArgsByIndexes(indexes ...int) []any {
	if len(indexes) == 0 {
		return []any{} // explicitly return empty slice
	}

	res := make([]any, 0, len(indexes))
	for _, i := range indexes {
		if i >= 0 && i < len(b.args) {
			res = append(res, b.args[i])
		}
	}

	return res
}

func (b *builder) AddArgs(args ...any) {
	b.args = append(b.args, args...)
}
