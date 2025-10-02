package sequel

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
	Where(column string, operator string, values ...any) QueryBuilder
	OrWhere(column string, operator string, values ...any) QueryBuilder

	WhereBetween(column string, from, to any) QueryBuilder
	OrWhereBetween(column string, from, to any) QueryBuilder
	WhereNotBetween(column string, from, to any) QueryBuilder
	OrWhereNotBetween(column string, from, to any) QueryBuilder

	WhereIn(column string, values ...any) QueryBuilder
	OrWhereIn(column string, values ...any) QueryBuilder
	WhereNotIn(column string, values ...any) QueryBuilder
	OrWhereNotIn(column string, values ...any) QueryBuilder

	WhereNull(column string) QueryBuilder
	OrWhereNull(column string) QueryBuilder
	WhereNotNull(column string) QueryBuilder
	OrWhereNotNull(column string) QueryBuilder

	WhereRaw(expr string, args ...any) QueryBuilder
	OrWhereRaw(expr string, args ...any) QueryBuilder

	WhereGroup(fn func(QueryBuilder)) QueryBuilder
	OrWhereGroup(fn func(QueryBuilder)) QueryBuilder

	WhereSub(column, operator string, sub func(QueryBuilder)) QueryBuilder
	OrWhereSub(column, operator string, sub func(QueryBuilder)) QueryBuilder

	WhereExists(sub func(QueryBuilder)) QueryBuilder
	OrWhereExists(sub func(QueryBuilder)) QueryBuilder
	WhereNotExists(sub func(QueryBuilder)) QueryBuilder
	OrWhereNotExists(sub func(QueryBuilder)) QueryBuilder

	// Order By
	OrderBy(column, direction string) QueryBuilder
	OrderByRaw(expr string, args ...any) QueryBuilder
	OrderBySafe(userInput, dir string, whitelist map[string]string) QueryBuilder

	// Pagination
	Limit(limit int) QueryBuilder
	Offset(offset int) QueryBuilder

	// Getter
	GetTable() string
	GetAction() string
	Dialect() Dialect
}

type QueryType uint8

const (
	QueryBasic   QueryType = 1
	QueryBetween QueryType = 2
	QueryIn      QueryType = 3
	QueryNested  QueryType = 4
	QueryNull    QueryType = 5
	QueryRaw     QueryType = 6
	QuerySub     QueryType = 7
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
	sub       QueryBuilder
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
	err      error
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
