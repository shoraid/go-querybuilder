package goquerybuilder

type QueryBuilder interface {
	// Core
	Select(columns ...string) QueryBuilder
	SelectSafe(userInput []string, whitelist map[string]string) (QueryBuilder, error)
	AddSelect(column string) QueryBuilder
	AddSelectSafe(userInput string, whitelist map[string]string) (QueryBuilder, error)
	From(table string) QueryBuilder
	FromSafe(userInput string, whitelist map[string]string) (QueryBuilder, error)
	ToSQL() (string, []any, error)

	// Where
	Where(column string, operator string, value any) QueryBuilder
	OrWhere(column string, operator string, value any) QueryBuilder

	// Order
	OrderBy(column string, direction string) QueryBuilder
	OrderByRaw(expr string) QueryBuilder
	OrderBySafe(userInput string, dir string, whitelist map[string]string) (QueryBuilder, error)

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

type condition struct {
	conj       string
	query      string
	argIndexes []int
}

type builder struct {
	dialect  Dialect
	action   string
	table    string
	columns  []string
	wheres   []condition
	orderBys []string
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
	return b.columns
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
