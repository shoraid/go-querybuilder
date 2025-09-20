package sequel

type Dialect interface {
	Capabilities() DialectCapabilities
	Placeholder(n int) string
	WrapColumn(expr string) string
	WrapIdentifier(identifier string) string
	WrapTable(expr string) string

	CompileSelect(b *builder) (string, []any, error)
}

type DialectCapabilities struct {
	SupportsExcept    bool
	SupportsFullJoin  bool
	SupportsIntersect bool
	SupportsReturning bool
}
