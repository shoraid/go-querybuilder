package goquerybuilder

type Dialect interface {
	Placeholder(n int) string
	QuoteIdentifier(identifier string) string
	QuoteTableWithAlias(expr string) string
	QuoteColumnWithAlias(expr string) string
	Capabilities() DialectCapabilities
}

type DialectCapabilities struct {
	SupportsReturning bool
	SupportsFullJoin  bool
	SupportsIntersect bool
	SupportsExcept    bool
}
