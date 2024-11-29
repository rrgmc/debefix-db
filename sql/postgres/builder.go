package postgres

import (
	"fmt"

	"github.com/rrgmc/debefix-db/v2/sql"
)

type QueryBuilderDialect struct {
}

func (d QueryBuilderDialect) QuoteTable(tableName string) string {
	return quoteIdentifier(tableName)
}

func (d QueryBuilderDialect) QuoteField(fieldName string) string {
	return quoteIdentifier(fieldName)
}

func (d QueryBuilderDialect) NewPlaceholderProvider() sql.QueryBuilderPlaceholderProvider {
	return &QueryBuilderDialectPlaceholderProvider{}
}

// QueryBuilderDialectPlaceholderProvider generates postgres-compatible placeholders ($1, $2).
type QueryBuilderDialectPlaceholderProvider struct {
	c int
}

var _ sql.QueryBuilderPlaceholderProvider = (*QueryBuilderDialectPlaceholderProvider)(nil)

func (p *QueryBuilderDialectPlaceholderProvider) Next() (placeholder string, argName string) {
	p.c++
	return fmt.Sprintf("$%d", p.c), ""
}
