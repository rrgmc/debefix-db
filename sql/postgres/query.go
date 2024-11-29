package postgres

import (
	"github.com/rrgmc/debefix-db/v2/sql"
)

// QueryBuilder returns a postgres-compatible sql.QueryBuilder
func QueryBuilder() sql.QueryBuilder {
	return sql.NewQueryBuilder(QueryBuilderDialect{})
}
