package postgres

import (
	"github.com/rrgmc/debefix-db/v2"
	"github.com/rrgmc/debefix-db/v2/sql"
	"github.com/rrgmc/debefix/v2"
)

func ResolveDBFunc(qi sql.QueryInterface) db.ResolveDBCallback {
	return sql.ResolveDBFunc(qi, QueryBuilder())
}

func ResolveFunc(qi sql.QueryInterface) debefix.ResolveCallback {
	return db.ResolveFunc(ResolveDBFunc(qi))
}
