package sql

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/rrgmc/debefix-db/v2"
	"github.com/rrgmc/debefix/v2"
)

// ResolveDBFunc is a db.ResolveDBCallback helper to generate SQL database records.
func ResolveDBFunc(qi QueryInterface, queryBuilder QueryBuilder) db.ResolveDBCallback {
	return func(ctx context.Context, resolveInfo db.ResolveDBInfo, fields map[string]any,
		returnFieldNames map[string]debefix.ResolveValue) (returnValues map[string]any, err error) {

		query, args, err := queryBuilder.BuildSQL(ctx, resolveInfo, fields, returnFieldNames)
		if err != nil {
			return nil, err
		}

		ret, err := qi.Query(ctx, resolveInfo.TableID, query, slices.Collect(maps.Keys(returnFieldNames)), args...)
		if err != nil {
			return nil, fmt.Errorf("error executing query `%s`: %w", query, err)
		}

		return ret, nil
	}
}

// ResolveFunc is a debefix.ResolveCallback helper to generate SQL database records.
func ResolveFunc(qi QueryInterface, queryBuilder QueryBuilder) debefix.ResolveCallback {
	return db.ResolveFunc(ResolveDBFunc(qi, queryBuilder))
}
