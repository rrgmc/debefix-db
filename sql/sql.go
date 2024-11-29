package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/rrgmc/debefix/v2"
)

// DB is an abstraction over [sql.DB] or similar..
type DB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// NewSQLQueryInterface returns a QueryInterface for the passed database.
func NewSQLQueryInterface(db DB) QueryInterface {
	return &sqlQueryInterface{
		db: db,
	}
}

type sqlQueryInterface struct {
	db DB
}

var _ QueryInterface = (*sqlQueryInterface)(nil)

func (q *sqlQueryInterface) Query(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	if len(returnFieldNames) == 0 {
		_, err := q.db.ExecContext(ctx, query, args...)
		return nil, err
	}

	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, errors.New("no records on query")
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	ret, err := rowToMap(cols, rows)
	if err != nil {
		return nil, err
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return ret, nil
}
