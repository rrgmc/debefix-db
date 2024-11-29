package sql

import (
	"context"

	"github.com/google/uuid"
	"github.com/rrgmc/debefix/v2"
)

// QueryInterface abstracts executing a query in a database.
type QueryInterface interface {
	Query(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error)
}

// QueryInterfaceFunc is a func adapter for QueryInterface
type QueryInterfaceFunc func(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error)

var _ QueryInterface = (QueryInterfaceFunc)(nil)

func (f QueryInterfaceFunc) Query(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	return f(ctx, tableID, query, returnFieldNames, args...)
}

// QueryInterfaceCheck generates a simulated response for QueryInterface.Query
func QueryInterfaceCheck(ctx context.Context, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	ret := map[string]any{}
	for _, fn := range returnFieldNames {
		// simulate fields being generated
		ret[fn] = uuid.New()
	}
	return ret, nil
}
