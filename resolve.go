package db

import (
	"context"
	"fmt"

	"github.com/rrgmc/debefix/v2"
)

// ResolveDBInfo is the context for one database resolve operation.
type ResolveDBInfo struct {
	Type            debefix.ResolveType
	TableID         debefix.TableID
	UpdateKeyFields []string
}

// ResolveDBCallback will be called for each table row to be inserted.
// fields are the fields to be inserted.
// returnFieldNames are the fields whose values are expected to be returned in the return map. Their values may
// contain a resolve info if sent by the caller.
type ResolveDBCallback func(ctx context.Context, resolveInfo ResolveDBInfo, fields map[string]any,
	returnFields map[string]debefix.ResolveValue) (returnValues map[string]any, err error)

// ResolveFunc is a [debefix.ResolveCallback] helper to generate database records.
func ResolveFunc(callback ResolveDBCallback) debefix.ResolveCallback {
	return func(ctx context.Context, resolveInfo debefix.ResolveInfo,
		values debefix.ValuesMutable) error {
		fields := map[string]any{}
		returnFields := map[string]debefix.ResolveValue{}

		for fn, fv := range values.All {
			if fresolve, ok := fv.(debefix.ResolveValue); ok {
				returnFields[fn] = fresolve
			} else {
				fields[fn] = fv
			}
		}

		resolved, err := callback(ctx, ResolveDBInfo{
			Type:            resolveInfo.Type,
			TableID:         resolveInfo.TableID,
			UpdateKeyFields: resolveInfo.UpdateKeyFields,
		}, fields, returnFields)
		if err != nil {
			return err
		}

		for rn, rv := range resolved {
			if rvp, ok := returnFields[rn]; ok {
				rv, err = rvp.ResolveValueParse(ctx, rv)
				if err != nil {
					return fmt.Errorf("error parsing resolve value '%s': %w", rn, err)
				}
			}
			values.Set(rn, rv)
		}

		return nil
	}
}
