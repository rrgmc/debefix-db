package db

import (
	"context"

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
	returnFields map[string]any) (returnValues map[string]any, err error)

// ResolveFunc is a debefix.ResolveCallback helper to generate database records.
func ResolveFunc(callback ResolveDBCallback) debefix.ResolveCallback {
	return func(ctx context.Context, resolveInfo debefix.ResolveInfo,
		values debefix.ValuesMutable) error {
		fields := map[string]any{}
		returnFields := map[string]any{}

		for fn, fv := range values.All {
			if fresolve, ok := fv.(debefix.ResolveValue); ok {
				switch fr := fresolve.(type) {
				case debefix.ResolveValueResolveData:
					returnFields[fn] = fr.ResolveInfo
				case *debefix.ResolveValueResolveData:
					returnFields[fn] = fr.ResolveInfo
				default:
					return debefix.NewResolveErrorf("unknown ResolveValue type: %T ", fresolve)
				}
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
			values.Set(rn, rv)
		}

		return nil
	}
}
