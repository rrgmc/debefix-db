package sql

import (
	"errors"
	"fmt"
	"io"
)

// rowInterface abstracts [sql.Row].
type rowInterface interface {
	Scan(dest ...any) error
}

// rowToMap converts a database row to a map[string]any.
func rowToMap(cols []string, row rowInterface) (map[string]any, error) {
	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i, _ := range columns {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := row.Scan(columnPointers...); err != nil {
		return nil, err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	m := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}

	return m, nil
}

// dumpSlice outputs a slice to the writer.
func dumpSlice(out io.Writer, s []any) error {
	var allErr error
	var err error

	for i, v := range s {
		prefix := ""
		if i > 0 {
			prefix = " "
		}

		_, err = fmt.Fprintf(out, `%s[%d:"%v"]`, prefix, i, v)
		allErr = errors.Join(allErr, err)
	}

	return allErr
}

// dumpMap outputs a map to the writer.
func dumpMap(out io.Writer, s map[string]any) error {
	var allErr error
	var err error

	first := true
	for i, v := range s {
		prefix := ""
		if !first {
			prefix = " "
		}
		first = false

		_, err = fmt.Fprintf(out, `%s[%s:"%v"]`, prefix, i, v)
		allErr = errors.Join(allErr, err)
	}

	return allErr
}

// sliceMapFunc calls a function to change the value of each slice item.
func sliceMapFunc[S any, T any](items []S, mapper func(S) T) []T {
	mapped := make([]T, len(items))
	for i, item := range items {
		mapped[i] = mapper(item)
	}
	return mapped
}
