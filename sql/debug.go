package sql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/rrgmc/debefix/v2"
)

// NewDebugQueryInterface returns a QueryInterface that outputs the generated queries.
// If out is nil, [os.Stdout] will be used.
func NewDebugQueryInterface(out io.Writer) QueryInterface {
	if out == nil {
		out = os.Stdout
	}
	return &debugQueryInterface{out: out}
}

type debugQueryInterface struct {
	out         io.Writer
	lastTableID debefix.TableID
}

func (m *debugQueryInterface) Query(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	var retErr error
	var err error

	outTable := tableID

	if m.lastTableID == nil || tableID.TableID() != m.lastTableID.TableID() {
		_, err = fmt.Fprintf(m.out, "%s %s %s\n", strings.Repeat("=", 15), outTable.TableID(), strings.Repeat("=", 15))
		retErr = errors.Join(retErr, err)

		m.lastTableID = tableID
	} else {
		_, _ = fmt.Fprint(m.out, strings.Repeat("-", 20))
	}

	_, err = fmt.Fprintln(m.out, query)
	retErr = errors.Join(retErr, err)

	if len(args) > 0 {
		_, err = fmt.Fprintf(m.out, "$$ ARGS: ")
		retErr = errors.Join(retErr, err)

		err = dumpSlice(m.out, args)
		retErr = errors.Join(retErr, err)

		_, err = fmt.Fprintf(m.out, "\n")
		retErr = errors.Join(retErr, err)
	}

	if retErr != nil {
		return nil, retErr
	}

	return QueryInterfaceCheck(ctx, query, returnFieldNames, args...)
}
