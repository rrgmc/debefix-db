package sql

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/rrgmc/debefix-db/v2"
	"github.com/rrgmc/debefix/v2"
)

// QueryBuilder is an abstraction for building SQL queries.
type QueryBuilder interface {
	BuildSQL(ctx context.Context, resolveInfo db.ResolveDBInfo, fields map[string]any, returnFieldNames map[string]debefix.ResolveValue) (string, []any, error)
}

// QueryBuilderDialect represents a database dialect used to build queries.
type QueryBuilderDialect interface {
	QuoteTable(tableName string) string
	QuoteField(fieldName string) string
	NewPlaceholderProvider() QueryBuilderPlaceholderProvider
}

// QueryBuilderPlaceholderProvider is a helper for generating database placeholders.
type QueryBuilderPlaceholderProvider interface {
	Next() (placeholder string, argName string)
}

// BuildQuery builds a query string and arguments.
func BuildQuery(dialect QueryBuilderDialect, resolveInfo db.ResolveDBInfo, fields map[string]any,
	returnFieldNames map[string]debefix.ResolveValue) (string, []any, error) {
	switch resolveInfo.Type {
	case debefix.ResolveTypeAdd:
		return buildInsertQuery(dialect, resolveInfo, fields, returnFieldNames)
	case debefix.ResolveTypeUpdate:
		return buildUpdateQuery(dialect, resolveInfo, fields, returnFieldNames)
	default:
		return "", nil, fmt.Errorf("unknown resolve type: %v", resolveInfo.Type)
	}
}

func buildInsertQuery(dialect QueryBuilderDialect, resolveInfo db.ResolveDBInfo, fields map[string]any,
	returnFields map[string]debefix.ResolveValue) (string, []any, error) {
	tn := dialect.QuoteTable(resolveInfo.TableID.TableName())

	placeholderProvider := dialect.NewPlaceholderProvider()

	var fieldNames []string
	var returnFieldNames []string
	var placeholders []string
	var args []any

	fieldNames = slices.Collect(maps.Keys(fields))
	returnFieldNames = slices.Collect(maps.Keys(returnFields))

	slices.Sort(fieldNames)
	slices.Sort(returnFieldNames)

	for _, fn := range fieldNames {
		placeholder, argName := placeholderProvider.Next()
		placeholders = append(placeholders, placeholder)
		fv, ok := fields[fn]
		if !ok {
			return "", nil, fmt.Errorf("field %s is not set", fn)
		}
		if argName != "" {
			args = append(args, sql.Named(argName, fv))
		} else {
			args = append(args, fv)
		}
	}

	fieldNames = sliceMapFunc(fieldNames, func(s string) string { return dialect.QuoteField(s) })
	returnFieldNames = sliceMapFunc(returnFieldNames, func(s string) string { return dialect.QuoteField(s) })

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tn,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "),
	)

	if len(returnFieldNames) > 0 {
		query += fmt.Sprintf(" RETURNING %s", strings.Join(returnFieldNames, ","))
	}

	return query, args, nil
}

func buildUpdateQuery(dialect QueryBuilderDialect, resolveInfo db.ResolveDBInfo, fields map[string]any,
	returnFields map[string]debefix.ResolveValue) (string, []any, error) {
	tn := dialect.QuoteTable(resolveInfo.TableID.TableName())

	placeholderProvider := dialect.NewPlaceholderProvider()

	var keyFieldNames []string
	var fieldNames []string
	var returnFieldNames []string
	var placeholders []string
	var keyFieldPlaceholders []string
	var args []any

	keyFieldNames = slices.Clone(resolveInfo.UpdateKeyFields)
	returnFieldNames = slices.Collect(maps.Keys(returnFields))

	for fn, _ := range fields {
		if slices.Contains(resolveInfo.UpdateKeyFields, fn) {
			continue
		}
		fieldNames = append(fieldNames, fn)
	}

	if len(keyFieldNames) == 0 {
		return "", nil, fmt.Errorf("no key fields found for update in '%s'", resolveInfo.TableID.TableID())
	}

	slices.Sort(keyFieldNames)
	slices.Sort(fieldNames)
	slices.Sort(returnFieldNames)

	for _, fn := range fieldNames {
		placeholder, argName := placeholderProvider.Next()
		placeholders = append(placeholders, placeholder)
		fv, ok := fields[fn]
		if !ok {
			return "", nil, fmt.Errorf("field %s is not set", fn)
		}
		if argName != "" {
			args = append(args, sql.Named(argName, fv))
		} else {
			args = append(args, fv)
		}
	}
	for _, fn := range keyFieldNames {
		placeholder, argName := placeholderProvider.Next()
		keyFieldPlaceholders = append(keyFieldPlaceholders, placeholder)
		fv, ok := fields[fn]
		if !ok {
			return "", nil, fmt.Errorf("field %s is not set", fn)
		}
		if argName != "" {
			args = append(args, sql.Named(argName, fv))
		} else {
			args = append(args, fv)
		}
	}

	fieldNames = sliceMapFunc(fieldNames, func(s string) string { return dialect.QuoteField(s) })
	keyFieldNames = sliceMapFunc(keyFieldNames, func(s string) string { return dialect.QuoteField(s) })
	returnFieldNames = sliceMapFunc(returnFieldNames, func(s string) string { return dialect.QuoteField(s) })

	var whereFields []string
	var setFields []string
	for fidx, fieldName := range fieldNames {
		setFields = append(setFields, fmt.Sprintf("%s = %s", fieldName, placeholders[fidx]))
	}
	for fidx, fieldName := range keyFieldNames {
		whereFields = append(whereFields, fmt.Sprintf("%s = %s", fieldName, keyFieldPlaceholders[fidx]))
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tn,
		strings.Join(setFields, ", "),
		strings.Join(whereFields, ", "),
	)

	if len(returnFieldNames) > 0 {
		query += fmt.Sprintf(" RETURNING %s", strings.Join(returnFieldNames, ","))
	}

	return query, args, nil
}

// NewQueryBuilder returns a QueryBuilder which uses the passed database dialect.
func NewQueryBuilder(dialect QueryBuilderDialect) QueryBuilder {
	return &queryBuilder{Dialect: dialect}
}

type queryBuilder struct {
	Dialect QueryBuilderDialect
}

func (b queryBuilder) BuildSQL(ctx context.Context, resolveInfo db.ResolveDBInfo, fields map[string]any,
	returnFieldNames map[string]debefix.ResolveValue) (string, []any, error) {
	return BuildQuery(b.Dialect, resolveInfo, fields, returnFieldNames)
}

// DefaultQueryBuilderDialect returns placeholders using ? and unquoted table and field names.
type DefaultQueryBuilderDialect struct {
}

func (d DefaultQueryBuilderDialect) QuoteTable(tableName string) string {
	return tableName
}

func (d DefaultQueryBuilderDialect) QuoteField(fieldName string) string {
	return fieldName
}

func (d DefaultQueryBuilderDialect) NewPlaceholderProvider() QueryBuilderPlaceholderProvider {
	return defaultQueryBuilderPlaceholderProvider{}
}

// defaultQueryBuilderPlaceholderProvider returns placeholders using ?
type defaultQueryBuilderPlaceholderProvider struct {
}

func (d defaultQueryBuilderPlaceholderProvider) Next() (placeholder string, argName string) {
	return "?", ""
}
