package postgres

import (
	"context"
	"testing"

	"github.com/rrgmc/debefix-db/v2/sql"
	"github.com/rrgmc/debefix/v2"
	"gotest.tools/v3/assert"
)

var (
	tableTags     = debefix.TableName("public.tags")
	tablePosts    = debefix.TableName("public.posts")
	tablePostTags = debefix.TableName("public.post_tags")
)

func TestResolve(t *testing.T) {
	data := debefix.NewData()

	data.AddValues(tableTags,
		debefix.MapValues{
			"tag_id":   2,
			"_refid":   debefix.SetValueRefID("all"),
			"tag_name": "All",
		},
		debefix.MapValues{
			"tag_id":   5,
			"_refid":   debefix.SetValueRefID("half"),
			"tag_name": "Half",
		},
	)

	data.AddValues(tablePosts,
		debefix.MapValues{
			"post_id": 1,
			"_refid":  debefix.SetValueRefID("post_1"),
			"title":   "First post",
		},
		debefix.MapValues{
			"post_id": 2,
			"_refid":  debefix.SetValueRefID("post_2"),
			"title":   "Second post",
		},
	)

	data.AddDependencies(tablePosts, tableTags)

	data.AddValues(debefix.TableName(tablePostTags),
		debefix.MapValues{
			"post_id": debefix.ValueRefID(tablePosts, "post_1", "post_id"),
			"tag_id":  debefix.ValueRefID(tableTags, "all", "tag_id"),
		},
		debefix.MapValues{
			"post_id": debefix.ValueRefID(tablePosts, "post_2", "post_id"),
			"tag_id":  debefix.ValueRefID(tableTags, "half", "tag_id"),
		},
	)

	type sqlQuery struct {
		SQL  string
		Args []any
	}

	expectedQueryList := []sqlQuery{
		{
			SQL:  `INSERT INTO "public.tags" ("tag_id", "tag_name") VALUES ($1, $2)`,
			Args: []any{2, "All"},
		},
		{
			SQL:  `INSERT INTO "public.tags" ("tag_id", "tag_name") VALUES ($1, $2)`,
			Args: []any{5, "Half"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "title") VALUES ($1, $2)`,
			Args: []any{1, "First post"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "title") VALUES ($1, $2)`,
			Args: []any{2, "Second post"},
		},
		{
			SQL:  `INSERT INTO "public.post_tags" ("post_id", "tag_id") VALUES ($1, $2)`,
			Args: []any{1, 2},
		},
		{
			SQL:  `INSERT INTO "public.post_tags" ("post_id", "tag_id") VALUES ($1, $2)`,
			Args: []any{2, 5},
		},
	}

	ctx := context.Background()

	var queryList []sqlQuery

	_, err := debefix.Resolve(ctx, data, ResolveFunc(
		sql.QueryInterfaceFunc(func(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})
			return nil, nil
		})))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)
}

func TestResolveGenerated(t *testing.T) {
	data := debefix.NewData()

	data.AddValues(tableTags,
		debefix.MapValues{
			"tag_id":   debefix.ResolveValueResolve(),
			"_refid":   debefix.SetValueRefID("all"),
			"tag_name": "All",
		},
		debefix.MapValues{
			"tag_id":   debefix.ResolveValueResolve(),
			"_refid":   debefix.SetValueRefID("half"),
			"tag_name": "Half",
		},
	)

	data.AddValues(tablePosts,
		debefix.MapValues{
			"post_id": 1,
			"_refid":  debefix.SetValueRefID("post_1"),
			"title":   "First post",
		},
		debefix.MapValues{
			"post_id": 2,
			"_refid":  debefix.SetValueRefID("post_2"),
			"title":   "Second post",
		},
	)

	data.AddDependencies(tablePosts, tableTags)

	data.AddValues(tablePostTags,
		debefix.MapValues{
			"post_id": debefix.ValueRefID(tablePosts, "post_1", "post_id"),
			"tag_id":  debefix.ValueRefID(tableTags, "all", "tag_id"),
		},
		debefix.MapValues{
			"post_id": debefix.ValueRefID(tablePosts, "post_2", "post_id"),
			"tag_id":  debefix.ValueRefID(tableTags, "half", "tag_id"),
		},
	)

	type sqlQuery struct {
		SQL  string
		Args []any
	}

	expectedQueryList := []sqlQuery{
		{
			SQL:  `INSERT INTO "public.tags" ("tag_name") VALUES ($1) RETURNING "tag_id"`,
			Args: []any{"All"},
		},
		{
			SQL:  `INSERT INTO "public.tags" ("tag_name") VALUES ($1) RETURNING "tag_id"`,
			Args: []any{"Half"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "title") VALUES ($1, $2)`,
			Args: []any{1, "First post"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "title") VALUES ($1, $2)`,
			Args: []any{2, "Second post"},
		},
		{
			SQL:  `INSERT INTO "public.post_tags" ("post_id", "tag_id") VALUES ($1, $2)`,
			Args: []any{1, 116},
		},
		{
			SQL:  `INSERT INTO "public.post_tags" ("post_id", "tag_id") VALUES ($1, $2)`,
			Args: []any{2, 117},
		},
	}

	ctx := context.Background()

	retTagID := 115

	var queryList []sqlQuery

	_, err := debefix.Resolve(ctx, data, ResolveFunc(
		sql.QueryInterfaceFunc(func(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})

			ret := map[string]any{}
			for _, rf := range returnFieldNames {
				if rf == "tag_id" {
					retTagID++
					ret["tag_id"] = retTagID
				}
			}

			return ret, nil
		})))
	assert.NilError(t, err)
	assert.DeepEqual(t, expectedQueryList, queryList)
}

func TestResolveUpdate(t *testing.T) {
	data := debefix.NewData()

	tagIID := data.AddWithID(tableTags,
		debefix.MapValues{
			"tag_id":   2,
			"_refid":   debefix.SetValueRefID("all"),
			"tag_name": "All",
		},
	).ValueForField("tag_id")

	data.Update(tagIID.UpdateQuery([]string{"tag_id"}),
		debefix.UpdateActionSetValues{Values: debefix.MapValues{
			"tag_name": "All updated",
		}})

	data.Add(tablePosts,
		debefix.MapValues{
			"post_id": 1,
			"_refid":  debefix.SetValueRefID("post_1"),
			"title":   "First post",
			"tag_id":  debefix.ValueRefID(tableTags, "all", "tag_id"),
		},
	)

	type sqlQuery struct {
		SQL  string
		Args []any
	}

	expectedQueryList := []sqlQuery{
		{
			SQL:  `INSERT INTO "public.tags" ("tag_id", "tag_name") VALUES ($1, $2)`,
			Args: []any{2, "All"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "tag_id", "title") VALUES ($1, $2, $3)`,
			Args: []any{1, 2, "First post"},
		},
		{
			SQL:  `UPDATE "public.tags" SET "tag_name" = $1 WHERE "tag_id" = $2`,
			Args: []any{"All updated", 2},
		},
	}

	ctx := context.Background()

	var queryList []sqlQuery

	_, err := debefix.Resolve(ctx, data, ResolveFunc(
		sql.QueryInterfaceFunc(func(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})
			return nil, nil
		})))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)
}

func TestResolveUpdateOrder(t *testing.T) {
	data := debefix.NewData()

	tagsIID := data.AddWithID(tableTags,
		debefix.MapValues{
			"tag_id":   2,
			"_refid":   debefix.SetValueRefID("all"),
			"tag_name": "All",
		},
	)

	data.UpdateAfter(tagsIID,
		tagsIID.UpdateQuery([]string{"tag_id"}),
		debefix.UpdateActionSetValues{Values: debefix.MapValues{
			"tag_name": "All updated",
		}})

	data.Add(tablePosts,
		debefix.MapValues{
			"post_id": 1,
			"_refid":  debefix.SetValueRefID("post_1"),
			"title":   "First post",
			"tag_id":  debefix.ValueRefID(tableTags, "all", "tag_id"),
		},
	)

	type sqlQuery struct {
		SQL  string
		Args []any
	}

	expectedQueryList := []sqlQuery{
		{
			SQL:  `INSERT INTO "public.tags" ("tag_id", "tag_name") VALUES ($1, $2)`,
			Args: []any{2, "All"},
		},
		{
			SQL:  `UPDATE "public.tags" SET "tag_name" = $1 WHERE "tag_id" = $2`,
			Args: []any{"All updated", 2},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "tag_id", "title") VALUES ($1, $2, $3)`,
			Args: []any{1, 2, "First post"},
		},
	}

	ctx := context.Background()

	var queryList []sqlQuery

	_, err := debefix.Resolve(ctx, data, ResolveFunc(
		sql.QueryInterfaceFunc(func(ctx context.Context, tableID debefix.TableID, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})
			return nil, nil
		})))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)
}
