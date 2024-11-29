package db

import (
	"context"
	"testing"

	"github.com/rrgmc/debefix/v2"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

var (
	tableTags     = debefix.TableName("public.tags")
	tablePosts    = debefix.TableName("public.posts")
	tablePostTags = debefix.TableName("public.post_tags")
)

func TestResolve(t *testing.T) {
	ctx := context.Background()

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

	tables := map[string][]map[string]any{}
	var tableOrder []string

	_, err := debefix.Resolve(ctx, data,
		ResolveFunc(func(ctx context.Context, resolveInfo ResolveDBInfo, fields map[string]any,
			returnFieldNames map[string]any) (returnValues map[string]any, err error) {
			tableOrder = append(tableOrder, resolveInfo.TableID.TableID())
			tables[resolveInfo.TableID.TableID()] = append(tables[resolveInfo.TableID.TableID()], fields)
			return nil, nil
		}))
	assert.NilError(t, err)

	assert.DeepEqual(t, []string{"public.tags", "public.tags", "public.posts", "public.posts",
		"public.post_tags", "public.post_tags"}, tableOrder)

	assert.DeepEqual(t, []map[string]any{
		{
			"tag_id":   2,
			"tag_name": "All",
		},
		{
			"tag_id":   5,
			"tag_name": "Half",
		},
	}, tables["public.tags"])

	assert.DeepEqual(t, []map[string]any{
		{
			"post_id": 1,
			"title":   "First post",
		},
		{
			"post_id": 2,
			"title":   "Second post",
		},
	}, tables["public.posts"])

	assert.DeepEqual(t, []map[string]any{
		{
			"post_id": 1,
			"tag_id":  2,
		},
		{
			"post_id": 2,
			"tag_id":  5,
		},
	}, tables["public.post_tags"])
}

func TestResolveGenerated(t *testing.T) {
	ctx := context.Background()

	data := debefix.NewData()

	data.AddValues(tableTags,
		debefix.MapValues{
			"tag_id":   debefix.ResolveValueResolve(),
			"tag_name": "All",
		},
	)

	var tableOrder []string

	_, err := debefix.Resolve(ctx, data,
		ResolveFunc(func(ctx context.Context, resolveInfo ResolveDBInfo, fields map[string]any,
			returnFieldNames map[string]any) (returnValues map[string]any, err error) {
			tableOrder = append(tableOrder, resolveInfo.TableID.TableID())
			assert.Equal(t, resolveInfo.TableID.TableID(), "public.tags")
			assert.Assert(t, is.Contains(returnFieldNames, "tag_id"))
			_, ok := fields["tag_id"]
			assert.Assert(t, !ok, "fields should not containg tag_id")
			return map[string]any{
				"tag_id": 1,
			}, nil
		}))
	assert.NilError(t, err)

	assert.DeepEqual(t, []string{"public.tags"}, tableOrder)
}
