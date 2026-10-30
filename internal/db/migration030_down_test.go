package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// 030 down keeps merged spans and labels each by where its rows were counted: from the node by the stats backfill
// when the span is wholly below START_BLOCK, from stored rows otherwise.
func TestMigration030DownRelabelsMergedSpans(t *testing.T) {
	pool, _ := emptyTestDB(t)
	ctx := context.Background()
	migrateTo(t, pool, -1)
	_, err := pool.Exec(ctx, `
		INSERT INTO stats_backfill (id, target, through) VALUES (1, 199, 199);
		INSERT INTO ledger_spans (lo, hi, source) VALUES (0, 200, 'merged'), (200, 400, 'merged'), (400, 500, 'rows')`)
	require.NoError(t, err)
	migrateTo(t, pool, 29)
	rows, err := pool.Query(ctx, `SELECT lo::TEXT || ' ' || source FROM ledger_spans ORDER BY lo`)
	require.NoError(t, err)
	var got []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		got = append(got, s)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"0 node", "200 rows", "400 rows"}, got)
}
