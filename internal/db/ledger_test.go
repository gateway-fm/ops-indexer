package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func ledgerSpans(t *testing.T, d *DB) [][2]int64 {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `SELECT lo, hi FROM ledger_spans ORDER BY lo`)
	require.NoError(t, err)
	defer rows.Close()
	var out [][2]int64
	for rows.Next() {
		var s [2]int64
		require.NoError(t, rows.Scan(&s[0], &s[1]))
		out = append(out, s)
	}
	return out
}

// Closed spans are a prefix of the chain, each final (below the reorg floor) and complete, and a total is the
// sum of their rows plus what the open spans' rows hold.
func TestLedgerClosesFinalCompleteSpansInOrder(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 2_499, 1)
	seedHistory(t, d, 2_600, 17_000, 1)

	closeLedger(t, d)
	require.Equal(t, [][2]int64{{0, 1000}, {1000, 2000}}, ledgerSpans(t, d), "stops at the span missing 2,500..2,599")

	seedHistory(t, d, 2_500, 2_599, 1)
	closeLedger(t, d)
	spans := ledgerSpans(t, d)
	require.Equal(t, [2]int64{3000, 4000}, spans[len(spans)-1], "17,000 − 12,800: [3,000, 4,000) is the last final span")

	var ledgerSum, rows int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT sum(value) FROM ledger_chain WHERE name = 'transactions_total'`).Scan(&ledgerSum))
	require.Equal(t, int64(4000), ledgerSum)
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT sum(transfers) FROM ledger_token) + (SELECT count(*) FROM token_transfers WHERE block_number >= 4000)`).Scan(&rows))
	require.Equal(t, int64(17_001), rows, "closed token rows + open rows = every transfer")
}

func TestLedgerSpansSplitAtPartitionBounds(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 60_001, 1)

	closeLedger(t, d)
	spans := ledgerSpans(t, d)
	require.Contains(t, spans, [2]int64{44_000, 44_001}, "the cut at 44,001 is a span bound")
	require.Contains(t, spans, [2]int64{44_001, 45_000})
	require.Contains(t, spans, [2]int64{46_000, 46_001}, "so is every partition bound")
}

func TestPruneRefusesARangeNotClosedInTheLedger(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)

	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Zero(t, wm, "the pass waits for the ledger")
	_, err = d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
	require.ErrorContains(t, err, "not closed in the ledger")
}

func TestMigration019RefusesFoldedOffsets(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	m, release := testMigrator(t, d)
	defer release()
	require.NoError(t, m.MigrateTo(ctx, 18))
	_, err := d.pool.Exec(ctx, `INSERT INTO prune_offsets (kind, key, count) VALUES ('chain', 'blocks_total', 5)`)
	require.NoError(t, err)

	require.ErrorContains(t, m.Migrate(ctx), "cannot convert folded prune_offsets")
	_, err = d.pool.Exec(ctx, `DELETE FROM prune_offsets`)
	require.NoError(t, err)
	require.NoError(t, m.Migrate(ctx))
	require.NoError(t, m.MigrateTo(ctx, 18), "reverts while nothing is pruned")
	require.NoError(t, m.Migrate(ctx))
}
