package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A pruned span recounted from the node, then merged by compaction before its settle: the settle finds the span gone
// and changes nothing, rather than replacing the merged span's rows with half its totals.
func TestANodeRecountDoesNotSettleASpanCompactionMergedMeanwhile(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `
		UPDATE prune_state SET pruned_through = 300, pruned_through_ts = 1700000300 WHERE id = 1;
		INSERT INTO ledger_spans (lo, hi, source, check_state) VALUES (0, 100, 'rows', 'ok'), (100, 200, 'rows', 'ok');
		INSERT INTO ledger_chain (lo, name, value) VALUES
			(0, 'blocks_total', 100), (0, 'transactions_total', 7), (0, 'transfers_total', 0),
			(100, 'blocks_total', 100), (100, 'transactions_total', 5), (100, 'transfers_total', 0)`)
	require.NoError(t, err)
	half := NewSpanTotals()
	half.Blocks, half.Transactions = 100, 7

	var merged *int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT ledger_compact(2)`).Scan(&merged))
	require.NotNil(t, merged, "compaction merged [0, 200) while the recount of [0, 100) counted")

	c, err := d.SettleNodeSpan(ctx, 0, 100, half, true)
	require.NoError(t, err)
	require.Zero(t, c.Drifted)
	var blocks, txs int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT value FROM ledger_chain WHERE lo = 0 AND name = 'blocks_total'),
		       (SELECT value FROM ledger_chain WHERE lo = 0 AND name = 'transactions_total')`).Scan(&blocks, &txs))
	require.Equal(t, [2]int64{200, 12}, [2]int64{blocks, txs}, "the merged span keeps its totals")
}

// A retained span's recount whose span compaction merges after the recount staged its totals: the recount starts
// over and never writes its half totals over the merged span.
func TestARetainedRecountDoesNotSettleASpanMergedAfterItsSnapshot(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 1, 300, 1)
	_, err := d.CloseLedgerSpans(ctx, LedgerConfig{SpanBlocks: 100, FirstBlock: 1, WorkMem: "64MB"}, 0)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, 'm' || g, 'm' || (g - 1), 1700000000 + g, 0, 1, 0 FROM generate_series(301, 13500) g`)
	require.NoError(t, err)
	_, err = d.CloseLedgerSpans(ctx, LedgerConfig{SpanBlocks: 100, FirstBlock: 1, WorkMem: "64MB"}, 2)
	require.NoError(t, err)
	var lo, hi, blocks int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT s.lo, s.hi, (SELECT value FROM ledger_chain WHERE lo = s.lo AND name = 'blocks_total')
		FROM ledger_spans s ORDER BY s.checked_at NULLS FIRST, s.lo LIMIT 1`).Scan(&lo, &hi, &blocks))

	merged := false
	retainedRecountStagedHook = func(l int64) {
		if merged || l != lo {
			return
		}
		merged = true
		_, err := d.pool.Exec(ctx, fmt.Sprintf(`
			DELETE FROM ledger_spans WHERE lo = %[2]d;
			UPDATE ledger_spans SET hi = %[3]d, source = 'merged' WHERE lo = %[1]d;
			UPDATE ledger_chain SET value = value + 100 WHERE lo = %[1]d AND name = 'blocks_total'`, lo, hi, hi+100))
		require.NoError(t, err, "a merge of the span with the next one")
	}
	t.Cleanup(func() { retainedRecountStagedHook = nil })
	c, ok, err := d.RecountRetainedSpan(ctx, true, "")
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, merged)
	require.NotEqual(t, "repaired", c.State)
	var got int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT value FROM ledger_chain WHERE lo = $1 AND name = 'blocks_total'`, lo).Scan(&got))
	require.Equal(t, blocks+100, got, "the merged span keeps its totals")
}
