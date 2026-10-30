package db

import (
	"context"
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
