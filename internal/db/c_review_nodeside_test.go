package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Compaction merges verified pruned spans into one; the node recount then takes the merged span first (it was never
// checked) and counts it over its whole range. Its settle must check that span, or the recount takes the same span
// again on every pass and never reaches another one.
func TestANodeRecountSettlesASpanThatCompactionMergedBeforeTheCount(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `
		UPDATE prune_state SET pruned_through = 400, pruned_through_ts = 1700000400 WHERE id = 1;
		INSERT INTO ledger_spans (lo, hi, source, check_state, checked_at) VALUES
			(0, 100, 'rows', 'ok', now() - interval '2 hours'), (100, 200, 'rows', 'ok', now() - interval '2 hours'),
			(200, 300, 'rows', 'ok', now() - interval '1 hour');
		INSERT INTO ledger_chain (lo, name, value) VALUES
			(0, 'blocks_total', 100), (0, 'transactions_total', 7), (0, 'transfers_total', 0),
			(100, 'blocks_total', 100), (100, 'transactions_total', 5), (100, 'transfers_total', 0),
			(200, 'blocks_total', 100), (200, 'transactions_total', 3), (200, 'transfers_total', 0)`)
	require.NoError(t, err)
	merged, ok, err := d.CompactLedger(ctx, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, merged)

	lo, hi, ok, err := d.NextPrunedSpan(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, [2]int64{0, 200}, [2]int64{lo, hi}, "the merged span is recounted first")

	whole := NewSpanTotals()
	whole.Blocks, whole.Transactions = 200, 12
	c, err := d.SettleNodeSpan(ctx, lo, hi, whole, true)
	require.NoError(t, err)
	require.Equal(t, "ok", c.State, "the merged span's correct totals settle it")

	lo, hi, ok, err = d.NextPrunedSpan(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, [2]int64{200, 300}, [2]int64{lo, hi}, "the recount moves on to the next span")
}
