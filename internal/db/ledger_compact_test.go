package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactionMergesVerifiedPrunedSpansWithoutMovingTotals(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 16_000, 1)
	require.NoError(t, d.Migrate())
	require.NoError(t, d.RebuildAddressStats(ctx))
	closeLedger(t, d)
	require.Equal(t, [][2]int64{{0, 1000}, {1000, 2000}, {2000, 3000}}, ledgerSpans(t, d))
	_, err := d.pool.Exec(ctx, `UPDATE prune_state SET pruned_through = 2999`)
	require.NoError(t, err)
	offsets := func() (rows []string) {
		r, err := d.pool.Query(ctx, `SELECT kind || ' ' || key || ' ' || count || ' ' || supply FROM prune_offsets ORDER BY 1`)
		require.NoError(t, err)
		defer r.Close()
		for r.Next() {
			var s string
			require.NoError(t, r.Scan(&s))
			rows = append(rows, s)
		}
		return rows
	}
	addressRows := func() (n int64) {
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM ledger_address`).Scan(&n))
		return n
	}
	before, rowsBefore := offsets(), addressRows()

	_, ok, err := d.CompactLedger(ctx, 3)
	require.NoError(t, err)
	require.False(t, ok, "spans the node recount has not verified are not merged")

	_, err = d.pool.Exec(ctx, `UPDATE ledger_spans SET check_state = 'ok', checked_at = now()`)
	require.NoError(t, err)
	lo, ok, err := d.CompactLedger(ctx, 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(0), lo)
	require.Equal(t, [][2]int64{{0, 3000}}, ledgerSpans(t, d))
	require.Equal(t, before, offsets(), "pruned totals do not move")
	require.Equal(t, rowsBefore/3, addressRows(), "an address active in all three spans keeps one row")
	var state string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT check_state FROM ledger_spans WHERE lo = 0`).Scan(&state))
	require.Equal(t, "unchecked", state, "the merged span is recounted from the node again")
	require.Empty(t, checkAllBuckets(t, d, false), "the running counters agree")

	_, ok, err = d.CompactLedger(ctx, 3)
	require.NoError(t, err)
	require.False(t, ok, "a merged span is not merged again")
}
