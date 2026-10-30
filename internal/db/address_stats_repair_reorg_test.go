package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A repair staged before a reorg can hold a correction for an address the reorg then removed (no remaining block had
// seen it). The repair stages again and does not bring the row back: an address with no rows left has no row, as
// after the revert. Corrections for addresses that still have rows apply.
func TestAddressStatsRepairDoesNotRecreateARowAReorgRemoved(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xf"}})))
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 5 WHERE address IN ('0xa', '0xf')`)
	require.NoError(t, err, "drift for the repair to correct")

	stage, err := d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.Equal(t, int64(2), stage.Changes)
	require.NoError(t, d.DeleteBlock(ctx, 2))

	require.Equal(t, 1, repairUntilDone(t, d), "the reorg made the stage stale")

	var rows int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM address_stats WHERE address = '0xf'`).Scan(&rows))
	require.Zero(t, rows, "the reorg removed 0xf's row; the repair does not bring it back")
	var txs int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT tx_count FROM address_stats WHERE address = '0xa'`).Scan(&txs))
	require.Equal(t, int64(1), txs, "0xa: its one remaining transaction")
	var total, counted int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count FROM chain_counters WHERE name = 'addresses_total'), (SELECT count(*) FROM address_stats)`).
		Scan(&total, &counted))
	require.Equal(t, counted, total, "addresses_total counts the rows")
}
