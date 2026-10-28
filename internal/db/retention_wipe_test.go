package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// FORCE_REINDEX wipes a database retention has pruned. What was derived from the wiped chain goes with it: the
// watermark, the ledger and so the offsets, the archive records and a staged repair or refill. Partitions cover the
// new chain from block 0 again, below the dropped ranges as above.
func TestWipeAfterAPruneResetsRetentionAndLedgerState(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := legacyPastTheWatermark(t, d)
	finished, err := d.finishDetached(ctx, cfg)
	require.NoError(t, err)
	require.True(t, finished)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO history_archives (suffix, tbl, location, row_count, bytes, sha256) VALUES ('legacy', 'blocks', '/archive/blocks_legacy', 1, 1, 'x')`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `CREATE TABLE `+DailyStatsRefillTable+` (`+refillColumns+`)`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `CREATE UNLOGGED TABLE `+AddressStatsRepairTable+` (address TEXT PRIMARY KEY)`)
	require.NoError(t, err)
	var lowest uint64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT min(lo) FROM history_partitions WHERE state = 'attached'`).Scan(&lowest))
	require.Positive(t, lowest)

	require.NoError(t, d.WipeAllData(ctx))

	through, throughTS, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Zero(t, through, "nothing of the new chain is pruned")
	require.Zero(t, throughTS)
	var spans, offsets, archives int64
	var refill, repair *string
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM ledger_spans), (SELECT count(*) FROM prune_offsets), (SELECT count(*) FROM history_archives),
		       to_regclass($1)::text, to_regclass($2)::text`, DailyStatsRefillTable, AddressStatsRepairTable).
		Scan(&spans, &offsets, &archives, &refill, &repair))
	require.Zero(t, spans, "no old-chain span is closed")
	require.Zero(t, offsets, "no old-chain counts in the new chain's totals")
	require.Zero(t, archives, "no old-chain archive is recorded")
	require.Nil(t, refill, "no old-chain day is staged for a refill")
	require.Nil(t, repair, "no old-chain correction is staged for a repair")
	for _, n := range []uint64{0, 100, cfg.PartitionBlocks, lowest - 1, lowest} {
		require.NoError(t, insertCutTestBlock(d, n), "the new chain's block %d can be stored", n)
	}
}
