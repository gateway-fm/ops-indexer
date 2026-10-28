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

// The wipe recreates the pruned ranges' partitions one transaction at a time. Stopped after the first one, the
// next start finds the watermark at 0 and no blocks, so it does not wipe again; the start's ensure fills every
// gap between block 0 and the highest partition, and ingest goes past the first range.
func TestAStartFillsThePartitionsAnInterruptedWipeLeftOut(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := legacyPastTheWatermark(t, d)
	finished, err := d.finishDetached(ctx, cfg)
	require.NoError(t, err)
	require.True(t, finished)
	var lowest uint64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT min(lo) FROM history_partitions WHERE state = 'attached'`).Scan(&lowest))
	require.NoError(t, d.WipeAllData(ctx))

	// The state of a wipe stopped after its first range: only [0, first) of the ranges below lowest exists.
	rows, err := d.pool.Query(ctx, `SELECT suffix FROM history_partitions WHERE lo > 0 AND lo < $1`, int64(lowest))
	require.NoError(t, err)
	var later []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		later = append(later, s)
	}
	rows.Close()
	require.NotEmpty(t, later)
	for _, s := range later {
		require.NoError(t, d.dropRange(ctx, s, 0))
	}
	var first uint64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT hi FROM history_partitions WHERE lo = 0`).Scan(&first))
	require.Error(t, insertCutTestBlock(d, first), "the interrupted wipe left no partition above its first range")

	_, err = d.EnsurePartitionsAhead(ctx, 0, cfg.PartitionBlocks)
	require.NoError(t, err)
	for _, n := range []uint64{0, first, first + cfg.PartitionBlocks, lowest - 1, lowest} {
		require.NoError(t, insertCutTestBlock(d, n), "block %d after the start's ensure", n)
	}
	made, err := d.EnsurePartitionsAhead(ctx, 0, cfg.PartitionBlocks)
	require.NoError(t, err)
	require.Zero(t, made, "a second ensure finds no gap")
}
