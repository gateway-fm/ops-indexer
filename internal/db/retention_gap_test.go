package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func dailyStatsWatermark(t *testing.T, d *DB) int64 {
	t.Helper()
	var wm int64
	require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT last_block FROM daily_stats_watermark WHERE id = 1`).Scan(&wm))
	return wm
}

func setDailyStatsWatermark(t *testing.T, d *DB, wm int64) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), `UPDATE daily_stats_watermark SET last_block = $1 WHERE id = 1`, wm)
	require.NoError(t, err)
}

// A catch-up from genesis with retention on: the head path commits at the tip while catch-up fills the oldest
// blocks. daily_stats starts aggregating at the first stored block of the newest day, so its watermark sits above
// blocks catch-up has not stored yet and cannot tell the prune that a range is incomplete.
func TestDailyStatsWatermarkStartsAboveBlocksCatchupHasNotStored(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	seedHistoryAt(t, d, 60_001, 80_001, 1, 1700000000, 1)
	_, err := d.pool.Exec(ctx, `DELETE FROM daily_stats_watermark`)
	require.NoError(t, err)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(80_001), dailyStatsWatermark(t, d), "advanced over the head path's contiguous blocks")

	seedHistoryAt(t, d, 0, 30_000, 1, 1700000000, 1)
	setDailyStatsWatermark(t, d, 80_001)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(80_001), dailyStatsWatermark(t, d),
		"blocks 30,001..60,000 are missing, yet the watermark is above them")
}

func TestRetentionNeverPrunesARangeWithMissingBlocks(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}

	seedHistoryAt(t, d, 60_001, 80_001, 1, 1700000000, 1)
	_, err := d.pool.Exec(ctx, `DELETE FROM daily_stats_watermark`)
	require.NoError(t, err)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.NoError(t, d.retentionPass(ctx, cfg))
	require.Equal(t, []string{"legacy:84001:attached", "p84001:86001:attached"}, partitionRanges(t, d))

	seedHistoryAt(t, d, 0, 30_000, 1, 1700000000, 1)
	ensurePartitions(t, d, 106_001, cfg)
	seedHistoryAt(t, d, 80_002, 106_001, 1, 1700000000, 1)
	setDailyStatsWatermark(t, d, 80_001)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(106_001), dailyStatsWatermark(t, d))

	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Zero(t, wm, "the legacy range is missing blocks 30,001..60,000: nothing may be pruned")

	seedHistoryAt(t, d, 30_001, 60_000, 1, 1700000000, 1)
	setDailyStatsWatermark(t, d, 106_001)
	_, err = d.pool.Exec(ctx, `INSERT INTO missing_block_ranges (from_number, to_number) VALUES (50000, 50010)`)
	require.NoError(t, err)
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err = d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Zero(t, wm, "a missing range overlapping the legacy range holds it too")

	_, err = d.pool.Exec(ctx, `DELETE FROM missing_block_ranges`)
	require.NoError(t, err)
	unpruned := totalsAfterReseed(t, d)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err = d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(86_000), wm, "complete, the legacy range goes, and p84001 below the window with it")
	require.Equal(t, unpruned, totalsAfterReseed(t, d), "totals after the prune equal the unpruned full sync")
	require.Equal(t, int64(106_002), unpruned.blocks, "blocks 0..106,001")
}
