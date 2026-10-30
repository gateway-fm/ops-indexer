package db

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// pruneReadyLegacy leaves the legacy range prunable: cut, history past it, the ledger closed.
func pruneReadyLegacy(t *testing.T, d *DB) RetentionConfig {
	t.Helper()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(context.Background(), cfg))
	ensurePartitions(t, d, 100_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	return cfg
}

// rangesBelowTheWatermark lists attached ranges that end at or below the watermark: pruned history recreated.
func rangesBelowTheWatermark(t *testing.T, d *DB) []string {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `
		SELECT suffix FROM history_partitions WHERE state = 'attached' AND hi IS NOT NULL AND hi <= history_from()`)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

// The partition keeper fills gaps while retention prunes and drops the oldest range. A prune and drop that land
// between the keeper's reading of the watermark and of the ranges make the dropped range look like a gap; it must
// not be recreated below the watermark.
func TestGapFillDoesNotRecreateARangePrunedMeanwhile(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := pruneReadyLegacy(t, d)

	var once sync.Once
	gapFillHook = func() {
		once.Do(func() {
			_, err := d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
			require.NoError(t, err)
			finished, err := d.finishDetached(ctx, cfg)
			require.NoError(t, err)
			require.True(t, finished)
		})
	}
	defer func() { gapFillHook = nil }()

	_, err := d.EnsurePartitionsAhead(ctx, 80_001, cfg.PartitionBlocks)
	require.NoError(t, err)
	through, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Positive(t, through, "the prune ran in between")
	require.Empty(t, rangesBelowTheWatermark(t, d), "no range is recreated below the watermark")
}

// A range below the watermark that is attached anyway (left by an earlier build) ends where no ledger span
// does, and pruneOldest refused it at every pass. Retention drops it and goes on pruning.
func TestRetentionDropsARangeLeftBelowTheWatermark(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := pruneReadyLegacy(t, d)
	_, err := d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
	require.NoError(t, err)
	finished, err := d.finishDetached(ctx, cfg)
	require.NoError(t, err)
	require.True(t, finished)
	through, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `SELECT history_partition_create($1, $2)`, int64(through)-1_499, int64(through)-499)
	require.NoError(t, err)
	require.Len(t, rangesBelowTheWatermark(t, d), 1)

	require.NoError(t, d.retentionPass(ctx, cfg))
	require.Empty(t, rangesBelowTheWatermark(t, d), "the range below the watermark is dropped")
}
