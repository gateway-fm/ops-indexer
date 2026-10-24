package db

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// Retention pruned the ranges from START_BLOCK while the stats backfill was still
// counting below it; its completion then found START_BLOCK gone and refused on every pass, so the backfill never
// completed. Retention now keeps them until the backfill has written its days.
func TestRetentionWaitsForTheStatsBackfill(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	require.NoError(t, d.InitStatsBackfill(ctx, 19_999, 1_700_019_999))
	seedHistory(t, d, 20_000, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 19_999, wm, "nothing from START_BLOCK is pruned while the backfill has not written its days")
	require.EqualValues(t, 1, gauge(t, "indexer_prune_blocked_by_stats_backfill"))

	_, err = d.pool.Exec(ctx, `UPDATE stats_backfill SET through = target, daily_done = true, completed_at = now()`)
	require.NoError(t, err)
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err = d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Greater(t, wm, uint64(19_999), "once it has, retention prunes as usual")
	require.EqualValues(t, 0, gauge(t, "indexer_prune_blocked_by_stats_backfill"))
}

// gauge is the current value of a gauge in the default registry.
func gauge(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("no gauge %s", name)
	return 0
}
