package db

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/stretchr/testify/require"
)

var reorgDayCfg = RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}

// seedPrunableHistory stores blocks 0..head with block b at epoch0 + b*blockSeconds, cut into reorgDayCfg's ranges,
// aggregated by daily_stats_advance from the newest day on and closed in the ledger: what a running indexer has
// before a pass prunes.
func seedPrunableHistory(t *testing.T, d *DB, head, epoch0, blockSeconds int64) {
	t.Helper()
	ctx := context.Background()
	seedHistoryAt(t, d, 0, 40_001, 1, epoch0, blockSeconds)
	require.NoError(t, d.retentionPass(ctx, reorgDayCfg))
	ensurePartitions(t, d, uint64(head), reorgDayCfg)
	seedHistoryAt(t, d, 40_002, head, 1, epoch0, blockSeconds)
	_, err := d.pool.Exec(ctx, `DELETE FROM daily_stats_watermark`)
	require.NoError(t, err)
	advanceDailyStats(t, d, head)
	closeLedger(t, d)
}

// seedAggregated stores blocks from..to after a seeded history and has daily_stats_advance count them.
func seedAggregated(t *testing.T, d *DB, from, to, epoch0, blockSeconds int64) {
	t.Helper()
	ctx := context.Background()
	var wm int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT last_block FROM daily_stats_watermark WHERE id = 1`).Scan(&wm))
	seedHistoryAt(t, d, from, to, 1, epoch0, blockSeconds)
	_, err := d.pool.Exec(ctx, `UPDATE daily_stats_watermark SET last_block = $1 WHERE id = 1`, wm)
	require.NoError(t, err)
	advanceDailyStats(t, d, to)
}

func advanceDailyStats(t *testing.T, d *DB, through int64) {
	t.Helper()
	for i := 0; i < 10; i++ {
		var wm int64
		require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT daily_stats_advance()`).Scan(&wm))
		if wm >= through {
			return
		}
	}
	t.Fatalf("daily_stats did not reach block %d", through)
}

func prunedThrough(t *testing.T, d *DB) uint64 {
	t.Helper()
	wm, _, err := d.PrunedThrough(context.Background())
	require.NoError(t, err)
	return wm
}

// requireDayMatchesBlocks checks the stored daily_stats row of the day of block n against a count over that day's
// stored blocks, which a reverted day must still have in full.
func requireDayMatchesBlocks(t *testing.T, d *DB, n int64) {
	t.Helper()
	var day string
	var statBlocks, statTxs, blocks, txs int64
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		WITH d AS (SELECT daily_stats_day(timestamp) AS day FROM blocks WHERE number = $1)
		SELECT d.day::TEXT, s.total_blocks, s.total_transactions,
		       (SELECT count(*) FROM blocks WHERE daily_stats_day(timestamp) = d.day),
		       (SELECT count(*) FROM transactions t JOIN blocks b ON b.number = t.block_number
		        WHERE daily_stats_day(b.timestamp) = d.day)
		FROM d JOIN daily_stats s ON s.date = d.day`, n).Scan(&day, &statBlocks, &statTxs, &blocks, &txs))
	require.Equal(t, blocks, statBlocks, "total_blocks of %s", day)
	require.Equal(t, txs, statTxs, "total_transactions of %s", day)
}

// With 20,000 blocks retained in 2,000-block ranges, a pass at head 80,001 used to prune through 60,000: blocks
// 6,400..80,001 are one UTC day, so reverting the head then failed in daily_stats_invalidate ("whose first blocks are
// pruned") and ingest stopped on the stale fork.
func TestRetentionHeadReorgAfterPruningPartOfTheDay(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedPrunableHistory(t, d, 80_001, 1700000000, 1)

	require.NoError(t, d.retentionPass(ctx, reorgDayCfg))
	require.NoError(t, d.DeleteBlock(ctx, 80_001), "a one-block reorg at the head is reverted")
	advanceDailyStats(t, d, 80_000)
	requireDayMatchesBlocks(t, d, 80_000)
	require.Zero(t, prunedThrough(t, d), "the oldest range ends in the head's UTC day, which a reorg can reach")
}

func TestRetentionPrunesUpToTheReorgDay(t *testing.T) {
	const midnight = 1700092800 // 2023-11-16 00:00 UTC
	for _, c := range []struct {
		name         string
		epoch0       int64
		blockSeconds int64
		equalFrom    int64
		want         uint64
	}{
		// The floor block 67,201 is on the day starting at block 51,000: [50001, 52001) has a block on it and stays.
		{name: "day starts inside a range", epoch0: midnight - 51_000, blockSeconds: 1, want: 50_000},
		{name: "day starts on a range edge", epoch0: midnight - 52_001, blockSeconds: 1, want: 52_000},
		// Blocks 51,990..52,000 share the midnight timestamp: the first of them starts the day.
		{name: "equal timestamps at midnight", epoch0: midnight - 52_001, blockSeconds: 1, equalFrom: 51_990, want: 50_000},
		// 12 s blocks: the floor spans days, and the window, not the day, bounds the cut.
		{name: "slow chain", epoch0: 1700000000, blockSeconds: 12, want: 60_000},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			seedPrunableHistory(t, d, 80_001, c.epoch0, c.blockSeconds)
			if c.equalFrom > 0 {
				_, err := d.pool.Exec(ctx, `UPDATE blocks SET timestamp = $1 WHERE number BETWEEN $2 AND 52000`, midnight, c.equalFrom)
				require.NoError(t, err)
				_, err = d.pool.Exec(ctx, `
					DELETE FROM daily_stats_watermark; DELETE FROM daily_stats_acc; DELETE FROM daily_stats;
					DELETE FROM daily_active_addresses`)
				require.NoError(t, err)
				advanceDailyStats(t, d, 80_001)
			}

			require.NoError(t, d.retentionPass(ctx, reorgDayCfg))
			require.Equal(t, c.want, prunedThrough(t, d))
			var buf bytes.Buffer
			prev := log.Default()
			log.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			err := d.DeleteBlock(ctx, 80_001)
			log.SetDefault(prev)
			require.NoError(t, err)
			require.NotContains(t, buf.String(), "not stored", "a watermark on the last pruned block is not a gap")
			advanceDailyStats(t, d, 80_000)
			requireDayMatchesBlocks(t, d, 80_000)
		})
	}
}

func TestPruneAdvanceRefusesARangeInTheReorgDay(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedPrunableHistory(t, d, 80_001, 1700000000, 1)

	_, err := d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
	require.ErrorContains(t, err, "reaches the UTC day a reorg can revert into")
	require.Zero(t, prunedThrough(t, d))
}

// prune_advance reads the head again: a range the pass found prunable is refused once the head, and with it the
// floor's day, has gone back.
func TestPruneAdvanceRechecksTheReorgDayAtItsHead(t *testing.T) {
	const midnight = 1700092800
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedPrunableHistory(t, d, 80_001, midnight-52_001, 1)
	for _, s := range []string{"legacy", "p44001", "p46001", "p48001"} {
		_, err := d.pool.Exec(ctx, `SELECT prune_advance($1)`, s)
		require.NoError(t, err, s)
	}

	var ok bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT history_range_before_reorg_day('p50001')`).Scan(&ok))
	require.True(t, ok)
	// Head 64,800: the range is still below the floor block 52,000, which is on the day the range ends in.
	_, err := d.pool.Exec(ctx, `DELETE FROM blocks WHERE number > 64800`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `SELECT prune_advance('p50001')`)
	require.ErrorContains(t, err, "reaches the UTC day a reorg can revert into")
	require.Equal(t, uint64(50_000), prunedThrough(t, d))
}

func TestPruneDayFloor(t *testing.T) {
	const midnight = 1700092800
	ctx := context.Background()

	t.Run("no stored block at or below the floor", func(t *testing.T) {
		d, cleanup := setupTestDB(t)
		defer cleanup()
		seedHistoryAt(t, d, 70_000, 80_001, 1, 1700000000, 1)
		var floor *int64
		var ok bool
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT prune_day_floor(), history_range_before_reorg_day('legacy')`).Scan(&floor, &ok))
		require.Nil(t, floor)
		require.False(t, ok, "nothing is pruned without a floor block")
	})

	t.Run("floor in a gap", func(t *testing.T) {
		d, cleanup := setupTestDB(t)
		defer cleanup()
		// Block 65,000 is at midnight; the floor block 67,201 is not stored and the newest stored below it, 60,000,
		// is on the day before: that earlier day is kept.
		seedHistoryAt(t, d, 0, 60_000, 1, midnight-65_000, 1)
		seedHistoryAt(t, d, 70_000, 80_001, 1, midnight-65_000, 1)
		var floor int64
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT prune_day_floor()`).Scan(&floor))
		require.Equal(t, int64(midnight-86_400), floor)
	})
}

// A one-block reorg at the head races the pass that prunes up to the reorg day: each succeeds and the head's day
// still recounts in full.
func TestRetentionPruneAndHeadReorgConcurrently(t *testing.T) {
	const midnight = 1700092800
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedPrunableHistory(t, d, 80_001, midnight-51_000, 1)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = d.retentionPass(ctx, reorgDayCfg) }()
	go func() {
		defer wg.Done()
		for n := int64(80_001); n > 79_990 && errs[1] == nil; n-- {
			errs[1] = d.DeleteBlock(ctx, uint64(n))
		}
	}()
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, uint64(50_000), prunedThrough(t, d))
	advanceDailyStats(t, d, 79_990)
	requireDayMatchesBlocks(t, d, 79_990)
}

// A database an earlier build pruned into the reorg day: the pass prunes no further and warns, until the floor has
// moved to a later day; then pruning resumes.
func TestRetentionAfterAnEarlierBuildPrunedIntoTheReorgDay(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedPrunableHistory(t, d, 80_001, 1700000000, 1)

	var guard string
	require.NoError(t, d.pool.QueryRow(ctx,
		`SELECT pg_get_functiondef('history_range_before_reorg_day(text)'::regprocedure)`).Scan(&guard))
	_, err := d.pool.Exec(ctx, `CREATE OR REPLACE FUNCTION history_range_before_reorg_day(p_suffix TEXT) RETURNS BOOLEAN LANGUAGE sql AS 'SELECT true'`)
	require.NoError(t, err)
	require.NoError(t, d.retentionPass(ctx, reorgDayCfg))
	require.Equal(t, uint64(60_000), prunedThrough(t, d), "the earlier build pruned into the head's day")
	_, err = d.pool.Exec(ctx, guard)
	require.NoError(t, err)

	var buf bytes.Buffer
	prev := log.Default()
	log.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	d.warnPartlyPrunedReorgDay(ctx)
	log.SetDefault(prev)
	require.Contains(t, buf.String(), "pruned part of a UTC day the reorg window reaches")
	require.Contains(t, buf.String(), "day=2023-11-15")

	ensurePartitions(t, d, 110_001, reorgDayCfg)
	seedAggregated(t, d, 80_002, 90_001, 1700000000, 1)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, reorgDayCfg))
	require.Equal(t, uint64(60_000), prunedThrough(t, d), "the floor block 77,201 is still on 2023-11-15")

	// Block 92,800 starts 2023-11-16; at head 110,001 the floor block 97,201 is on it.
	seedAggregated(t, d, 90_002, 110_001, 1700000000, 1)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, reorgDayCfg))
	require.Equal(t, uint64(90_000), prunedThrough(t, d), "pruning resumes up to the range below 92,800")
	buf.Reset()
	log.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	d.warnPartlyPrunedReorgDay(ctx)
	log.SetDefault(prev)
	require.False(t, strings.Contains(buf.String(), "pruned part of a UTC day"), buf.String())
	require.NoError(t, d.DeleteBlock(ctx, 110_001))
}

// 024 goes down to the block-count rule and up again.
func TestPruneWholeDaysMigrationRevertsAndReapplies(t *testing.T) {
	pool, _ := emptyTestDB(t)
	ctx := context.Background()
	guarded := func() (bool, bool) {
		var floor, check bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT to_regprocedure('prune_day_floor()') IS NOT NULL,
			       position('history_range_before_reorg_day' IN prosrc) > 0
			FROM pg_proc WHERE proname = 'prune_advance'`).Scan(&floor, &check))
		return floor, check
	}
	migrateTo(t, pool, -1)
	floor, check := guarded()
	require.True(t, floor)
	require.True(t, check)
	migrateTo(t, pool, 23)
	floor, check = guarded()
	require.False(t, floor)
	require.False(t, check)
	migrateTo(t, pool, -1)
	floor, check = guarded()
	require.True(t, floor)
	require.True(t, check)
}
