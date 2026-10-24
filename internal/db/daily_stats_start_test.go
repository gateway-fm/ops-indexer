package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const dayStart = int64(19_676 * 86_400)

func storeBlocks(t *testing.T, d *DB, numbers []int64, ts func(int64) int64) {
	t.Helper()
	for _, n := range numbers {
		_, err := d.pool.Exec(context.Background(), `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, size)
			VALUES ($1, $2, $3, $4, 21000, 30000000, 0, 600)`, n, fmt.Sprintf("0xd%d", n), fmt.Sprintf("0xd%d", n-1), ts(n))
		require.NoError(t, err)
	}
}

func span(lo, hi int64) []int64 {
	var out []int64
	for n := lo; n <= hi; n++ {
		out = append(out, n)
	}
	return out
}

// PRST-5436: the first daily_stats run started at the newest day's first *stored* block. With catch-up still filling
// the start of that day, the day was cut, and the blocks stored later fell below the watermark for good (on a production deployment one day
// had 62,972 of 587,903 blocks). The first watermark now waits until the day's first block is known.
func TestDailyStatsFirstRunWaitsForTheDaysFirstBlock(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	ts := func(n int64) int64 { return dayStart - 100*7 + n*7 + (n%5)*1 }
	firstToday := int64(0)
	for n := int64(1); ; n++ {
		if ts(n) >= dayStart {
			firstToday = n
			break
		}
	}
	storeBlocks(t, d, span(1, firstToday-20), ts)
	storeBlocks(t, d, span(firstToday+30, firstToday+120), ts)

	upto, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.Zero(t, upto, "the newest day's start is not stored yet: no watermark, no rows")
	var watermarks int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM daily_stats_watermark`).Scan(&watermarks))
	require.Zero(t, watermarks)

	storeBlocks(t, d, span(firstToday-19, firstToday+29), ts)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	var blocks int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT total_blocks FROM daily_stats WHERE date = DATE '1970-01-01' + $1::int`, dayStart/86_400).Scan(&blocks))
	require.Equal(t, 121, blocks, "the whole day, from its first block")
}

// A fresh start stores nothing before the newest day: the first run starts at once.
func TestDailyStatsFirstRunOnAFreshStart(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ts := func(n int64) int64 { return dayStart + n*3 }
	storeBlocks(t, d, span(500, 520), ts)
	upto, err := d.AdvanceDailyStats(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 520, upto)
}

// A reorg on a day whose start is still in a catch-up gap, with the watermark mid-day as a production deployment's first run left it:
// daily_stats_invalidate set the watermark below the day's first *stored* block again, and the gap's blocks, stored
// later, were never counted. The watermark now goes to the last stored block below, and both days count every block.
func TestDailyStatsReorgOnADayWhoseStartIsNotStored(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	ts := func(n int64) int64 { return dayStart - 100*7 + n*7 + (n%5)*1 }
	firstToday := int64(0)
	for n := int64(1); ; n++ {
		if ts(n) >= dayStart {
			firstToday = n
			break
		}
	}
	storeBlocks(t, d, span(1, firstToday-5), ts)
	_, err := d.pool.Exec(ctx, `INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, 0)`)
	require.NoError(t, err)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)

	storeBlocks(t, d, span(firstToday+30, firstToday+120), ts)
	_, err = d.pool.Exec(ctx, `UPDATE daily_stats_watermark SET last_block = $1`, firstToday+29)
	require.NoError(t, err)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)

	require.NoError(t, d.DeleteBlock(ctx, uint64(firstToday+100)))
	storeBlocks(t, d, span(firstToday-4, firstToday+29), ts)
	storeBlocks(t, d, []int64{firstToday + 100}, ts)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)

	var yesterday, today int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT total_blocks FROM daily_stats WHERE date = DATE '1970-01-01' + $1::int - 1),
		       (SELECT total_blocks FROM daily_stats WHERE date = DATE '1970-01-01' + $1::int)`, dayStart/86_400).Scan(&yesterday, &today))
	require.Equal(t, int(firstToday-1), yesterday, "yesterday's last blocks, stored after the reorg, are counted")
	require.Equal(t, 121, today, "today from its first block")
}

// When the reorged day is the earliest aggregated one, the day before was never aggregated: lowering the watermark
// into it would give it a partial row. The watermark stays, and the day before, whose blocks are stored below it, is
// counted whole from them.
func TestDailyStatsReorgNeverStartsADayThatWasNotAggregated(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	ts := func(n int64) int64 { return dayStart - 100*7 + n*7 + (n%5)*1 }
	firstToday := int64(0)
	for n := int64(1); ; n++ {
		if ts(n) >= dayStart {
			firstToday = n
			break
		}
	}
	storeBlocks(t, d, span(1, firstToday-5), ts)
	storeBlocks(t, d, span(firstToday+30, firstToday+120), ts)
	_, err := d.pool.Exec(ctx, `INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, $1)`, firstToday+29)
	require.NoError(t, err)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)

	require.NoError(t, d.DeleteBlock(ctx, uint64(firstToday+100)))
	storeBlocks(t, d, span(firstToday-4, firstToday+29), ts)
	storeBlocks(t, d, []int64{firstToday + 100}, ts)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)

	var yesterday int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT total_blocks FROM daily_stats WHERE date = DATE '1970-01-01' + $1::int - 1`, dayStart/86_400).Scan(&yesterday))
	require.Equal(t, firstToday-1, yesterday, "the day before counts every one of its blocks, not a part")
}

// With a stats backfill, nothing below START_BLOCK is ever stored, so "nothing stored before the day" is not
// a fresh start. Taken as one, the first watermark started on START_BLOCK's day, and daily_stats_backfill_complete
// then refused for good: the backfill never completed and never read its balances. The watermark now waits for a day
// whose previous block is stored, which is always after START_BLOCK's day.
func TestDailyStatsFirstRunWithAStatsBackfillStartsAfterStartBlocksDay(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	const start = int64(1_000)
	ts := func(n int64) int64 { return dayStart - 200*7 + (n-start)*7 }
	nextDay := start
	for ts(nextDay) < dayStart {
		nextDay++
	}
	require.NoError(t, d.InitStatsBackfill(ctx, uint64(start-1), uint64(ts(start-1))))

	storeBlocks(t, d, span(start+5, start+60), ts)
	_, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	storeBlocks(t, d, span(start, start+4), ts)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	var watermarks int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM daily_stats_watermark`).Scan(&watermarks))
	require.Zero(t, watermarks, "START_BLOCK's day is not a fresh start: nothing below START_BLOCK is ever stored")

	storeBlocks(t, d, span(start+61, nextDay+50), ts)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	var first int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT min(first_block) FROM daily_stats_acc`).Scan(&first))
	require.Equal(t, nextDay, first, "the aggregate starts at the day after START_BLOCK's")

	_, err = d.pool.Exec(ctx, `UPDATE stats_backfill SET through = target`)
	require.NoError(t, err)
	require.NoError(t, d.CompleteStatsBackfill(ctx), "the backfill completes: the aggregate starts after START_BLOCK's day")
}
