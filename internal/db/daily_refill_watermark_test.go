package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRefillRefusesADayTheWatermarkHasNotPassed(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	ts := func(n int64) int64 {
		if n <= 100 {
			return dayStart + n*10
		}
		return dayStart + 86_400 + n
	}
	_, err := d.pool.Exec(ctx, `INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, 0)`)
	require.NoError(t, err)
	storeBlocks(t, d, span(1, 50), ts)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)

	storeBlocks(t, d, span(51, 110), ts)
	date := time.Unix(dayStart, 0).UTC()
	day := RefillDay{Date: date, FirstBlock: 1, LastBlock: 100, Blocks: 100,
		GasUsed: 100 * 21000, SizeSum: 100 * 600, SizeN: 100, BlockTimeSum: 99 * 10, BlockTimeN: 99}
	_, err = d.WriteRefilledDailyStats(ctx, []RefillDay{day}, false)
	require.ErrorContains(t, err, "watermark", "the advance has counted the day only through block 50")
	_, err = d.WriteRefilledDailyStats(ctx, []RefillDay{day}, true)
	require.ErrorContains(t, err, "watermark", "a dry run reports it too")

	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	var blocks int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT total_blocks FROM daily_stats WHERE date = $1`, date).Scan(&blocks))
	require.EqualValues(t, 100, blocks, "the advance counts the day once")

	_, err = d.WriteRefilledDailyStats(ctx, []RefillDay{day}, false)
	require.NoError(t, err, "once the watermark has passed the day, it can be refilled")
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT total_blocks FROM daily_stats WHERE date = $1`, date).Scan(&blocks))
	require.EqualValues(t, 100, blocks)
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT total_blocks FROM daily_stats WHERE date = $1`, date).Scan(&blocks))
	require.EqualValues(t, 100, blocks, "a later advance adds nothing to a refilled day")
}
