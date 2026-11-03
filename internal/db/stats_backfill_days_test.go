package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// docs/API.md "Stats backfill" says what GetDailyStats serves while the backfill runs: the days before daily_stats'
// first are absent, and every day served leaves out of its cumulative columns what the backfill has not yet counted.
func TestStatsBackfillDaysServedWhileItRunsAreAsDocumented(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	const start = int64(1_000)
	ts := func(n int64) int64 { return dayStart - 100*600 + (n-start)*600 }
	day := func(n int64) time.Time { return time.Unix(ts(n), 0).UTC().Truncate(24 * time.Hour) }
	nextDay := start + 100
	require.Equal(t, day(start).Add(24*time.Hour), day(nextDay))
	dayAfter := nextDay + 144
	require.Equal(t, day(nextDay).Add(24*time.Hour), day(dayAfter))

	require.NoError(t, d.InitStatsBackfill(ctx, uint64(start-1), uint64(ts(start-1))))
	half := NewSpanTotals()
	half.Blocks, half.Transactions = 500, 100
	require.NoError(t, d.CommitBackfillBatch(ctx, &BackfillBatch{From: 0, To: 500,
		Shared: &BlockData{AddressStats: map[string]*AddressStatsDelta{}},
		Spans:  map[uint64]*SpanTotals{0: half}, SpanHi: map[uint64]uint64{0: uint64(start)},
		Days: map[string]*DayParts{"2023-11-01": backfillDay(0)}}))

	storeBlocks(t, d, span(start, dayAfter+50), ts)
	_, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)

	served := func(date time.Time) *int64 {
		rows, err := d.GetDailyStats(ctx, date, date)
		require.NoError(t, err)
		if len(rows) == 0 {
			return nil
		}
		v := int64(rows[0].CumulativeTransactions)
		return &v
	}
	duringNext, duringAfter := served(day(nextDay)), served(day(dayAfter))

	rest := NewSpanTotals()
	rest.Blocks, rest.Transactions = 500, 100
	require.NoError(t, d.CommitBackfillBatch(ctx, &BackfillBatch{From: 500, To: uint64(start),
		Shared: &BlockData{AddressStats: map[string]*AddressStatsDelta{}},
		Spans:  map[uint64]*SpanTotals{0: rest}, SpanHi: map[uint64]uint64{0: uint64(start)},
		Days: map[string]*DayParts{"2023-11-02": backfillDay(500)}}))
	require.NoError(t, d.CompleteStatsBackfill(ctx))
	afterNext, afterAfter := served(day(nextDay)), served(day(dayAfter))
	require.NotNil(t, afterNext)
	require.NotNil(t, afterAfter)

	require.Nil(t, duringNext, "the day after START_BLOCK's, before daily_stats' first, is absent while the backfill runs")
	require.NotNil(t, duringAfter, "daily_stats' first day is served while the backfill runs")
	require.Less(t, *duringAfter, *afterAfter,
		"its cumulative_transactions leaves out the blocks the backfill had not counted, and grows once it completes")
}

// backfillDay is a day of 500 backfilled blocks from first, holding 100 transactions, as the backfill writes it.
func backfillDay(first uint64) *DayParts {
	p := NewDayParts(first)
	p.Blocks, p.Txs, p.Successful = 500, 100, 100
	return p
}
