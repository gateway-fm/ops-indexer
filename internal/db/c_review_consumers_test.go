package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// docs/API.md "Stats backfill" says that while the backfill runs, GetDailyStats is partial only for the days up to and
// including START_BLOCK's day. The day after START_BLOCK's, whose blocks are all stored, must be served, and the
// first day the aggregate counts must carry the same cumulative_transactions as it does once the backfill completes.
func TestStatsBackfillDaysAfterStartBlocksDayAreWholeWhileItRuns(t *testing.T) {
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

	if duringNext == nil {
		t.Errorf("the day after START_BLOCK's day, every block of it stored, is not served while the backfill runs; it is %d once the backfill completes", *afterNext)
	}
	require.Equal(t, *afterAfter, *duringAfter,
		"the aggregate's first day, two days after START_BLOCK's, served a cumulative_transactions that the completion then changed")
}

// backfillDay is a day of 500 backfilled blocks from first, holding 100 transactions, as the backfill writes it.
func backfillDay(first uint64) *DayParts {
	p := NewDayParts(first)
	p.Blocks, p.Txs, p.Successful = 500, 100, 100
	return p
}
