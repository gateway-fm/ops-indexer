package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func u(v uint64) *uint64 { return &v }

func TestDaySpansInfersOnlyVerifiedBoundaries(t *testing.T) {
	d := func(i int) time.Time { return time.Date(2026, 10, 16+i, 0, 0, 0, 0, time.UTC) }
	days := []DayRow{
		{Date: d(0), TotalBlocks: 100, FirstBlock: u(1000)},
		{Date: d(1), TotalBlocks: 50},
		{Date: d(2), TotalBlocks: 70},
		{Date: d(3), TotalBlocks: 80, FirstBlock: u(1220)},
		{Date: d(4), TotalBlocks: 30, FirstBlock: u(1300)},
	}
	spans, inferred, skipped := daySpans(days, 1329)
	require.Equal(t, map[time.Time]uint64{d(1): 1100, d(2): 1150}, inferred, "1000+100, then +50; 1150+70 lands on 1220")
	require.Equal(t, []DaySpan{{d(0), 1000, 1100}, {d(1), 1100, 1150}, {d(2), 1150, 1220}, {d(3), 1220, 1300}, {d(4), 1300, 1330}}, spans)
	require.Empty(t, skipped)

	days[2].TotalBlocks = 69
	spans, inferred, skipped = daySpans(days, 1329)
	require.Empty(t, inferred, "the run does not land on the next recorded first block: not trusted")
	require.Equal(t, []time.Time{d(0), d(1), d(2)}, skipped, "and day 0 is not stretched over the unknown days")
	require.Equal(t, []DaySpan{{d(3), 1220, 1300}, {d(4), 1300, 1330}}, spans)

	gap := []DayRow{{Date: d(0), TotalBlocks: 100, FirstBlock: u(1000)}, {Date: d(2), TotalBlocks: 70, FirstBlock: u(1150)}}
	spans, _, skipped = daySpans(gap, 1219)
	require.Equal(t, []time.Time{d(0)}, skipped, "a missing date between: day 0's end is unknown")
	require.Equal(t, []DaySpan{{d(2), 1150, 1220}}, spans)
}
