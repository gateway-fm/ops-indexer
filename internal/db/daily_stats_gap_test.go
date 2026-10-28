package db

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/log"
)

func dailyStatsLagMetric(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "indexer_daily_stats_watermark_lag_blocks" {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("indexer_daily_stats_watermark_lag_blocks is not registered")
	return 0
}

// A block that is never stored holds the daily_stats watermark below it, and every later day with it. The hold is
// logged once, with the missing block and how many blocks wait above it, and the lag metric shows it; a chain without
// a hole stays quiet.
func TestDailyStatsHeldByAMissingBlockIsLoggedOnce(t *testing.T) {
	for _, hole := range []bool{true, false} {
		name := "without a hole"
		if hole {
			name = "with block 61 missing"
		}
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := log.Default()
			log.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			defer log.SetDefault(prev)

			d, cleanup := setupTestDB(t)
			defer cleanup()
			d.statsGap.warnAbove = 10
			ctx := context.Background()
			advance := func() {
				_, err := d.AdvanceDailyStats(ctx)
				require.NoError(t, err)
			}
			for n := 1; n <= 60; n++ {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
			}
			advance()
			for n := 62; n <= lateBlocksTotal; n++ {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
			}
			if !hole {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(61)))
			}
			advance()
			advance()
			advance()

			warnings := strings.Count(buf.String(), "daily statistics are held at a missing block")
			if !hole {
				require.Zero(t, warnings, buf.String())
				require.Zero(t, dailyStatsLagMetric(t))
				return
			}
			require.Equal(t, 1, warnings, buf.String())
			require.Contains(t, buf.String(), "missing_block=61")
			require.Contains(t, buf.String(), "blocks_waiting=59")
			require.Equal(t, float64(60), dailyStatsLagMetric(t))
			laterDays := func() (n int) {
				require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM daily_stats WHERE date >= '2026-10-07'`).Scan(&n))
				return n
			}
			require.Zero(t, laterDays(), "the days after the hole have rows while it is open")

			require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(61)))
			advance()
			require.Zero(t, dailyStatsLagMetric(t))
			require.Equal(t, 2, laterDays())
			require.Equal(t, 1, strings.Count(buf.String(), "daily statistics are held at a missing block"))
		})
	}
}
