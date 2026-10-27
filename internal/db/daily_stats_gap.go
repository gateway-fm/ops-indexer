package db

import (
	"context"
	"sync"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

// dailyStatsGapWarnBlocks is how many stored blocks may wait above a watermark that did not move before the hold is
// logged.
const dailyStatsGapWarnBlocks = 1000

// statsGap watches the daily_stats watermark. daily_stats_advance (012) only crosses a contiguous run of stored
// blocks, so a block that is never stored, such as one skipped for unavailable receipts that catch-up keeps retrying,
// holds the watermark below it and every later day's row with it.
type statsGap struct {
	mu        sync.Mutex
	watermark int64
	seen      bool
	warned    int64
	hasWarned bool
	warnAbove int64
}

// observe records the watermark an advance returned and how many stored blocks lie above it, and reports whether
// this is a hold to log: the watermark is the same as the previous advance's, more than the threshold of blocks wait
// above it, and this watermark has not been logged yet.
func (g *statsGap) observe(watermark, above int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	held := g.seen && g.watermark == watermark
	g.watermark, g.seen = watermark, true
	threshold := g.warnAbove
	if threshold == 0 {
		threshold = dailyStatsGapWarnBlocks
	}
	if !held || above <= threshold || (g.hasWarned && g.warned == watermark) {
		return false
	}
	g.warned, g.hasWarned = watermark, true
	return true
}

// reportDailyStatsGap sets the watermark lag metric after an advance and logs a hold once per watermark. Its errors
// are logged and do not fail the advance.
func (d *DB) reportDailyStatsGap(ctx context.Context, watermark int64) {
	var highest *int64
	if err := d.pool.QueryRow(ctx, `SELECT max(number) FROM blocks`).Scan(&highest); err != nil {
		log.Debug("could not read the highest stored block for the daily statistics lag", "error", err)
		return
	}
	if highest == nil {
		return
	}
	lag := *highest - watermark
	if lag < 0 {
		lag = 0
	}
	metrics.SetDailyStatsLag(lag)
	if !d.statsGap.observe(watermark, lag) {
		return
	}
	var waiting int64
	if err := d.pool.QueryRow(ctx, `SELECT count(*) FROM blocks WHERE number > $1`, watermark).Scan(&waiting); err != nil {
		log.Debug("could not count the blocks above the daily statistics watermark", "error", err)
		return
	}
	log.Warn("daily statistics are held at a missing block; later days are not updated until it is stored",
		"missing_block", watermark+1, "blocks_waiting", waiting, "highest_stored", *highest)
}
