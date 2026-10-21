package db

import (
	"context"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

type LedgerConfig struct {
	SpanBlocks uint64
	FirstBlock uint64
	Interval   time.Duration
}

// CloseLedgerSpans closes up to max spans after the closed prefix, stopping at the first that is not final and
// complete; it returns how many it closed.
func (d *DB) CloseLedgerSpans(ctx context.Context, cfg LedgerConfig, max int) (int, error) {
	closed := 0
	for closed < max {
		var lo, hi int64
		if err := d.pool.QueryRow(ctx, `SELECT lo, hi FROM ledger_next_span($1, $2)`, int64(cfg.SpanBlocks), int64(cfg.FirstBlock)).Scan(&lo, &hi); err != nil {
			return closed, err
		}
		start := time.Now()
		var ok bool
		if err := d.pool.QueryRow(ctx, `SELECT ledger_close_span($1, $2)`, lo, hi).Scan(&ok); err != nil {
			return closed, err
		}
		if !ok {
			return closed, nil
		}
		closed++
		metrics.ObserveLedgerClose(time.Since(start))
		metrics.SetLedgerClosedThrough(uint64(hi - 1))
	}
	return closed, nil
}

// RunLedger closes one span per interval while there is one to close, retention on or off.
func (d *DB) RunLedger(ctx context.Context, cfg LedgerConfig) {
	log.Info("ledger closer started", "span_blocks", cfg.SpanBlocks, "first_block", cfg.FirstBlock, "interval", cfg.Interval)
	for {
		if n, err := d.CloseLedgerSpans(ctx, cfg, 1); err != nil && ctx.Err() == nil {
			log.Warn("ledger: closing a span failed; retrying next interval", "error", err)
		} else if n > 0 {
			log.Debug("ledger: span closed")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(cfg.Interval):
		}
	}
}
