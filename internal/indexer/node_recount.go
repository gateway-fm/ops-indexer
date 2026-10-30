package indexer

import (
	"context"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

type prunedSpanStore interface {
	NextPrunedSpan(ctx context.Context) (lo, hi int64, ok bool, err error)
	PrunedSpanFrom(ctx context.Context, lo, hi int64) (int64, error)
	SettleNodeSpan(ctx context.Context, lo, hi int64, s *db.SpanTotals, repair bool) (db.SpanCheck, error)
}

type NodeRecountConfig struct {
	// RPS bounds every call the recount makes to the node.
	RPS    int
	Repair bool
	// Idle is the wait when nothing is pruned or a recount failed.
	Idle time.Duration
}

// RecountPrunedSpan recounts, from the node, the pruned span checked longest ago and settles it against its ledger
// rows; ok is false when nothing is pruned.
func (i *Indexer) RecountPrunedSpan(ctx context.Context, store prunedSpanStore, counter *spanCounter, repair bool) (db.SpanCheck, bool, error) {
	lo, hi, ok, err := store.NextPrunedSpan(ctx)
	if err != nil || !ok {
		return db.SpanCheck{}, false, err
	}
	from, err := store.PrunedSpanFrom(ctx, lo, hi)
	if err != nil {
		return db.SpanCheck{}, false, err
	}
	totals, err := counter.count(ctx, uint64(from), uint64(hi))
	if err != nil {
		return db.SpanCheck{}, false, err
	}
	c, err := store.SettleNodeSpan(ctx, lo, hi, totals, repair)
	return c, err == nil, err
}

// RunNodeRecount recounts pruned spans from the node one after another, oldest-checked first, within cfg.RPS.
func (i *Indexer) RunNodeRecount(ctx context.Context, store prunedSpanStore, cfg NodeRecountConfig) {
	log.Info("pruned-span recount from the node started", "rps", cfg.RPS, "repair", cfg.Repair)
	counter := i.newSpanCounter(cfg.RPS)
	cycle := time.Now()
	for ctx.Err() == nil {
		start := time.Now()
		c, ok, err := i.RecountPrunedSpan(ctx, store, counter, cfg.Repair)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Warn("ledger: pruned-span recount failed", "error", err)
		case ok:
			metrics.VerifyChecked("node", c.Drifted, c.State)
			log.Info("ledger: pruned span recounted from the node", "lo", c.Lo, "hi", c.Hi, "drifted", c.Drifted,
				"state", c.State, "elapsed", time.Since(start))
			if next, _, more, err := store.NextPrunedSpan(ctx); err == nil && more && next <= c.Lo {
				metrics.VerifyCycle("node", time.Since(cycle))
				cycle = time.Now()
			}
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(cfg.Idle):
		}
	}
}
