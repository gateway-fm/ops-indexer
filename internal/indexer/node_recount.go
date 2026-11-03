package indexer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

type prunedSpanStore interface {
	NextPrunedSpan(ctx context.Context) (lo, hi int64, ok bool, err error)
	PrunedSpanFrom(ctx context.Context, lo, hi int64) (int64, error)
	SettleNodeSpan(ctx context.Context, lo, hi int64, s *db.SpanTotals, repair bool) (db.SpanCheck, error)
	DeferPrunedSpan(ctx context.Context, lo int64) error
}

// nodeRecountFailuresBeforeDefer is how many recounts of a span in a row may fail before the span goes to the back of
// the queue. Each recount already tries every block's reads spanBlockReads times.
const nodeRecountFailuresBeforeDefer = 2

type NodeRecountConfig struct {
	// RPS is the recount's node budget: every call waits for a token of it, and a batch call that falls back to one
	// request per transaction runs at RPS too, so the recount stays under twice RPS while a fallback runs.
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
	if errors.Is(err, errContractUndecided) {
		// The node cannot say whether a creation in the span is a contract, and will not on the next try either: the
		// span is left as its ledger has it and goes to the back of the queue, so the other pruned spans are checked.
		log.Warn("ledger: a pruned span's recount cannot judge a contract creation on this node; span left unchecked",
			"lo", lo, "hi", hi, "error", err)
		if err := store.DeferPrunedSpan(ctx, lo); err != nil {
			return db.SpanCheck{}, false, err
		}
		counter.leftUnchecked(lo, "undecided")
		return db.SpanCheck{Lo: lo, Hi: hi, State: "undecided"}, true, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return db.SpanCheck{}, false, err
		}
		metrics.NodeRecountFailed()
		if counter.failingLo != lo {
			counter.failingLo, counter.failures = lo, 0
		}
		counter.failures++
		if counter.failures < nodeRecountFailuresBeforeDefer {
			return db.SpanCheck{}, false, err
		}
		// A span whose count fails every time (a trace past the node's tracer timeout, state the node dropped) would
		// stay the span checked longest ago and hold the recount on it: it goes to the back of the queue instead.
		counter.failures = 0
		if derr := store.DeferPrunedSpan(ctx, lo); derr != nil {
			return db.SpanCheck{}, false, errors.Join(err, derr)
		}
		counter.leftUnchecked(lo, "failing")
		return db.SpanCheck{}, false, fmt.Errorf("span [%d, %d) failed %d recounts in a row, left unchecked: %w",
			lo, hi, nodeRecountFailuresBeforeDefer, err)
	}
	if counter.failingLo == lo {
		counter.failures = 0
	}
	c, err := store.SettleNodeSpan(ctx, lo, hi, totals, repair)
	if err == nil && c.State != "changed" {
		counter.checked(lo, hi)
	}
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
