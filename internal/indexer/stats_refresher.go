package indexer

import (
	"context"
	"sync"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
)

// statsRefresher coalesces the RefreshTokenStats calls requested by balance
// worker flushes into at most one refresh per token per tick.
//
// Before this, every balance worker (30 by default) called RefreshTokenStats
// for every token in every batch it flushed, and a worker flushes at least once
// a second. Under a 500 tx/s load that was ~7 refreshes per indexed block on top
// of the one the block path already does, all of them the same
// COUNT(*) FROM token_balances_current, each linear in the token's holder
// count (918 ms at 826k holders) and ~10 of them running concurrently.
// Measured 2026-09-08 23:12Z: 349 calls for 43 blocks. The extra calls bought
// nothing: the count is the same whichever flush triggers it.
//
// Marking is O(1) and never blocks a worker. The refresher goroutine drains the
// dirty set on a ticker and refreshes each token once. The freshness guarantee
// the callback gave -- holder_count reflects flushed balances without waiting
// for the next transfer of that token -- is kept, delayed by at most one tick.
type statsRefresher struct {
	db    Database
	every time.Duration

	mu    sync.Mutex
	dirty map[string]struct{}
}

func newStatsRefresher(db Database, every time.Duration) *statsRefresher {
	return &statsRefresher{db: db, every: every, dirty: make(map[string]struct{})}
}

// mark records tokens whose derived stats are stale. Safe from any goroutine.
func (r *statsRefresher) mark(tokenAddresses []string) {
	r.mu.Lock()
	for _, a := range tokenAddresses {
		r.dirty[a] = struct{}{}
	}
	r.mu.Unlock()
}

// run refreshes dirty tokens once per tick until ctx is done, then does a
// final pass so nothing marked during shutdown is left stale.
func (r *statsRefresher) run(ctx context.Context) {
	t := time.NewTicker(r.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.flush(context.Background())
			return
		case <-t.C:
			r.flush(ctx)
		}
	}
}

func (r *statsRefresher) flush(ctx context.Context) {
	r.mu.Lock()
	if len(r.dirty) == 0 {
		r.mu.Unlock()
		return
	}
	batch := r.dirty
	r.dirty = make(map[string]struct{}, len(batch))
	r.mu.Unlock()

	for addr := range batch {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := r.db.RefreshTokenStats(cctx, addr)
		cancel()
		if err != nil {
			log.Warn("refresh token stats after balance flush failed", "token", addr, "error", err)
		}
	}
}
