package indexer

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
)

// spanCounter counts a span of blocks from the node with the head path's own block builder, writing nothing, under
// its own RPC budget: every call its builder makes waits on limiter (limitedRPC).
type spanCounter struct {
	idx     *Indexer
	limiter *rate.Limiter
	// retry is the wait before a block's second read; it doubles for each read after.
	retry time.Duration
	// failingLo is the span whose count failed last, failures how many times in a row.
	failingLo int64
	failures  int
}

func (i *Indexer) newSpanCounter(rps int) *spanCounter {
	cfg := *i.config
	cfg.CountersOnly, cfg.StrictTraces = true, true
	cfg.RPCRateLimit, cfg.TraceRateLimit = rps, rps
	cfg.RPCWorkers, cfg.TraceWorkers = min(cfg.RPCWorkers, 4), min(cfg.TraceWorkers, 4)
	limiter := rate.NewLimiter(rate.Limit(rps), 1)
	return &spanCounter{
		idx: &Indexer{db: i.db, rpc: newLimitedRPC(i.rpc, limiter, rps, 1), config: &cfg, tokenCache: i.tokenCache,
			contractCache: i.contractCache, tracingSupported: i.tracingSupported},
		limiter: limiter,
		retry:   time.Second,
	}
}

// spanBlockReads is how many times a block's reads are tried before the span gives up and starts over: one dropped
// connection must not cost a span that takes an hour to count.
const spanBlockReads = 5

// count sums what [lo, hi) contributes to the ledger.
func (c *spanCounter) count(ctx context.Context, lo, hi uint64) (*db.SpanTotals, error) {
	totals := db.NewSpanTotals()
	for n := lo; n < hi; n++ {
		for attempt := 1; ; attempt++ {
			data, err := c.block(ctx, n)
			if err == nil {
				if err := totals.AddBlock(data); err != nil {
					return nil, err
				}
				break
			}
			if ctx.Err() != nil || attempt == spanBlockReads {
				return nil, err
			}
			log.Warn("ledger: pruned-span recount read failed; retrying the block", "block", n, "attempt", attempt, "error", err)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.retry << (attempt - 1)):
			}
		}
	}
	return totals, nil
}

// block reads and builds block n as the head path does.
func (c *spanCounter) block(ctx context.Context, n uint64) (*db.BlockData, error) {
	raw, err := c.idx.rpc.RawBlockByNumber(ctx, n)
	if err != nil {
		return nil, fmt.Errorf("block %d: %w", n, err)
	}
	receipts, err := c.idx.rpc.FetchReceiptsBatch(ctx, c.idx.receiptTxHashes(raw), c.idx.config.RPCWorkers, c.idx.config.RPCRateLimit, n)
	if err != nil {
		return nil, fmt.Errorf("block %d receipts: %w", n, err)
	}
	b := c.idx.buildBlock(ctx, raw, receipts)
	if b.tracesErr != nil {
		return nil, fmt.Errorf("block %d traces: %w", n, b.tracesErr)
	}
	if b.codeErr != nil {
		return nil, fmt.Errorf("block %d: %w", n, b.codeErr)
	}
	return b.data, nil
}
