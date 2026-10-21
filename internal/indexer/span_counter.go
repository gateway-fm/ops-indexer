package indexer

import (
	"context"
	"fmt"

	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/db"
)

// spanCounter counts a span of blocks from the node with the head path's own block builder, writing nothing, under
// its own RPC budget.
type spanCounter struct {
	idx     *Indexer
	limiter *rate.Limiter
}

func (i *Indexer) newSpanCounter(rps int) *spanCounter {
	cfg := *i.config
	cfg.CountersOnly = true
	cfg.RPCRateLimit, cfg.TraceRateLimit = rps, rps
	cfg.RPCWorkers, cfg.TraceWorkers = min(cfg.RPCWorkers, 4), min(cfg.TraceWorkers, 4)
	return &spanCounter{
		idx: &Indexer{db: i.db, rpc: i.rpc, config: &cfg, tokenCache: i.tokenCache, contractCache: i.contractCache,
			tracingSupported: i.tracingSupported},
		limiter: rate.NewLimiter(rate.Limit(rps), 1),
	}
}

// count sums what [lo, hi) contributes to the ledger.
func (c *spanCounter) count(ctx context.Context, lo, hi uint64) (*db.SpanTotals, error) {
	totals := db.NewSpanTotals()
	for n := lo; n < hi; n++ {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, err
		}
		raw, err := c.idx.rpc.RawBlockByNumber(ctx, n)
		if err != nil {
			return nil, fmt.Errorf("block %d: %w", n, err)
		}
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, err
		}
		receipts, err := c.idx.rpc.FetchReceiptsBatch(ctx, c.idx.receiptTxHashes(raw), c.idx.config.RPCWorkers, c.idx.config.RPCRateLimit, n)
		if err != nil {
			return nil, fmt.Errorf("block %d receipts: %w", n, err)
		}
		b := c.idx.buildBlock(ctx, raw, receipts)
		if b.tracesErr != nil {
			return nil, fmt.Errorf("block %d traces: %w", n, b.tracesErr)
		}
		if err := totals.AddBlock(b.data); err != nil {
			return nil, err
		}
	}
	return totals, nil
}
