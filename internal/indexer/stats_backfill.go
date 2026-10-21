package indexer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

type backfillStore interface {
	StatsBackfillState(ctx context.Context) (db.BackfillState, error)
	CommitBackfillBatch(ctx context.Context, b *db.BackfillBatch) error
}

type StatsBackfillConfig struct {
	// RPS bounds every node call of the backfill; Workers is its fetch parallelism under that budget.
	RPS, Workers int
	BatchBlocks  uint64
	SpanBlocks   uint64
	Retry        time.Duration
}

// backfiller counts [0, START_BLOCK) from the node with the head path's block builder and writes only counters,
// state and the ledger.
type backfiller struct {
	idx     *Indexer
	store   backfillStore
	cfg     StatsBackfillConfig
	limiter *rate.Limiter
}

func (i *Indexer) newBackfiller(store backfillStore, cfg StatsBackfillConfig) *backfiller {
	c := *i.config
	c.RPCRateLimit, c.TraceRateLimit = cfg.RPS, cfg.RPS
	c.RPCWorkers, c.TraceWorkers, c.TokenMetadataWorkers = 1, 1, 1
	return &backfiller{
		idx: &Indexer{db: i.db, rpc: i.rpc, config: &c, tokenCache: i.tokenCache, contractCache: i.contractCache,
			tracingSupported: i.tracingSupported},
		store: store, cfg: cfg, limiter: rate.NewLimiter(rate.Limit(cfg.RPS), max(cfg.Workers, 1)),
	}
}

// spanOf is the ledger span holding block n below the backfill target: the grid, cut at target + 1.
func (b *backfiller) spanOf(n, target uint64) (lo, hi uint64) {
	lo = n - n%b.cfg.SpanBlocks
	return lo, min(lo+b.cfg.SpanBlocks, target+1)
}

type fetched struct {
	raw      *rpc.RawBlock
	receipts map[common.Hash]*rpclient.Receipt
}

// batch fetches and builds blocks [from, to) and merges them into one batch.
func (b *backfiller) batch(ctx context.Context, from, to, target uint64) (*db.BackfillBatch, []string, error) {
	blocks := make([]fetched, to-from)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(b.cfg.Workers, 1))
	for n := from; n < to; n++ {
		g.Go(func() error {
			if err := b.limiter.Wait(gctx); err != nil {
				return err
			}
			raw, err := b.idx.rpc.RawBlockByNumber(gctx, n)
			if err != nil {
				return fmt.Errorf("block %d: %w", n, err)
			}
			if err := b.limiter.Wait(gctx); err != nil {
				return err
			}
			receipts, err := b.idx.rpc.FetchReceiptsBatch(gctx, b.idx.receiptTxHashes(raw), 1, b.cfg.RPS, n)
			if err != nil {
				return fmt.Errorf("block %d receipts: %w", n, err)
			}
			blocks[n-from] = fetched{raw: raw, receipts: receipts}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	out := &db.BackfillBatch{From: from, To: to, Spans: map[uint64]*db.SpanTotals{}, SpanHi: map[uint64]uint64{},
		Shared: &db.BlockData{AddressStats: map[string]*db.AddressStatsDelta{}}, TokenFirstSeen: map[string]db.TokenSeen{}}
	var discovered []string
	for _, f := range blocks {
		built := b.idx.buildBlock(ctx, f.raw, f.receipts)
		if built.tracesErr != nil {
			return nil, nil, fmt.Errorf("block %d traces: %w", built.blockNumber, built.tracesErr)
		}
		lo, hi := b.spanOf(built.blockNumber, target)
		span := out.Spans[lo]
		if span == nil {
			span = db.NewSpanTotals()
			out.Spans[lo], out.SpanHi[lo] = span, hi
		}
		if err := span.AddBlock(built.data); err != nil {
			return nil, nil, err
		}
		mergeShared(out, built)
		discovered = append(discovered, built.discovered...)
	}
	return out, discovered, nil
}

func mergeShared(out *db.BackfillBatch, built *builtBlock) {
	data := built.data
	out.Shared.Tokens = append(out.Shared.Tokens, data.Tokens...)
	out.Shared.NFTTokens = append(out.Shared.NFTTokens, data.NFTTokens...)
	out.Contracts = append(out.Contracts, data.Contracts...)
	for _, t := range data.Transfers {
		token := strings.ToLower(t.TokenAddress)
		if _, seen := out.TokenFirstSeen[token]; !seen {
			out.TokenFirstSeen[token] = db.TokenSeen{Block: t.BlockNumber, Tx: t.TxHash}
		}
	}
	for addr, d := range data.AddressStats {
		m := out.Shared.AddressStats[addr]
		if m == nil {
			m = &db.AddressStatsDelta{Address: addr, BlockNumber: d.BlockNumber}
			out.Shared.AddressStats[addr] = m
		}
		m.TxCountDelta += d.TxCountDelta
		m.InternalTxCountDelta += d.InternalTxCountDelta
		m.TokenTransferDelta += d.TokenTransferDelta
		m.SentDelta += d.SentDelta
		m.IsContract = m.IsContract || d.IsContract
		m.BlockNumber = min(m.BlockNumber, d.BlockNumber)
		m.LastBlock = max(m.LastBlock, d.BlockNumber)
	}
	for _, w := range built.balanceWork {
		out.BalanceKeys = append(out.BalanceKeys, db.BalanceKey{Token: strings.ToLower(w.TokenAddress.Hex()),
			Address: strings.ToLower(w.Address.Hex()), Block: w.BlockNumber})
	}
}

// step commits the next batch; done reports the backfill complete (or absent).
func (b *backfiller) step(ctx context.Context) (done bool, err error) {
	s, err := b.store.StatsBackfillState(ctx)
	if err != nil || !s.Exists || s.Complete {
		return err == nil, err
	}
	from := uint64(s.Through + 1)
	to := min(from+b.cfg.BatchBlocks, uint64(s.Target)+1)
	batch, discovered, err := b.batch(ctx, from, to, uint64(s.Target))
	if err != nil {
		return false, err
	}
	if err := b.store.CommitBackfillBatch(ctx, batch); err != nil {
		return false, err
	}
	for _, addr := range discovered {
		b.idx.tokenCache.Add(addr)
	}
	metrics.SetStatsBackfillThrough(to - 1)
	return to-1 == uint64(s.Target), nil
}

// RunStatsBackfill walks the backfill to its target, batch by batch, resuming from the recorded progress.
func (i *Indexer) RunStatsBackfill(ctx context.Context, store backfillStore, cfg StatsBackfillConfig) {
	b := i.newBackfiller(store, cfg)
	log.Info("stats backfill started", "rps", cfg.RPS, "workers", cfg.Workers, "batch_blocks", cfg.BatchBlocks)
	for ctx.Err() == nil {
		done, err := b.step(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Warn("stats backfill: batch failed; retrying", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(cfg.Retry):
			}
		case done:
			log.Info("stats backfill complete")
			return
		}
	}
}
