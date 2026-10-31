package indexer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

type backfillStore interface {
	StatsBackfillState(ctx context.Context) (db.BackfillState, error)
	CommitBackfillBatch(ctx context.Context, b *db.BackfillBatch) error
	CompleteStatsBackfill(ctx context.Context) error
	NextBackfillBalanceKeys(ctx context.Context, after *db.BalanceKey, n int) ([]db.BalanceKey, error)
	SettleBackfillBalances(ctx context.Context, keys []db.BalanceKey, balances []*types.Balance) error
}

const balanceKeysPerRead = 1000

// completionRecheck spaces the checks of a completion that waits for catch-up; each counts the blocks it needs.
const completionRecheck = time.Minute

type StatsBackfillConfig struct {
	// RPS is the backfill's node budget: every call waits for a token of it, and a batch call that falls back to one
	// request per transaction runs at RPS/Workers, so the backfill stays under twice RPS while fallbacks run.
	// Workers is its fetch and balance-read parallelism under that budget.
	RPS, Workers int
	BatchBlocks  uint64
	SpanBlocks   uint64
	Retry        time.Duration
}

// strictBlockTries is how many times in a row the backfill fails a block it cannot count whole (a trace or a contract's
// code the node does not serve) before it counts the block without what failed, as ingest would have.
var strictBlockTries = 30

// backfiller counts [0, START_BLOCK) from the node with the head path's block builder and writes only counters,
// state and the ledger.
type backfiller struct {
	idx *Indexer
	// lenient builds a block that failed strictBlockTries times in a row, failing is that block and its failures.
	lenient *Indexer
	failing struct {
		block uint64
		tries int
	}
	store   backfillStore
	cfg     StatsBackfillConfig
	limiter *rate.Limiter
	// readAfter is the last key the balance reads took: the next pass starts after it and wraps at the end, so a key
	// kept after a failed read is retried after every other key.
	readAfter *db.BalanceKey
	// waitUntil is when a completion that waits for catch-up is checked again.
	waitUntil time.Time
	// retrying keeps a failed batch's fetched blocks, and those it built before the one that failed, for its retry:
	// the blocks are final, and a retry fetches and builds only from the block that failed.
	retrying *batchRetry
}

type batchRetry struct {
	from, to uint64
	blocks   []fetched
	built    []*builtBlock
}

func (i *Indexer) newBackfiller(store backfillStore, cfg StatsBackfillConfig) *backfiller {
	c := *i.config
	c.RPCRateLimit, c.TraceRateLimit = cfg.RPS, cfg.RPS
	c.RPCWorkers, c.TraceWorkers, c.TokenMetadataWorkers = 1, 1, 1
	c.StrictTraces, c.CodeAtBlock = true, true
	lc := c
	lc.StrictTraces, lc.CodeAtBlock = false, false
	limiter := rate.NewLimiter(rate.Limit(cfg.RPS), max(cfg.Workers, 1))
	node := newLimitedRPC(i.rpc, limiter, cfg.RPS, cfg.Workers)
	return &backfiller{
		idx: &Indexer{db: i.db, rpc: node, config: &c, tokenCache: i.tokenCache, contractCache: i.contractCache,
			tracingSupported: i.tracesOldBlocks()},
		lenient: &Indexer{db: i.db, rpc: node, config: &lc, tokenCache: i.tokenCache, contractCache: i.contractCache,
			tracingSupported: i.tracesOldBlocks()},
		store: store, cfg: cfg, limiter: limiter,
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

// batch fetches and builds blocks [from, to) and merges them into one batch; prevTs is block from - 1's timestamp.
func (b *backfiller) batch(ctx context.Context, from, to, target uint64, prevTs *int64) (*db.BackfillBatch, []string, error) {
	if r := b.retrying; r == nil || r.from != from || r.to != to {
		b.retrying = &batchRetry{from: from, to: to, blocks: make([]fetched, to-from)}
	}
	blocks := b.retrying.blocks
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(b.cfg.Workers, 1))
	for n := from; n < to; n++ {
		if blocks[n-from].raw != nil {
			continue
		}
		g.Go(func() error {
			raw, err := b.idx.rpc.RawBlockByNumber(gctx, n)
			if err != nil {
				return fmt.Errorf("block %d: %w", n, err)
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
		Shared: &db.BlockData{AddressStats: map[string]*db.AddressStatsDelta{}}, TokenFirstSeen: map[string]db.TokenSeen{},
		Days: map[string]*db.DayParts{}, Active: map[string]map[string]struct{}{}}
	var discovered []string
	for i, f := range blocks {
		if i < len(b.retrying.built) {
			if err := addBlock(out, b.retrying.built[i], b.spanOf, target, &prevTs, &discovered); err != nil {
				return nil, nil, err
			}
			continue
		}
		built := b.idx.buildBlock(ctx, f.raw, f.receipts)
		if err := errors.Join(built.tracesErr, built.codeErr); err != nil {
			if !b.givesUp(built.blockNumber) {
				return nil, nil, fmt.Errorf("block %d: %w", built.blockNumber, err)
			}
			log.Error("stats backfill: a block failed on every try; counted without what failed, as ingest would have",
				"block", built.blockNumber, "tries", strictBlockTries, "error", err,
				"metric", "indexer_stats_backfill_lenient_blocks_total")
			metrics.StatsBackfillLenientBlock()
			built = b.lenient.buildBlock(ctx, f.raw, f.receipts)
		}
		b.retrying.built = append(b.retrying.built, built)
		if err := addBlock(out, built, b.spanOf, target, &prevTs, &discovered); err != nil {
			return nil, nil, err
		}
	}
	b.retrying = nil
	return out, discovered, nil
}

// addBlock merges a built block into the batch: its span's totals, the shared rows, its day's parts.
func addBlock(out *db.BackfillBatch, built *builtBlock, spanOf func(n, target uint64) (uint64, uint64), target uint64,
	prevTs **int64, discovered *[]string) error {
	lo, hi := spanOf(built.blockNumber, target)
	span := out.Spans[lo]
	if span == nil {
		span = db.NewSpanTotals()
		out.Spans[lo], out.SpanHi[lo] = span, hi
	}
	if err := span.AddBlock(built.data); err != nil {
		return err
	}
	mergeShared(out, built)
	addDay(out, built, *prevTs)
	ts := int64(built.blockTimestamp)
	*prevTs = &ts
	*discovered = append(*discovered, built.discovered...)
	return nil
}

// givesUp records that block failed, and is whether it has now failed strictBlockTries times in a row.
func (b *backfiller) givesUp(block uint64) bool {
	if b.failing.block != block {
		b.failing.block, b.failing.tries = block, 0
	}
	b.failing.tries++
	return b.failing.tries >= strictBlockTries
}

// addDay adds a block to its day's daily_stats parts, as daily_stats_advance aggregates stored blocks.
func addDay(out *db.BackfillBatch, built *builtBlock, prevTs *int64) {
	data := built.data
	date := blockDay(built.blockTimestamp).Format("2006-01-02")
	p := out.Days[date]
	if p == nil {
		p = db.NewDayParts(built.blockNumber)
		out.Days[date], out.Active[date] = p, map[string]struct{}{}
	}
	p.FirstBlock = min(p.FirstBlock, built.blockNumber)
	p.Blocks++
	p.GasUsed.Add(p.GasUsed, new(big.Int).SetUint64(data.Block.GasUsed))
	p.SizeSum.Add(p.SizeSum, new(big.Int).SetUint64(data.Block.Size))
	p.SizeN++
	if prevTs != nil && built.blockNumber > 0 {
		p.BlockTimeSum.Add(p.BlockTimeSum, big.NewInt(int64(built.blockTimestamp)-*prevTs))
		p.BlockTimeN++
	}
	for _, t := range data.Transactions {
		p.Txs++
		p.GasPriceSum.Add(p.GasPriceSum, new(big.Int).SetUint64(t.GasPrice))
		switch t.Status {
		case 1:
			p.Successful++
		case 0:
			p.Failed++
		}
		out.Active[date][strings.ToLower(t.From)] = struct{}{}
		if t.To != nil {
			out.Active[date][strings.ToLower(*t.To)] = struct{}{}
		}
	}
	p.TokenTransfers += int64(len(data.Transfers))
	out.LastTs = int64(built.blockTimestamp)
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
	if s.Through == s.Target {
		if time.Now().Before(b.waitUntil) {
			return false, db.ErrBackfillWaitsForCatchup
		}
		if err := b.store.CompleteStatsBackfill(ctx); err != nil {
			if errors.Is(err, db.ErrBackfillWaitsForCatchup) {
				b.waitUntil = time.Now().Add(completionRecheck)
			}
			return false, err
		}
		log.Info("stats backfill: every block counted and daily_stats written", "through", s.Target)
		return true, nil
	}
	from := uint64(s.Through + 1)
	to := min(from+b.cfg.BatchBlocks, uint64(s.Target)+1)
	batch, discovered, err := b.batch(ctx, from, to, uint64(s.Target), s.LastTs)
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
	return false, nil
}

// readBalances reads, at latest, the balance of up to n keys the backfill touched, once each, Workers at a time
// within RPS. A key whose balanceOf reverts or returns no uint256 (not a token after all) is dropped, as the head
// path's balance worker drops it; a key whose read fails otherwise (rate limit, timeout, transport) stays for the
// next pass, and the pass reports the failure so the caller backs off. It reports how many keys it took.
func (b *backfiller) readBalances(ctx context.Context, n int) (int, error) {
	keys, err := b.store.NextBackfillBalanceKeys(ctx, b.readAfter, n)
	if err == nil && len(keys) == 0 && b.readAfter != nil {
		b.readAfter = nil
		keys, err = b.store.NextBackfillBalanceKeys(ctx, nil, n)
	}
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	b.readAfter = &keys[len(keys)-1]
	read := make([]*types.Balance, len(keys))
	failed := make([]error, len(keys))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(b.cfg.Workers, 1))
	for i, k := range keys {
		g.Go(func() error {
			data := append(append([]byte{}, balanceOfSelector...), common.LeftPadBytes(common.HexToAddress(k.Address).Bytes(), 32)...)
			out, err := b.idx.rpc.CallContract(gctx, common.HexToAddress(k.Token), data)
			switch {
			case err != nil && !rpclient.IsExecutionError(err):
				failed[i] = err
			case err != nil || len(out) < 32:
			default:
				read[i] = &types.Balance{Address: k.Address, TokenAddress: k.Token, BlockNumber: k.Block,
					Balance: types.JSONString(new(big.Int).SetBytes(out[:32]).String())}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	settled := make([]db.BalanceKey, 0, len(keys))
	balances := make([]*types.Balance, 0, len(keys))
	var firstErr error
	for i, k := range keys {
		if failed[i] != nil {
			firstErr = cmp.Or(firstErr, failed[i])
			continue
		}
		settled = append(settled, k)
		if read[i] != nil {
			balances = append(balances, read[i])
		}
	}
	if err := b.store.SettleBackfillBalances(ctx, settled, balances); err != nil {
		return 0, err
	}
	if firstErr != nil {
		return len(keys), fmt.Errorf("%d of %d balance reads failed, kept for the next pass: %w",
			len(keys)-len(settled), len(keys), firstErr)
	}
	return len(keys), nil
}

// RunStatsBackfill walks the backfill to its target, batch by batch, resuming from the recorded progress, and reads
// the balances of the keys it touched as it goes, whether or not the step succeeded.
func (i *Indexer) RunStatsBackfill(ctx context.Context, store backfillStore, cfg StatsBackfillConfig) {
	b := i.newBackfiller(store, cfg)
	log.Info("stats backfill started", "rps", cfg.RPS, "workers", cfg.Workers, "batch_blocks", cfg.BatchBlocks)
	done, waiting := false, false
	for ctx.Err() == nil {
		var stepErr error
		if !done {
			done, stepErr = b.step(ctx)
		}
		read, readErr := b.readBalances(ctx, balanceKeysPerRead)
		waitingNow := errors.Is(stepErr, db.ErrBackfillWaitsForCatchup)
		if waitingNow {
			if !waiting {
				log.Info("stats backfill: every block counted; waiting for catch-up to store the blocks from START_BLOCK",
					"detail", stepErr)
			}
			stepErr = nil
		}
		waiting = waitingNow
		metrics.SetStatsBackfillWaiting(waiting)
		err := errors.Join(stepErr, readErr)
		switch {
		case (err != nil || waitingNow && read == 0) && ctx.Err() == nil:
			if err != nil {
				log.Warn("stats backfill: batch failed; retrying", "error", err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(cfg.Retry):
			}
		case done && read == 0:
			log.Info("stats backfill complete")
			return
		}
	}
}

// blockTracer traces a whole block, as the start's tracing check does.
type blockTracer interface {
	TraceBlock(ctx context.Context, blockNumber uint64) (map[string]rpc.TraceBlockResult, error)
}

// historicalTraces checks that the node traces block 1, the oldest the backfill traces (many nodes cannot trace
// genesis). A node without the old state cannot trace any block of the range, and the backfill would fail every one
// of them: the prepare fails instead, and says so. A node that has no tracing at all is counted without traces, as
// ingest counts it.
func historicalTraces(ctx context.Context, node RPCClient, startBlock uint64) error {
	t, ok := node.(blockTracer)
	if !ok || startBlock < 2 {
		return nil
	}
	if _, err := t.TraceBlock(ctx, 1); err != nil && !rpc.TracingAbsent(err) {
		return fmt.Errorf("the stats backfill traces the blocks below START_BLOCK, and the node does not trace block 1 "+
			"(it needs an archive node with tracing, or ENABLE_TRACING=false to count without internal transactions): %w", err)
	}
	return nil
}

type backfillInitStore interface {
	StatsBackfillState(ctx context.Context) (db.BackfillState, error)
	InitStatsBackfill(ctx context.Context, target, targetTimestamp uint64) error
}

type reconcileStore interface {
	backfillInitStore
	Reconcile(ctx context.Context) error
}

// ReconcileForStart re-derives the counters, finishing a FORCE_REINDEX wipe a stop interrupted (Reconcile), then
// prepares the stats backfill when one is wanted. The prepare comes second: the resumed wipe removes a backfill.
func ReconcileForStart(ctx context.Context, store reconcileStore, node RPCClient, startBlock uint64, backfill, traces bool) error {
	if err := store.Reconcile(ctx); err != nil {
		return err
	}
	if !backfill {
		return nil
	}
	if err := PrepareStatsBackfill(ctx, store, node, startBlock, traces); err != nil {
		return fmt.Errorf("prepare the stats backfill: %w", err)
	}
	return nil
}

// PrepareStatsBackfill starts the stats backfill of [0, startBlock) unless one exists. The whole range, and the whole
// day holding startBlock, must be below the node's reorg floor: a reorg there could not be reverted (the day's first
// blocks are pruned from day one). With traces, the node must trace the range's old blocks (historicalTraces), checked
// again at every start while blocks remain to count: the node may have been replaced since.
func PrepareStatsBackfill(ctx context.Context, store backfillInitStore, node RPCClient, startBlock uint64, traces bool) error {
	if startBlock == 0 {
		return nil
	}
	s, err := store.StatsBackfillState(ctx)
	if err != nil {
		return err
	}
	if s.Exists {
		if traces && !s.Complete && s.Through < s.Target {
			return historicalTraces(ctx, node, startBlock)
		}
		return nil
	}
	head, err := node.BlockNumber(ctx)
	if err != nil {
		return err
	}
	if startBlock+db.PruneFloorGap > head {
		return fmt.Errorf("START_BLOCK %d is within the reorg floor (%d blocks) of the node head %d: the stats backfill range must be final",
			startBlock, db.PruneFloorGap, head)
	}
	first, err := node.RawBlockByNumber(ctx, startBlock)
	if err != nil {
		return err
	}
	floor, err := node.RawBlockByNumber(ctx, head-db.PruneFloorGap)
	if err != nil {
		return err
	}
	if day, floorDay := blockDay(uint64(first.Timestamp)), blockDay(uint64(floor.Timestamp)); !day.Before(floorDay) {
		return fmt.Errorf("START_BLOCK %d is on %s, the day of the reorg floor (block %d): choose a START_BLOCK whose whole day is final",
			startBlock, day.Format("2006-01-02"), head-db.PruneFloorGap)
	}
	if traces {
		if err := historicalTraces(ctx, node, startBlock); err != nil {
			return err
		}
	}
	prev, err := node.RawBlockByNumber(ctx, startBlock-1)
	if err != nil {
		return err
	}
	log.Info("stats backfill: counting the blocks below START_BLOCK", "start_block", startBlock, "head", head)
	return store.InitStatsBackfill(ctx, startBlock-1, uint64(prev.Timestamp))
}
