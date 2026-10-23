package indexer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/events"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"

	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

type RealtimeConfig struct {
	ConfirmationBlocks int           // Blocks to wait before processing (default: 1)
	PollInterval       time.Duration // Fallback poll interval if WS fails
}

type RealtimeIndexer struct {
	db       Database
	rpc      RPCClient
	config   *RealtimeConfig
	idxCfg   *Config // Main indexer config for RPC settings
	eventBus *events.Bus

	// Caches (shared with main indexer)
	tokenCache    *TokenCache
	contractCache *ContractCache

	// Balance workers (shared with main indexer)
	balanceWorkers *BalanceWorkerPool

	// Shared with the main indexer (see Indexer.dailyStats).
	dailyStats *dailyStatsThrottle

	// Tracing support
	tracingSupported bool

	// State tracking
	lastProcessedBlock uint64
	// behind is how many blocks realtime has left before the node's safe head; catch-up yields to it (Behind).
	behind   atomic.Uint64
	lastSafe atomic.Uint64
	mu       sync.RWMutex
	receipts receiptSkip
	// failedRevertFrom is the fork point of a revert that returned an error, until the next tick has checked what it
	// left stored; 0 when there is none.
	failedRevertFrom uint64

	// Coordination
	ctx    context.Context
	cancel context.CancelFunc

	// On-demand indexing requests channel
	indexRequests chan indexRequest
}

func NewRealtimeIndexer(
	database Database,
	rpcClient RPCClient,
	cfg *RealtimeConfig,
	idxCfg *Config,
	tokenCache *TokenCache,
	contractCache *ContractCache,
	balanceWorkers *BalanceWorkerPool,
	tracingSupported bool,
) *RealtimeIndexer {
	ctx, cancel := context.WithCancel(context.Background())
	return &RealtimeIndexer{
		db:               database,
		rpc:              rpcClient,
		config:           cfg,
		idxCfg:           idxCfg,
		tokenCache:       tokenCache,
		contractCache:    contractCache,
		balanceWorkers:   balanceWorkers,
		tracingSupported: tracingSupported,
		indexRequests:    make(chan indexRequest, 100),
		ctx:              ctx,
		cancel:           cancel,
	}
}

// SetDailyStatsThrottle shares the main indexer's day-aggregate throttle with
// the per-block Indexer values this realtime indexer builds.
func (r *RealtimeIndexer) SetDailyStatsThrottle(t *dailyStatsThrottle) {
	r.dailyStats = t
}

func (r *RealtimeIndexer) SetEventBus(bus *events.Bus) {
	r.eventBus = bus
}

func (r *RealtimeIndexer) SetLastProcessedBlock(blockNum uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastProcessedBlock = blockNum
}

func (r *RealtimeIndexer) GetLastProcessedBlock() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastProcessedBlock
}

func (r *RealtimeIndexer) IndexBlock(ctx context.Context, blockNumber uint64) error {
	done := make(chan error, 1)
	select {
	case r.indexRequests <- indexRequest{blockNumber: blockNumber, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *RealtimeIndexer) Start(ctx context.Context, startBlock uint64) error {
	r.SetLastProcessedBlock(startBlock)

	log.Info("realtime: starting",
		"from_block", startBlock,
		"confirmation_blocks", r.config.ConfirmationBlocks)

	headers := make(chan *rpclient.Header)
	sub, err := r.rpc.SubscribeNewHead(ctx, headers)
	if err != nil {
		log.Warn("realtime: WebSocket subscription failed, falling back to polling", "error", err)
		return r.runPollingMode(ctx)
	}

	log.Info("realtime: WebSocket subscription active")
	return r.runSubscriptionMode(ctx, headers, sub)
}

func (r *RealtimeIndexer) Stop() {
	r.cancel()
}

func (r *RealtimeIndexer) runSubscriptionMode(ctx context.Context, headers <-chan *rpclient.Header, sub rpclient.Subscription) error {
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-r.ctx.Done():
			return r.ctx.Err()

		case err := <-sub.Err():
			log.Warn("realtime: subscription error, switching to polling", "error", err)
			return r.runPollingMode(ctx)

		case req := <-r.indexRequests:
			err := r.processBlock(ctx, req.blockNumber)
			req.done <- err

		case header := <-headers:
			if header == nil {
				continue
			}

			latestBlock := header.Number.Uint64()
			safeBlock := latestBlock - uint64(r.config.ConfirmationBlocks)

			lastProcessed := r.jumpIfFarBehind(ctx, safeBlock)
			r.indexUpTo(ctx, lastProcessed, safeBlock)
		}
	}
}

// runPollingMode is the fallback when WebSocket subscriptions are unavailable
func (r *RealtimeIndexer) runPollingMode(ctx context.Context) error {
	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-r.ctx.Done():
			return r.ctx.Err()

		case req := <-r.indexRequests:
			err := r.processBlock(ctx, req.blockNumber)
			req.done <- err

		case <-ticker.C:
			latestOnChain, err := r.rpc.BlockNumber(ctx)
			if err != nil {
				log.Error("realtime: failed to get latest block number", "error", err)
				continue
			}

			safeBlock := latestOnChain - uint64(r.config.ConfirmationBlocks)
			lastProcessed := r.jumpIfFarBehind(ctx, safeBlock)
			r.indexUpTo(ctx, lastProcessed, safeBlock)
		}
	}
}

// indexUpTo indexes lastProcessed+1..safeBlock. The reorg check runs once, on the block realtime stands on; the
// blocks are then read up to blockPrefetchDepth ahead while earlier ones commit, each checked against its parent
// (indexPrefetched), with the cursor and Behind updated per block. Walking block by block, with detectReorg's ten
// hash reads before each, held realtime to ~3.8 blocks/s in a load test against a chain at ~8.4 (PRST-5438): after a
// stall it could never catch up on its own.
func (r *RealtimeIndexer) indexUpTo(ctx context.Context, lastProcessed, safeBlock uint64) {
	if safeBlock <= lastProcessed {
		return
	}
	if lastProcessed > 0 {
		depth, err := r.detectReorg(ctx, lastProcessed)
		if err != nil {
			log.Error("realtime: failed to check for reorg", "error", err)
		} else if depth > 0 {
			log.Warn("realtime: detected reorg", "depth", depth)
			if err := r.handleReorg(ctx, lastProcessed+1-depth); err != nil {
				log.Error("realtime: failed to handle reorg", "error", err)
			}
			r.SetLastProcessedBlock(lastProcessed - depth)
			return
		}
	}
	idx := r.blockIndexer()
	idx.indexRequests = r.indexRequests
	idx.onIndexed = func(n uint64) {
		r.SetLastProcessedBlock(n)
		r.behind.Store(safeBlock - n)
	}
	r.SetLastProcessedBlock(idx.indexPrefetched(ctx, lastProcessed, safeBlock, &r.receipts))
}

// resumeAfterFailedRevert moves the cursor down to what a failed revert left stored. A delete that reported an error
// may have committed (a lost acknowledgement), and the reorg check skips blocks that are not stored, so with the
// cursor still above them the deleted blocks would never be indexed again. The cursor goes to the highest stored
// block at or below it, and no lower than the block before the fork: a block the revert deleted is indexed again,
// and a stale block it left is found by the reorg check again. It reports false when the store cannot be read.
func (r *RealtimeIndexer) resumeAfterFailedRevert(ctx context.Context) bool {
	if r.failedRevertFrom == 0 {
		return true
	}
	cursor := r.GetLastProcessedBlock()
	below, err := r.db.HighestBlockAtOrBelow(ctx, cursor)
	if err != nil {
		log.Error("realtime: failed to read what a failed revert left stored, retrying on the next tick", "error", err)
		return false
	}
	resume := r.failedRevertFrom - 1
	if below != nil && below.Number > resume {
		resume = below.Number
	}
	if resume < cursor {
		log.Warn("realtime: a failed revert left blocks deleted; indexing again from the highest stored block",
			"cursor", cursor, "from", resume)
		r.SetLastProcessedBlock(resume)
	}
	r.failedRevertFrom = 0
	return true
}

func (r *RealtimeIndexer) processBlock(ctx context.Context, number uint64) error {
	return r.processBlockRaw(ctx, number)
}

func (r *RealtimeIndexer) processBlockRaw(ctx context.Context, number uint64) error {
	return r.blockIndexer().processBlockRaw(ctx, number)
}

func (r *RealtimeIndexer) blockIndexer() *Indexer {
	return &Indexer{
		db:               r.db,
		rpc:              r.rpc,
		config:           r.idxCfg,
		tokenCache:       r.tokenCache,
		contractCache:    r.contractCache,
		balanceWorkers:   r.balanceWorkers,
		tracingSupported: r.tracingSupported,
		eventBus:         r.eventBus,
		dailyStats:       r.dailyStats,
	}
}

// Behind is how many blocks realtime had left before the node's safe head when it last looked: 0 once caught up.
func (r *RealtimeIndexer) Behind() uint64 {
	if r.GetLastProcessedBlock() >= r.lastSafe.Load() {
		return 0
	}
	return r.behind.Load()
}

// jumpIfFarBehind returns where realtime continues from. More than RealtimeMaxBehind blocks behind safeBlock (a long
// stall), it moves to just below safeBlock, as a fresh start does, and registers the blocks it skips with catch-up:
// walked one by one, they kept the explorer's head minutes old while the chain moved on.
func (r *RealtimeIndexer) jumpIfFarBehind(ctx context.Context, safeBlock uint64) uint64 {
	lastProcessed := r.GetLastProcessedBlock()
	r.lastSafe.Store(safeBlock)
	if safeBlock > lastProcessed {
		r.behind.Store(safeBlock - lastProcessed)
	}
	limit := r.idxCfg.RealtimeMaxBehind
	if limit == 0 || safeBlock <= lastProcessed+limit {
		return lastProcessed
	}
	skipped := db.BlockRange{FromNumber: lastProcessed + 1, ToNumber: safeBlock - 1}
	if err := r.db.SaveMissingRanges(ctx, []db.BlockRange{skipped}); err != nil {
		log.Error("realtime: registering the blocks a jump to the head would skip failed; walking them instead", "error", err)
		return lastProcessed
	}
	log.Warn("realtime: far behind the node; jumping to the head, catch-up fills the skipped blocks",
		"from", skipped.FromNumber, "to", skipped.ToNumber, "max_behind", limit)
	r.SetLastProcessedBlock(safeBlock - 1)
	return safeBlock - 1
}

// detectReorg: see Indexer.detectReorg. Same walk, same fix (PRST-5320): it
// scans the whole 10-block window and returns the depth of the deepest stored
// block that no longer matches the chain, not 1 as soon as the top mismatches.
func (r *RealtimeIndexer) detectReorg(ctx context.Context, blockNumber uint64) (uint64, error) {
	maxReorgCheck := uint64(10)
	var deepest uint64
	for depth := uint64(0); depth < maxReorgCheck && blockNumber > depth; depth++ {
		checkBlock := blockNumber - depth

		storedBlock, err := r.db.GetBlock(ctx, checkBlock)
		if err != nil {
			return deepest, err
		}
		if storedBlock == nil {
			continue
		}

		chainHash, err := r.rpc.RawBlockHash(ctx, checkBlock)
		if err != nil {
			return deepest, err
		}

		if storedBlock.Hash == chainHash {
			continue
		}
		storedHash, chainHashLog := storedBlock.Hash, chainHash
		if len(storedHash) > 16 {
			storedHash = storedHash[:16]
		}
		if len(chainHashLog) > 16 {
			chainHashLog = chainHashLog[:16]
		}
		log.Warn("realtime: reorg detected",
			"block", checkBlock,
			"stored_hash", storedHash,
			"chain_hash", chainHashLog)
		deepest = depth + 1
	}
	return deepest, nil
}

func (r *RealtimeIndexer) handleReorg(ctx context.Context, fromBlock uint64) error {
	log.Info("realtime: reverting blocks due to reorg", "from_block", fromBlock)

	lastIndexed, err := r.db.GetLatestBlockNumber(ctx)
	if err != nil {
		return err
	}

	// Detect chain reset: if we need to revert more than maxReorgDepth blocks,
	// this is not a reorg, it is a chain reset.
	if lastIndexed > fromBlock && lastIndexed-fromBlock > maxReorgDepth {
		metrics.ChainResetDetected()
		return fmt.Errorf(
			"chain reset detected: need to revert %d blocks (max reorg depth: %d). "+
				"Restart with FORCE_REINDEX=true to auto-wipe",
			lastIndexed-fromBlock, maxReorgDepth)
	}

	metrics.ReorgDetected()

	for blockNum := lastIndexed; blockNum >= fromBlock; blockNum-- {
		if err := r.db.DeleteBlock(ctx, blockNum); err != nil {
			log.Error("realtime: failed to delete block", "block", blockNum, "error", err)
			return err
		}
		log.Debug("realtime: reverted block", "block", blockNum)
	}

	return nil
}
