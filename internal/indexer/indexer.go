package indexer

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/events"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// maxReorgDepth is the maximum number of blocks we expect a legitimate reorg
// to span. If the chain head is more than this many blocks behind our last
// indexed block, we treat it as a chain reset (e.g. Anvil restart) rather
// than a reorg.
const maxReorgDepth = 128

type Config struct {
	RPCWorkers           int
	RPCRateLimit         int
	DBBatchSize          int
	TokenMetadataWorkers int
	BalanceWorkers       int
	EnableAsyncBalance   bool

	EnableTracing  bool
	TraceRateLimit int
	TraceWorkers   int

	CatchupEnabled   bool
	CatchupWorkers   int
	CatchupBatchSize int
	CatchupQueueSize int
	// RealtimeMaxBehind: see config.Config.RealtimeMaxBehind.
	RealtimeMaxBehind uint64

	SkipAddressStats   bool
	SkipReceiptTxTypes map[int]bool // tx types to skip receipt fetching for (e.g. 126 for OP deposit)

	// RetentionEnabled: history below a window is (or will be) pruned. Catch-up then writes address_stats per
	// block instead of rebuilding the table from history afterwards, which would drop the pruned counts.
	RetentionEnabled bool

	// SkipAddressStatsRebuild skips the address_stats rebuild after the initial catch-up (retention off), which
	// truncates the table and blocks ingest for as long as it runs (ADDRESS_STATS_REBUILD=false).
	SkipAddressStatsRebuild bool

	// DailyStatsMinInterval is the minimum time between two daily_stats
	// advances. Zero advances on every 100th block. See dailyStatsThrottle.
	DailyStatsMinInterval time.Duration

	// CommitPipelineDepth is how many blocks the standard loop writes at once (the pipelined
	// commit). 1 is the sequential committer. See indexPipelined.
	CommitPipelineDepth int

	// BalanceDedupWindow holds balance work per (address, token) for this long before one fetch.
	BalanceDedupWindow time.Duration

	// BalanceReaderBatch > 1 reads up to that many balances per eth_call through BalanceReader;
	// BalanceSkipL1 sends skipL1Charging with those calls.
	BalanceReaderBatch int
	BalanceSkipL1      bool

	EnableOPDeposits bool
}

var transferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

var zeroAddress = common.HexToAddress("0x0000000000000000000000000000000000000000")

type Indexer struct {
	db           Database
	rpc          RPCClient
	pollInterval time.Duration
	startBlock   uint64

	indexRequests chan indexRequest
	// onIndexed, when set, is told each block indexPrefetched commits (realtime moves its cursor with it).
	onIndexed func(uint64)
	config    *Config

	tokenCache    *TokenCache
	contractCache *ContractCache

	balanceWorkers *BalanceWorkerPool

	tracingSupported bool
	eventBus         *events.Bus

	missingRangeCollector *MissingRangeCollector

	catchupIndexer  *CatchupIndexer
	realtimeIndexer *RealtimeIndexer
	// liveRealtime is realtimeIndexer for readers outside the indexer's goroutine (IngestHead).
	liveRealtime atomic.Pointer[RealtimeIndexer]

	catchupRunning bool

	// dailyStats is shared with the catchup path: CatchupIndexer builds a fresh
	// Indexer value for every block it processes, so a value field here would
	// be a fresh, never-run throttle per block and fire the day aggregate on
	// every one of them (measured: 104 runs for 105 blocks, 22 s each).
	dailyStats *dailyStatsThrottle
}

// dailyStatsThrottle gates the day-aggregate recomputation: at most one run in
// flight, and at most one start per minInterval (or per 100 blocks when
// minInterval is zero, the pre-PRST-4669 behaviour). Every block, empty or not,
// marks its day dirty and every run recomputes all dirty days, so a day is
// recomputed after its last block whatever order its blocks are committed in.
// Days left dirty when no later block arrives (the last burst before a quiet
// period) are recomputed by a timer once the interval has passed.
type dailyStatsThrottle struct {
	mu      sync.Mutex
	last    time.Time
	running bool
	armed   bool
	dirty   map[time.Time]struct{}
}

func (t *dailyStatsThrottle) due(blockNumber uint64, day time.Time, minInterval time.Duration) []time.Time {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirty == nil {
		t.dirty = make(map[time.Time]struct{})
	}
	if !day.IsZero() {
		t.dirty[day] = struct{}{}
	}
	if t.running || len(t.dirty) == 0 {
		return nil
	}
	if minInterval <= 0 {
		if blockNumber%100 != 0 {
			return nil
		}
	} else if !t.last.IsZero() && time.Since(t.last) < minInterval {
		return nil
	}
	days := make([]time.Time, 0, len(t.dirty))
	for d := range t.dirty {
		days = append(days, d)
	}
	sort.Slice(days, func(a, b int) bool { return days[a].Before(days[b]) })
	clear(t.dirty)
	t.last = time.Now()
	t.running = true
	return days
}

func (t *dailyStatsThrottle) markDirty(day time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirty == nil {
		t.dirty = make(map[time.Time]struct{})
	}
	t.dirty[day] = struct{}{}
}

func (t *dailyStatsThrottle) done() {
	t.mu.Lock()
	t.running = false
	t.mu.Unlock()
}

// arm reports whether a deferred run must be scheduled, and when: dirty days are
// waiting, no run is in flight to pick them up afterwards, and none is scheduled.
func (t *dailyStatsThrottle) arm(minInterval time.Duration) (time.Duration, bool) {
	if t == nil || minInterval <= 0 {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running || t.armed || len(t.dirty) == 0 {
		return 0, false
	}
	t.armed = true
	return max(minInterval-time.Since(t.last), time.Millisecond), true
}

func (t *dailyStatsThrottle) disarm() {
	t.mu.Lock()
	t.armed = false
	t.mu.Unlock()
}

func (i *Indexer) SetEventBus(bus *events.Bus) {
	i.eventBus = bus
}

func (i *Indexer) GetCatchupProgress() (processed int64, total uint64, percentComplete float64, isRunning bool) {
	if i.catchupIndexer == nil || !i.catchupRunning {
		return 0, 0, 100, false
	}
	processed, total, percentComplete = i.catchupIndexer.Progress()
	return processed, total, percentComplete, true
}

func (i *Indexer) IsCatchupRunning() bool {
	return i.catchupRunning
}

func (i *Indexer) GetMissingRangeProgress() (minFetched, maxFetched, chainHead uint64, backfillComplete bool, totalMissing int64) {
	if i.missingRangeCollector == nil {
		return 0, 0, 0, true, 0
	}
	minFetched, maxFetched, chainHead, backfillComplete = i.missingRangeCollector.GetProgress()
	totalMissing, _ = i.missingRangeCollector.GetTotalMissingBlocks(context.Background())
	return
}

type indexRequest struct {
	blockNumber uint64
	done        chan error
}

func New(database Database, rpcClient RPCClient, pollInterval time.Duration, startBlock uint64) *Indexer {
	return NewWithConfig(database, rpcClient, pollInterval, startBlock, &Config{
		RPCWorkers:           50,
		RPCRateLimit:         500,
		DBBatchSize:          500,
		TokenMetadataWorkers: 20,
		BalanceWorkers:       30,
		EnableAsyncBalance:   true,
		EnableTracing:        false,
		TraceRateLimit:       50,
		TraceWorkers:         10,
		CatchupEnabled:       true,
		CatchupWorkers:       10,
		CatchupBatchSize:     100,
		CatchupQueueSize:     1000,
	})
}

func NewWithConfig(database Database, rpcClient RPCClient, pollInterval time.Duration, startBlock uint64, cfg *Config) *Indexer {
	idx := &Indexer{
		db:            database,
		rpc:           rpcClient,
		pollInterval:  pollInterval,
		startBlock:    startBlock,
		indexRequests: make(chan indexRequest, 100),
		config:        cfg,
		tokenCache:    NewTokenCache(),
		contractCache: NewContractCache(),
		dailyStats:    &dailyStatsThrottle{},
	}

	if cfg.EnableAsyncBalance && cfg.BalanceWorkers > 0 {
		idx.balanceWorkers = NewBalanceWorkerPool(database, rpcClient, cfg.BalanceWorkers, cfg.RPCRateLimit)
		idx.balanceWorkers.SetDedupWindow(cfg.BalanceDedupWindow)
		idx.balanceWorkers.SetReaderBatch(cfg.BalanceReaderBatch, cfg.BalanceSkipL1)
		// No post-flush stats refresh any more: holder_count is maintained by
		// InsertBalancesBatch itself, in the transaction that writes the
		// balances (PRST-4669). The callback used to trigger a COUNT(*) per
		// token per flush of each worker -- ~7 per indexed block under load.
	}

	return idx
}

func (i *Indexer) IndexBlock(ctx context.Context, blockNumber uint64) error {
	done := make(chan error, 1)
	select {
	case i.indexRequests <- indexRequest{blockNumber: blockNumber, done: done}:
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

const (
	progressMetricsInterval = 15 * time.Second

	// GetBlockCount is a COUNT(*) over blocks, so it runs far less often than
	// the cheap gauges. Backfill holes change slowly; lag does not.
	missingBlocksInterval = 5 * time.Minute
)

// refreshProgress updates the cheap gauges and reports the chain head.
func (i *Indexer) refreshProgress(ctx context.Context) (head uint64, ok bool) {
	head, err := i.rpc.BlockNumber(ctx)
	if err != nil {
		log.Debug("progress metrics: chain head unavailable", "error", err)
		return 0, false
	}
	metrics.SetChainHead(head)

	if indexed, err := i.db.GetLatestBlockNumber(ctx); err == nil {
		metrics.SetLastIndexed(indexed)
	}
	if i.balanceWorkers != nil {
		metrics.SetQueueDepth(metrics.QueueBalance, i.balanceWorkers.QueueSize())
	}
	return head, true
}

// missingBlockCount is the number of blocks in [startBlock, head] absent from
// the database. The range starts at startBlock, not genesis: assuming genesis
// makes a fully caught-up database report startBlock missing forever.
func missingBlockCount(startBlock, head uint64, count int64) int64 {
	if head < startBlock {
		return 0
	}
	missing := int64(head-startBlock+1) - count
	if missing < 0 {
		return 0
	}
	return missing
}

// refreshMissingBlocks counts real holes, which head-last_indexed cannot see:
// that only measures the tip and reads zero while blocks behind it are absent.
func (i *Indexer) refreshMissingBlocks(ctx context.Context, head uint64) {
	if head < i.startBlock {
		metrics.SetMissingBlocks(0)
		return
	}
	count, err := i.db.GetBlockCountInRange(ctx, i.startBlock, head)
	if err != nil {
		return
	}
	metrics.SetMissingBlocks(missingBlockCount(i.startBlock, head, count))
}

func (i *Indexer) publishProgressMetrics(ctx context.Context) {
	ticker := time.NewTicker(progressMetricsInterval)
	defer ticker.Stop()
	missingTicker := time.NewTicker(missingBlocksInterval)
	defer missingTicker.Stop()

	// Seed before the first tick. Gauges register at zero and a ticker does not
	// fire immediately, so otherwise every restart reports no lag and no missing
	// blocks for five minutes -- suppressing the alerts these exist to raise.
	head, ok := i.refreshProgress(ctx)
	if ok {
		i.refreshMissingBlocks(ctx, head)
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if h, o := i.refreshProgress(ctx); o {
				head, ok = h, true
			}

		case <-missingTicker.C:
			if ok {
				i.refreshMissingBlocks(ctx, head)
			}
		}
	}
}

func (i *Indexer) Start(ctx context.Context) error {
	// Blocks at or below the prune watermark are gone on purpose: gap counting, the missing-range collector
	// and catch-up all start above it, or they would fetch the pruned history back.
	if wm, _, err := i.db.PrunedThrough(ctx); err != nil {
		return fmt.Errorf("read prune watermark: %w", err)
	} else if wm > 0 && wm >= i.startBlock {
		log.Info("starting above the prune watermark", "start_block", i.startBlock, "pruned_through", wm)
		i.startBlock = wm + 1
	}

	go i.publishProgressMetrics(ctx)

	lastIndexed, err := i.db.GetLatestBlockNumber(ctx)
	if err != nil {
		log.Error("failed to get latest indexed block", "error", err)
	}

	if err := i.tokenCache.LoadFromDB(ctx, i.db); err != nil {
		log.Warn("failed to pre-load token cache", "error", err)
	} else {
		log.Info("loaded tokens into cache", "count", i.tokenCache.Size())
	}

	if i.config.EnableTracing {
		supported, err := i.rpc.CheckTracingSupport(ctx)
		if err != nil {
			log.Warn("failed to check tracing support", "error", err)
		} else if supported {
			i.tracingSupported = true
			log.Info("tracing enabled: node supports debug_traceBlockByNumber")
		} else {
			log.Warn("tracing requested but node does not support it, disabling")
			i.tracingSupported = false
		}
	}

	if i.balanceWorkers != nil {
		i.balanceWorkers.Start()
	}

	latestOnChain, err := i.rpc.BlockNumber(ctx)
	if err != nil {
		return err
	}

	blockCount, err := i.db.GetBlockCount(ctx)
	if err != nil {
		log.Warn("failed to get block count", "error", err)
	}

	var expectedBlocks uint64
	if lastIndexed > i.startBlock {
		expectedBlocks = lastIndexed - i.startBlock + 1
	}
	hasGaps := blockCount < int64(expectedBlocks)

	blocksToSync := uint64(0)
	if latestOnChain > lastIndexed {
		blocksToSync = latestOnChain - lastIndexed
	}

	log.Info("indexer starting",
		"last_indexed", lastIndexed,
		"chain_head", latestOnChain,
		"blocks_indexed", blockCount,
		"expected_blocks", expectedBlocks,
		"has_gaps", hasGaps,
		"blocks_behind", blocksToSync)

	// Detect chain reset: if the chain head is significantly behind our last
	// indexed block, this is a chain reset (e.g. Anvil restart), not a reorg.
	if lastIndexed > 0 && latestOnChain+maxReorgDepth < lastIndexed {
		metrics.ChainResetDetected()
		log.Error("CHAIN RESET DETECTED: chain head is far behind last indexed block",
			"chain_head", latestOnChain,
			"last_indexed", lastIndexed,
			"delta", lastIndexed-latestOnChain,
			"max_reorg_depth", maxReorgDepth)

		if os.Getenv("FORCE_REINDEX") == "true" {
			log.Warn("FORCE_REINDEX=true: wiping indexed data and re-indexing from scratch")
			if err := i.db.WipeAllData(ctx); err != nil {
				return fmt.Errorf("failed to wipe data for reindex: %w", err)
			}
			lastIndexed = 0
			blockCount = 0
			expectedBlocks = 0
			hasGaps = false
			blocksToSync = latestOnChain
			log.Info("data wiped, re-indexing from block 0")
		} else {
			return fmt.Errorf(
				"chain reset detected (chain_head=%d, last_indexed=%d). "+
					"The chain was likely restarted with fresh state. "+
					"Wipe the explorer DB and restart, or set FORCE_REINDEX=true to auto-wipe",
				latestOnChain, lastIndexed)
		}
	}

	// The stored head may be on a stale fork: a reorg while the indexer was down, or a revert that kept failing
	// until a restart. It is reverted before the start mode is chosen: collector mode starts realtime at the node
	// head, whose reorg check never reaches below its own cursor, and catch-up fills the gap with no parent check.
	// Stored blocks above the node's head cannot be compared yet; a stale fork below them takes them with it.
	if top := min(lastIndexed, latestOnChain); top > 0 {
		head, err := i.revertStaleHead(ctx, top)
		if err != nil {
			return err
		}
		if head < top {
			lastIndexed = head
			blocksToSync = latestOnChain - lastIndexed
		}
	}

	i.db.UpdateSyncStatus(ctx, lastIndexed, true)
	catchupThreshold := uint64(100)
	useCatchup := i.config.CatchupEnabled && (hasGaps || blocksToSync > catchupThreshold)

	if useCatchup {
		return i.startWithMissingRangeCollector(ctx, latestOnChain)
	}

	return i.startStandardMode(ctx, lastIndexed)
}

// newCatchupIndexer and newRealtimeIndexer build the sub-indexers with the
// state they share with this one, including the daily-stats throttle.
func (i *Indexer) newCatchupIndexer(cfg *CatchupConfig) *CatchupIndexer {
	c := NewCatchupIndexer(
		i.db, i.rpc, cfg, i.config,
		i.tokenCache, i.contractCache, i.balanceWorkers,
		i.tracingSupported,
	)
	c.SetEventBus(i.eventBus)
	c.SetDailyStatsThrottle(i.dailyStats)
	return c
}

// IngestHead is the block the head path has written up to: realtime's cursor once realtime runs. The highest stored
// block is not it: catch-up can store blocks near the node head while realtime is far behind. Before realtime
// starts (standard mode, or startup) it is the highest stored block.
func (i *Indexer) IngestHead(ctx context.Context) (uint64, error) {
	if r := i.liveRealtime.Load(); r != nil {
		return r.GetLastProcessedBlock(), nil
	}
	return i.db.GetLatestBlockNumber(ctx)
}

func (i *Indexer) newRealtimeIndexer(cfg *RealtimeConfig) *RealtimeIndexer {
	r := NewRealtimeIndexer(
		i.db, i.rpc, cfg, i.config,
		i.tokenCache, i.contractCache, i.balanceWorkers,
		i.tracingSupported,
	)
	r.SetEventBus(i.eventBus)
	r.SetDailyStatsThrottle(i.dailyStats)
	return r
}

func (i *Indexer) startWithCatchup(ctx context.Context, lastIndexed, latestOnChain uint64) error {
	log.Info("starting catchup mode",
		"from_block", lastIndexed+1,
		"to_block", latestOnChain,
		"workers", i.config.CatchupWorkers)

	catchupCfg := &CatchupConfig{
		Workers:   i.config.CatchupWorkers,
		BatchSize: i.config.CatchupBatchSize,
		QueueSize: i.config.CatchupQueueSize,
	}

	i.catchupIndexer = i.newCatchupIndexer(catchupCfg)

	realtimeCfg := &RealtimeConfig{
		ConfirmationBlocks: 1,
		PollInterval:       i.pollInterval,
	}

	i.realtimeIndexer = i.newRealtimeIndexer(realtimeCfg)
	i.catchupIndexer.SetRealtimeBehind(i.realtimeIndexer.Behind)
	i.liveRealtime.Store(i.realtimeIndexer)

	i.catchupRunning = true
	i.catchupIndexer.SetOnComplete(func() {
		i.catchupRunning = false
		log.Info("catchup indexing completed, realtime indexer continues")
	})

	targetBlock := latestOnChain - 1
	if err := i.catchupIndexer.Start(ctx, lastIndexed+1, targetBlock); err != nil {
		return err
	}

	go func() {
		if err := i.realtimeIndexer.Start(ctx, targetBlock); err != nil {
			log.Error("realtime indexer error", "error", err)
		}
	}()

	<-ctx.Done()

	if i.catchupIndexer != nil {
		i.catchupIndexer.Stop()
	}
	if i.realtimeIndexer != nil {
		i.realtimeIndexer.Stop()
	}
	if i.balanceWorkers != nil {
		i.balanceWorkers.Stop()
	}

	return ctx.Err()
}

// linkCatchupToRealtime makes catch-up yield while realtime is far behind and stops the collector's forward scan at
// realtime's cursor, so the two never write the same blocks at the tip.
func (i *Indexer) linkCatchupToRealtime() {
	i.catchupIndexer.SetRealtimeBehind(i.realtimeIndexer.Behind)
	i.missingRangeCollector.SetCeiling(i.realtimeIndexer.GetLastProcessedBlock)
}

func (i *Indexer) startWithMissingRangeCollector(ctx context.Context, latestOnChain uint64) error {
	log.Info("starting with missing range collector",
		"start_block", i.startBlock,
		"chain_head", latestOnChain,
		"workers", i.config.CatchupWorkers)

	collectorCfg := &MissingRangeCollectorConfig{
		BatchSize:            100000,                // Scan 100k blocks at a time
		BackwardScanInterval: 10 * time.Millisecond, // Fast initial backward scan
		ForwardScanInterval:  1 * time.Minute,       // Check for new blocks every minute
		FirstBlock:           i.startBlock,          // Start from configured block (usually 0)
		LastBlock:            latestOnChain,         // Scan up to current chain head
	}

	i.missingRangeCollector = NewMissingRangeCollector(i.db, i.rpc, collectorCfg)
	if err := i.missingRangeCollector.Start(ctx); err != nil {
		return err
	}

	catchupCfg := &CatchupConfig{
		Workers:   i.config.CatchupWorkers,
		BatchSize: i.config.CatchupBatchSize,
		QueueSize: i.config.CatchupQueueSize,
	}

	i.catchupIndexer = i.newCatchupIndexer(catchupCfg)
	i.catchupIndexer.SetCollector(i.missingRangeCollector)

	realtimeCfg := &RealtimeConfig{
		ConfirmationBlocks: 1,
		PollInterval:       i.pollInterval,
	}

	i.realtimeIndexer = i.newRealtimeIndexer(realtimeCfg)
	i.liveRealtime.Store(i.realtimeIndexer)
	i.linkCatchupToRealtime()

	i.catchupRunning = true
	i.catchupIndexer.SetOnComplete(func() {
		i.catchupRunning = false
		log.Info("catchup indexing completed, realtime indexer continues")
	})

	if err := i.catchupIndexer.Start(ctx, i.startBlock, latestOnChain); err != nil {
		return err
	}

	go func() {
		if err := i.realtimeIndexer.Start(ctx, latestOnChain); err != nil {
			log.Error("realtime indexer error", "error", err)
		}
	}()

	<-ctx.Done()

	if i.missingRangeCollector != nil {
		i.missingRangeCollector.Stop()
	}
	if i.catchupIndexer != nil {
		i.catchupIndexer.Stop()
	}
	if i.realtimeIndexer != nil {
		i.realtimeIndexer.Stop()
	}
	if i.balanceWorkers != nil {
		i.balanceWorkers.Stop()
	}

	return ctx.Err()
}

func (i *Indexer) startStandardMode(ctx context.Context, lastIndexed uint64) error {
	log.Info("starting standard mode",
		"workers", i.config.RPCWorkers,
		"rate_limit", i.config.RPCRateLimit,
		"tracing", i.tracingSupported)

	ticker := time.NewTicker(i.pollInterval)
	defer ticker.Stop()
	var receipts receiptSkip

	for {
		select {
		case <-ctx.Done():
			if i.balanceWorkers != nil {
				i.balanceWorkers.Stop()
			}
			i.db.UpdateSyncStatus(ctx, lastIndexed, false)
			return ctx.Err()

		case req := <-i.indexRequests:
			err := i.processBlock(ctx, req.blockNumber)
			req.done <- err

		case <-ticker.C:
			latestOnChain, err := i.rpc.BlockNumber(ctx)
			if err != nil {
				log.Error("failed to get latest block number", "error", err)
				continue
			}

			if lastIndexed > 0 {
				reorgDepth, err := i.detectReorg(ctx, lastIndexed)
				if err != nil {
					log.Error("failed to check for reorg, retrying on the next tick", "error", err, "stale_blocks_found", reorgDepth)
					continue
				}
				if reorgDepth > 0 {
					log.Warn("detected reorg, reverting blocks", "depth", reorgDepth)
					if err := i.handleReorg(ctx, lastIndexed-reorgDepth+1); err != nil {
						log.Error("failed to handle reorg", "error", err)
						continue
					}
					lastIndexed = lastIndexed - reorgDepth
				}
			}

			if i.config.CommitPipelineDepth > 1 {
				lastIndexed = i.indexPipelined(ctx, lastIndexed, latestOnChain, i.config.CommitPipelineDepth, &receipts)
			} else {
				lastIndexed = i.indexPrefetched(ctx, lastIndexed, latestOnChain, &receipts)
			}
		}
	}
}

// detectReorg walks down from blockNumber comparing stored and chain hashes and
// returns how many blocks, counted from blockNumber, no longer match the chain.
//
// It used to return at the FIRST mismatch, i.e. always depth 1 when the head
// block differed, so a reorg deeper than one block reverted only the head and
// left the stale blocks beneath it in place forever (a test that corrupted three
// stored hashes saw one reverted, two kept). It now scans the whole
// window and returns the DEEPEST mismatch: a real reorg mismatches from the
// head down to the fork point, and a stored block that disagrees with the
// chain beneath a matching head (a corrupted or partially reverted database)
// is reverted too rather than kept. The window was always scanned in full in
// the no-reorg case, so the cost per poll tick is unchanged.
//
// A read that fails part way returns the error with the depth found so far.
// The caller indexes nothing on that tick and walks again on the next one.
func (i *Indexer) detectReorg(ctx context.Context, blockNumber uint64) (uint64, error) {
	maxReorgCheck := uint64(10)
	var deepest uint64
	for depth := uint64(0); depth < maxReorgCheck && blockNumber > depth; depth++ {
		checkBlock := blockNumber - depth

		storedBlock, err := i.db.GetBlock(ctx, checkBlock)
		if err != nil {
			return deepest, err
		}
		if storedBlock == nil {
			continue
		}

		chainHash, err := i.rpc.RawBlockHash(ctx, checkBlock)
		if err != nil {
			return deepest, err
		}

		if storedBlock.Hash == chainHash {
			continue
		}
		storedHash := storedBlock.Hash
		chainHashLog := chainHash
		if len(storedHash) > 16 {
			storedHash = storedHash[:16]
		}
		if len(chainHashLog) > 16 {
			chainHashLog = chainHashLog[:16]
		}
		log.Warn("reorg detected",
			"block", checkBlock,
			"stored_hash", storedHash,
			"chain_hash", chainHashLog)
		deepest = depth + 1
	}
	if deepest > 0 {
		return deepest, nil
	}
	return 0, nil
}

// revertStaleHead reverts the stored blocks at top and below that no longer match the chain, with every stored
// block above them, ten blocks per check as detectReorg reads them, and returns the block the stored chain then
// ends at (top when nothing was stale). A stale fork deeper than maxReorgDepth, or one whose revert fails, refuses
// the start: starting on top of it would keep it stored for good.
func (i *Indexer) revertStaleHead(ctx context.Context, top uint64) (uint64, error) {
	head := top
	for head > 0 {
		depth, err := i.detectReorg(ctx, head)
		if err != nil {
			return head, fmt.Errorf("check the stored block %d against the chain: %w", head, err)
		}
		if depth == 0 {
			return head, nil
		}
		if top-head+depth > maxReorgDepth {
			return head, fmt.Errorf("the stored chain below block %d is on a stale fork deeper than %d blocks: "+
				"set FORCE_REINDEX=true to wipe and re-index", top, maxReorgDepth)
		}
		log.Warn("the stored head is on a stale fork; reverting it before starting", "block", head, "depth", depth)
		if err := i.handleReorg(ctx, head-depth+1); err != nil {
			return head, fmt.Errorf("the stored block %d is on a stale fork and reverting it failed: %w", head, err)
		}
		head -= depth
	}
	return head, nil
}

func (i *Indexer) handleReorg(ctx context.Context, fromBlock uint64) error {
	log.Info("reverting blocks due to reorg", "from_block", fromBlock)

	lastIndexed, err := i.db.GetLatestBlockNumber(ctx)
	if err != nil {
		return err
	}
	metrics.ReorgDetected()

	for blockNum := lastIndexed; blockNum >= fromBlock; blockNum-- {
		if err := i.db.DeleteBlock(ctx, blockNum); err != nil {
			log.Error("failed to delete block", "block", blockNum, "error", err)
			return err
		}
		log.Debug("reverted block", "block", blockNum)
	}

	return nil
}

func (i *Indexer) processBlock(ctx context.Context, number uint64) error {
	return i.processBlockRaw(ctx, number)
}

func (i *Indexer) processBlockRaw(ctx context.Context, number uint64) error {
	rawBlock, err := i.rpc.RawBlockByNumber(ctx, number)
	if err != nil {
		return err
	}

	if len(rawBlock.Transactions) > 0 {
		return i.processBlockParallelRaw(ctx, rawBlock)
	}
	return i.insertEmptyBlock(ctx, rawBlock)
}

func (i *Indexer) insertEmptyBlock(ctx context.Context, rawBlock *rpc.RawBlock) error {
	b := &types.Block{
		Number:           rawBlock.NumberU64(),
		Hash:             rawBlock.Hash.Hex(),
		ParentHash:       rawBlock.ParentHash.Hex(),
		Timestamp:        uint64(rawBlock.Timestamp),
		GasUsed:          uint64(rawBlock.GasUsed),
		GasLimit:         uint64(rawBlock.GasLimit),
		BaseFeePerGas:    rawBlock.BaseFeeU64(),
		TransactionCount: 0,
		Size:             uint64(rawBlock.Size),
		Difficulty:       rawBlock.DifficultyString(),
		TotalDifficulty:  rawBlock.TotalDifficultyString(),
		Nonce:            rawBlock.NonceHex(),
		Miner:            rawBlock.Miner.Hex(),
		ExtraData:        common.Bytes2Hex(rawBlock.ExtraData),
		StateRoot:        rawBlock.StateRoot.Hex(),
		TransactionsRoot: rawBlock.TransactionsRoot.Hex(),
		ReceiptsRoot:     rawBlock.ReceiptsRoot.Hex(),
	}

	if err := i.db.InsertBlock(ctx, b); err != nil {
		return err
	}
	metrics.BlockIndexed(0, uint64(rawBlock.GasUsed))
	i.dailyStats.markDirty(blockDay(b.Timestamp))
	i.scheduleDeferredDailyStats(ctx)
	return nil
}

// receiptTxHashes lists the transactions whose receipts are fetched, skipping tx types that are
// known to fail (e.g. type 126 OP deposit system transactions on op-reth devnets).
func (i *Indexer) receiptTxHashes(rawBlock *rpc.RawBlock) []common.Hash {
	txHashes := make([]common.Hash, 0, len(rawBlock.Transactions))
	for _, tx := range rawBlock.Transactions {
		if i.config.SkipReceiptTxTypes != nil && i.config.SkipReceiptTxTypes[int(tx.Type)] {
			continue
		}
		txHashes = append(txHashes, tx.Hash)
	}
	return txHashes
}

func (i *Indexer) processBlockParallelRaw(ctx context.Context, rawBlock *rpc.RawBlock) error {
	receipts, err := i.rpc.FetchReceiptsBatch(ctx, i.receiptTxHashes(rawBlock), i.config.RPCWorkers, i.config.RPCRateLimit, rawBlock.NumberU64())
	if err != nil {
		return fmt.Errorf("%w: %w", errReceiptsUnavailable, err)
	}
	return i.processBlockWithReceipts(ctx, rawBlock, receipts)
}

// processBlockWithReceipts is everything after the two chain reads: it builds the block's rows and
// commits them. The standard loop prefetches the reads (prefetch.go) and calls this in block order.
func (i *Indexer) processBlockWithReceipts(ctx context.Context, rawBlock *rpc.RawBlock, receipts map[common.Hash]*rpclient.Receipt) error {
	defer metrics.StageTimer(metrics.StageBlockTotal)()
	b := i.buildBlock(ctx, rawBlock, receipts)
	commitStart := time.Now()
	if err := i.db.InsertBlockDataBatch(ctx, b.data); err != nil {
		return err
	}
	metrics.ObserveStage(metrics.StageDBCommit, time.Since(commitStart))
	i.afterCommit(ctx, b)
	return nil
}

// builtBlock is a block turned into rows, with what has to happen once those rows are committed.
type builtBlock struct {
	raw            *rpc.RawBlock
	data           *db.BlockData
	balanceWork    []BalanceWork
	discovered     []string
	start          time.Time
	blockNumber    uint64
	blockTimestamp uint64
}

func (i *Indexer) buildBlock(ctx context.Context, rawBlock *rpc.RawBlock, receipts map[common.Hash]*rpclient.Receipt) *builtBlock {
	start := time.Now()
	blockNumber := rawBlock.NumberU64()
	rawTxs := rawBlock.Transactions
	blockTimestamp := uint64(rawBlock.Timestamp)
	txHashes := i.receiptTxHashes(rawBlock)

	blockData := &db.BlockData{
		Block: &types.Block{
			Number:           blockNumber,
			Hash:             rawBlock.Hash.Hex(),
			ParentHash:       rawBlock.ParentHash.Hex(),
			Timestamp:        blockTimestamp,
			GasUsed:          uint64(rawBlock.GasUsed),
			GasLimit:         uint64(rawBlock.GasLimit),
			BaseFeePerGas:    rawBlock.BaseFeeU64(),
			TransactionCount: len(rawTxs),
			Size:             uint64(rawBlock.Size),
			Difficulty:       rawBlock.DifficultyString(),
			TotalDifficulty:  rawBlock.TotalDifficultyString(),
			Nonce:            rawBlock.NonceHex(),
			Miner:            rawBlock.Miner.Hex(),
			ExtraData:        common.Bytes2Hex(rawBlock.ExtraData),
			StateRoot:        rawBlock.StateRoot.Hex(),
			TransactionsRoot: rawBlock.TransactionsRoot.Hex(),
			ReceiptsRoot:     rawBlock.ReceiptsRoot.Hex(),
		},
		Transactions:         make([]*types.Transaction, 0, len(rawTxs)),
		Logs:                 make([]*types.Log, 0),
		Transfers:            make([]*types.TokenTransfer, 0),
		Contracts:            make([]*types.Contract, 0),
		Tokens:               make([]*types.Token, 0),
		InternalTransactions: make([]*types.InternalTransaction, 0),
		AddressStats:         make(map[string]*db.AddressStatsDelta),
		SkipAddressStats:     i.config.SkipAddressStats,
	}

	newTokenAddresses := make([]common.Address, 0)
	newTokenTxHashes := make(map[common.Address]string)
	addressTxs := make(map[string]map[int]struct{})
	balanceWork := make([]BalanceWork, 0)

	// NFT instances touched this block, and (for mints) the tokenURI fetches to
	// run after the log loop. nftURIRowIdx[k] indexes into blockData.NFTTokens.
	var nftURIReqs []rpc.NFTURIRequest
	var nftURIRowIdx []int

	for idx, rawTx := range rawTxs {
		receipt := receipts[rawTx.Hash]

		from := rawTx.From.Hex()

		var to *string
		if rawTx.To != nil {
			toStr := rawTx.To.Hex()
			to = &toStr
		}

		inputData := ""
		if len(rawTx.Input) > 0 {
			inputData = common.Bytes2Hex(rawTx.Input)
		}

		var nonce uint64
		if rawTx.Nonce != nil {
			nonce = uint64(*rawTx.Nonce)
		}

		gasLimit := uint64(rawTx.Gas)
		txType := int(rawTx.Type)

		var gasPrice uint64
		if rawTx.GasPrice != nil {
			gasPrice = rawTx.GasPrice.ToInt().Uint64()
		}

		value := "0"
		if rawTx.Value != nil {
			value = rawTx.Value.ToInt().String()
		}

		var maxFeePerGas, maxPriorityFeePerGas *uint64
		if rawTx.MaxFeePerGas != nil {
			mfpg := rawTx.MaxFeePerGas.ToInt().Uint64()
			maxFeePerGas = &mfpg
		}
		if rawTx.MaxPriorityFeePerGas != nil {
			mpfpg := rawTx.MaxPriorityFeePerGas.ToInt().Uint64()
			maxPriorityFeePerGas = &mpfpg
		}

		var gasUsed uint64
		var status int
		var txError *string
		if receipt != nil {
			gasUsed = receipt.GasUsed
			status = int(receipt.Status)
			if receipt.Status == 0 {
				errMsg := "transaction reverted"
				txError = &errMsg
			}
		} else {
			gasUsed = gasLimit
			status = 1
		}

		blockData.Transactions = append(blockData.Transactions, &types.Transaction{
			Hash:                 rawTx.Hash.Hex(),
			BlockNumber:          blockNumber,
			TxIndex:              idx,
			From:                 from,
			To:                   to,
			Value:                types.JSONString(value),
			GasUsed:              gasUsed,
			GasPrice:             gasPrice,
			GasLimit:             &gasLimit,
			MaxFeePerGas:         maxFeePerGas,
			MaxPriorityFeePerGas: maxPriorityFeePerGas,
			Nonce:                &nonce,
			TxType:               txType,
			InputData:            inputData,
			Status:               status,
			Error:                txError,
		})

		touchAddressTx(blockData.AddressStats, addressTxs, from, idx, blockNumber)

		if to != nil {
			toDelta := touchAddressTx(blockData.AddressStats, addressTxs, *to, idx, blockNumber)
			toDelta.IsContract = toDelta.IsContract || i.contractCache.Has(*to)
		}

		if receipt != nil && rawTx.To == nil && receipt.ContractAddress != (common.Address{}) {
			contractAddr := receipt.ContractAddress.Hex()
			// Set the contract address on the transaction so it shows in the UI
			blockData.Transactions[len(blockData.Transactions)-1].ContractAddress = &contractAddr
			code, err := i.rpc.GetCode(ctx, receipt.ContractAddress)
			if err == nil && len(code) > 0 {
				bytecodeHash := common.BytesToHash(common.FromHex(common.Bytes2Hex(code))).Hex()
				blockData.Contracts = append(blockData.Contracts, &types.Contract{
					Address:      contractAddr,
					Bytecode:     common.Bytes2Hex(code),
					BytecodeHash: &bytecodeHash,
					Creator:      from,
					CreationTx:   rawTx.Hash.Hex(),
					BlockNumber:  blockNumber,
					IsVerified:   false,
				})
				i.contractCache.Add(contractAddr)
				addressDelta(blockData.AddressStats, contractAddr, blockNumber).IsContract = true
			}
		}

		if receipt == nil {
			continue
		}
		for _, logEntry := range receipt.Logs {
			var topic0, topic1, topic2, topic3 *string
			if len(logEntry.Topics) > 0 {
				t := logEntry.Topics[0].Hex()
				topic0 = &t
			}
			if len(logEntry.Topics) > 1 {
				t := logEntry.Topics[1].Hex()
				topic1 = &t
			}
			if len(logEntry.Topics) > 2 {
				t := logEntry.Topics[2].Hex()
				topic2 = &t
			}
			if len(logEntry.Topics) > 3 {
				t := logEntry.Topics[3].Hex()
				topic3 = &t
			}

			blockData.Logs = append(blockData.Logs, &types.Log{
				TxHash:      rawTx.Hash.Hex(),
				LogIndex:    int(logEntry.Index),
				Address:     logEntry.Address.Hex(),
				Topic0:      topic0,
				Topic1:      topic1,
				Topic2:      topic2,
				Topic3:      topic3,
				Data:        common.Bytes2Hex(logEntry.Data),
				BlockNumber: blockNumber,
				Timestamp:   &blockTimestamp,
				Removed:     logEntry.Removed,
			})

			if len(logEntry.Topics) >= 3 && logEntry.Topics[0] == transferTopic {
				fromAddr := common.BytesToAddress(logEntry.Topics[1].Bytes())
				toAddr := common.BytesToAddress(logEntry.Topics[2].Bytes())

				transferType := types.TransferTypeTransfer
				if fromAddr == zeroAddress {
					transferType = types.TransferTypeMint
				} else if toAddr == zeroAddress {
					transferType = types.TransferTypeBurn
				}

				tokenType := types.TokenTypeERC20
				var tokenID *string
				if len(logEntry.Topics) == 4 {
					tokenType = types.TokenTypeERC721
					tid := logEntry.Topics[3].Big().String()
					tokenID = &tid
				}

				transferValue := "0"
				if tokenType == types.TokenTypeERC20 && len(logEntry.Data) > 0 {
					transferValue = new(big.Int).SetBytes(logEntry.Data).String()
				} else if tokenType == types.TokenTypeERC721 {
					transferValue = "1"
				}

				blockData.Transfers = append(blockData.Transfers, &types.TokenTransfer{
					TxHash:       rawTx.Hash.Hex(),
					LogIndex:     int(logEntry.Index),
					TokenAddress: logEntry.Address.Hex(),
					From:         fromAddr.Hex(),
					To:           toAddr.Hex(),
					Value:        types.JSONString(transferValue),
					BlockNumber:  blockNumber,
					Timestamp:    &blockTimestamp,
					TransferType: transferType,
					TokenType:    tokenType,
					TokenID:      tokenID,
					IsInternal:   false,
				})

				// Track the per-instance owner for the inventory view. Owner
				// advances to the latest transfer's `to` (zero = burned); the
				// tokenURI is fetched once, at mint.
				if tokenType == types.TokenTypeERC721 && tokenID != nil {
					blockData.NFTTokens = append(blockData.NFTTokens, &types.NFTToken{
						TokenAddress: logEntry.Address.Hex(),
						TokenID:      *tokenID,
						Owner:        toAddr.Hex(),
						BlockNumber:  blockNumber,
					})
					if transferType == types.TransferTypeMint {
						nftURIReqs = append(nftURIReqs, rpc.NFTURIRequest{
							Address: logEntry.Address,
							TokenID: logEntry.Topics[3].Big(),
						})
						nftURIRowIdx = append(nftURIRowIdx, len(blockData.NFTTokens)-1)
					}
				}

				if fromAddr != zeroAddress {
					balanceWork = append(balanceWork, BalanceWork{
						Address:      fromAddr,
						TokenAddress: logEntry.Address,
						BlockNumber:  blockNumber,
					})
				}
				if toAddr != zeroAddress {
					balanceWork = append(balanceWork, BalanceWork{
						Address:      toAddr,
						TokenAddress: logEntry.Address,
						BlockNumber:  blockNumber,
					})
				}

				if !i.tokenCache.Has(logEntry.Address.Hex()) {
					if _, exists := newTokenTxHashes[logEntry.Address]; !exists {
						newTokenAddresses = append(newTokenAddresses, logEntry.Address)
						newTokenTxHashes[logEntry.Address] = rawTx.Hash.Hex()
					}
				}

				// Token-transfer participation tracks the Transfer EVENT's
				// from/to (parsed from log topics), NOT the transaction's
				// from/to. The tx sender is the caller of the contract; the
				// actual holders being credited / debited live in the event.
				// Pre-bug, we incremented the txn's from/to here, which both
				// over-counted the contract address and missed every transfer
				// recipient who never sent a tx themselves.
				countTransferParticipant(blockData.AddressStats, addressTxs, fromAddr, idx, blockNumber)
				if toAddr != fromAddr {
					countTransferParticipant(blockData.AddressStats, addressTxs, toAddr, idx, blockNumber)
				}
			}
		}
	}

	for addr, txs := range addressTxs {
		blockData.AddressStats[addr].TxCountDelta += len(txs)
	}

	if i.tracingSupported && len(txHashes) > 0 {
		internalTxs, err := i.rpc.FetchTracesBatch(ctx, txHashes, blockNumber, blockTimestamp,
			i.config.TraceWorkers, i.config.TraceRateLimit)
		if err != nil {
			log.Warn("failed to fetch traces for block", "block", blockNumber, "error", err)
		} else if len(internalTxs) > 0 {
			blockData.InternalTransactions = internalTxs

			for _, it := range internalTxs {
				addressDelta(blockData.AddressStats, it.From, blockNumber).InternalTxCountDelta++
				if it.To != nil && !strings.EqualFold(*it.To, it.From) {
					addressDelta(blockData.AddressStats, *it.To, blockNumber).InternalTxCountDelta++
				}
			}
			for _, it := range internalCreations(internalTxs) {
				c := contractFromCreation(ctx, i.rpc.GetCode, it, blockNumber)
				if c == nil {
					continue
				}
				blockData.Contracts = append(blockData.Contracts, c)
				i.contractCache.Add(c.Address)
				addressDelta(blockData.AddressStats, c.Address, blockNumber).IsContract = true
			}
		}
	}

	// Addresses to mark as known, but only once their tokens row is committed --
	// see the Add below the batch.
	var discovered []string

	if len(newTokenAddresses) > 0 {
		// A token is only ever discovered from a Transfer event we just parsed,
		// and that path already classifies ERC721 by its 4-topic signature (the
		// tokenId is carried as an indexed topic). Carry that signal over to the
		// token entity instead of defaulting every new contract to ERC20.
		erc721Tokens := make(map[string]bool)
		for _, tr := range blockData.Transfers {
			if tr.TokenType == types.TokenTypeERC721 {
				erc721Tokens[tr.TokenAddress] = true
			}
		}

		tokenMetadata, err := i.rpc.FetchTokenMetadataBatch(ctx, newTokenAddresses, i.config.TokenMetadataWorkers, i.config.RPCRateLimit)
		if err != nil {
			log.Warn("failed to fetch token metadata batch", "error", err)
		} else {
			for addr, meta := range tokenMetadata {
				if meta.Err != nil {
					continue
				}
				txHash := newTokenTxHashes[addr]

				tokenType, decimals := classifyNewToken(erc721Tokens[addr.Hex()], meta.Decimals)

				blockData.Tokens = append(blockData.Tokens, &types.Token{
					Address:     addr.Hex(),
					Symbol:      meta.Symbol,
					Name:        meta.Name,
					Decimals:    decimals,
					TokenType:   tokenType,
					BlockNumber: blockNumber,
					CreationTx:  &txHash,
				})
				discovered = append(discovered, addr.Hex())
			}
		}
	}

	// Resolve tokenURI for freshly minted NFTs and attach to their rows.
	if len(nftURIReqs) > 0 {
		uris := i.rpc.FetchTokenURIsBatch(ctx, nftURIReqs, i.config.TokenMetadataWorkers, i.config.RPCRateLimit)
		for k, rowIdx := range nftURIRowIdx {
			if uri, ok := uris[k]; ok {
				u := uri
				blockData.NFTTokens[rowIdx].TokenURI = &u
			}
		}
	}

	return &builtBlock{raw: rawBlock, data: blockData, balanceWork: balanceWork, discovered: discovered,
		start: start, blockNumber: blockNumber, blockTimestamp: blockTimestamp}
}

func (i *Indexer) afterCommit(ctx context.Context, b *builtBlock) {
	rawBlock, blockData, balanceWork, discovered := b.raw, b.data, b.balanceWork, b.discovered
	start, blockNumber, blockTimestamp, rawTxs := b.start, b.blockNumber, b.blockTimestamp, b.raw.Transactions

	// Marked known only now, after the row is committed. Adding before the
	// batch made the cache claim a token whose row was still uncommitted (or
	// whose batch went on to fail), and another worker parsing a transfer of
	// that token in the window would then omit it from its own data.Tokens --
	// so it neither seeded the row nor was covered by the seed, and its delta
	// UPDATE found no visible row and silently counted nothing.
	//
	// With the Add here, that worker still sees a cache miss, still queues the
	// token, and blocks on the tokens primary key until the first worker
	// commits. It then reads wasNew = false against a visible row and its own
	// delta applies. The serialisation point is a lock the insert already
	// takes; the cost is one duplicate metadata fetch in that window.
	for _, addr := range discovered {
		i.tokenCache.Add(addr)
	}

	// Refresh the stats that are still recomputed for each token touched in
	// this block -- only ERC721 total_supply is left. transfer_count and
	// total_supply (ERC20) are maintained by the delta writes inside
	// InsertBlockDataBatch above; holder_count by InsertBalancesBatch when the
	// balances land (PRST-4669). For an ERC20 token this is one PK read.
	//
	// Runs synchronously so the new count is visible on the next read.
	if len(blockData.Transfers) > 0 {
		touched := make(map[string]struct{}, len(blockData.Transfers))
		for _, t := range blockData.Transfers {
			touched[strings.ToLower(t.TokenAddress)] = struct{}{}
		}
		for tokenAddr := range touched {
			refreshStart := time.Now()
			if err := i.db.RefreshTokenStats(ctx, tokenAddr); err != nil {
				log.Warn("refresh token stats failed", "token", tokenAddr, "error", err)
			}
			metrics.ObserveStage(metrics.StageRefreshTokenStats, time.Since(refreshStart))
		}
	}

	if i.balanceWorkers != nil && len(balanceWork) > 0 {
		queueStart := time.Now()
		queued := i.balanceWorkers.QueueCommittedWork(ctx, balanceWork)
		metrics.ObserveStage(metrics.StageBalanceQueueWait, time.Since(queueStart))
		if queued < len(balanceWork) {
			metrics.BalanceQueueDropped(len(balanceWork) - queued)
			log.Warn("balance work dropped: enqueue cancelled by shutdown", "dropped", len(balanceWork)-queued)
		}
		metrics.SetQueueDepth(metrics.QueueBalance, i.balanceWorkers.QueueSize())
	}

	if i.eventBus != nil {
		i.eventBus.PublishNewBlock(blockData.Block)
		for _, tx := range blockData.Transactions {
			i.eventBus.PublishNewTransaction(tx)
		}
	}

	metrics.BlockIndexed(len(rawTxs), uint64(rawBlock.GasUsed))

	elapsed := time.Since(start)
	if blockNumber%100 == 0 || elapsed > 2*time.Second {
		log.Info("indexed block (raw)",
			"block", blockNumber,
			"txs", len(rawTxs),
			"transfers", len(blockData.Transfers),
			"new_tokens", len(blockData.Tokens),
			"elapsed", elapsed)
	}

	// Advance daily_stats, throttled. The row is a dashboard aggregate: a day's
	// row may lag its blocks by up to one interval plus one run.
	i.scheduleDailyStats(ctx, blockNumber, blockDay(blockTimestamp))
}

func blockDay(timestamp uint64) time.Time {
	return time.Unix(int64(timestamp), 0).UTC().Truncate(24 * time.Hour)
}

func (i *Indexer) scheduleDailyStats(ctx context.Context, blockNumber uint64, day time.Time) {
	if ctx.Err() != nil {
		return
	}
	days := i.dailyStats.due(blockNumber, day, i.config.DailyStatsMinInterval)
	if days == nil {
		i.scheduleDeferredDailyStats(ctx)
		return
	}
	go func() {
		defer i.scheduleDeferredDailyStats(ctx)
		defer i.dailyStats.done()
		if !i.advanceDailyStats(ctx) {
			for _, d := range days {
				i.dailyStats.markDirty(d)
			}
		}
	}()
}

func (i *Indexer) scheduleDeferredDailyStats(ctx context.Context) {
	if wait, ok := i.dailyStats.arm(i.config.DailyStatsMinInterval); ok {
		time.AfterFunc(wait, func() {
			i.dailyStats.disarm()
			i.scheduleDailyStats(ctx, 0, time.Time{})
		})
	}
}

func addressDelta(stats map[string]*db.AddressStatsDelta, address string, blockNumber uint64) *db.AddressStatsDelta {
	key := strings.ToLower(address)
	delta, ok := stats[key]
	if !ok {
		delta = &db.AddressStatsDelta{Address: key, BlockNumber: blockNumber}
		stats[key] = delta
	}
	return delta
}

func touchAddressTx(stats map[string]*db.AddressStatsDelta, addressTxs map[string]map[int]struct{}, address string, txIndex int, blockNumber uint64) *db.AddressStatsDelta {
	delta := addressDelta(stats, address, blockNumber)
	txs := addressTxs[delta.Address]
	if txs == nil {
		txs = make(map[int]struct{})
		addressTxs[delta.Address] = txs
	}
	txs[txIndex] = struct{}{}
	return delta
}

func countTransferParticipant(stats map[string]*db.AddressStatsDelta, addressTxs map[string]map[int]struct{}, addr common.Address, txIndex int, blockNumber uint64) {
	if addr == zeroAddress {
		return
	}
	touchAddressTx(stats, addressTxs, addr.Hex(), txIndex, blockNumber).TokenTransferDelta++
}

func (i *Indexer) advanceDailyStats(ctx context.Context) bool {
	if _, err := i.db.AdvanceDailyStats(ctx); err != nil {
		log.Warn("failed to advance daily stats; will retry", "error", err)
		return false
	}
	return true
}

// classifyNewToken decides the stored token_type and decimals for a freshly
// discovered contract. ERC721 is inferred from the transfer topology already
// parsed for this block (a 4-topic Transfer carries the tokenId as an indexed
// topic). NFTs do not implement decimals(), so the metadata layer's fallback
// of 18 is meaningless for them and is replaced with 0.
func classifyNewToken(isERC721 bool, metaDecimals int) (tokenType string, decimals int) {
	if isERC721 {
		return types.TokenTypeERC721, 0
	}
	return types.TokenTypeERC20, metaDecimals
}

func (i *Indexer) trackBalance(ctx context.Context, addr common.Address, tokenAddress common.Address, blockNumber uint64) {
	if addr == zeroAddress {
		return
	}

	if i.balanceWorkers != nil {
		i.balanceWorkers.QueueWork(BalanceWork{
			Address:      addr,
			TokenAddress: tokenAddress,
			BlockNumber:  blockNumber,
		})
		return
	}

	addrPadded := common.LeftPadBytes(addr.Bytes(), 32)
	callData := append(common.FromHex("0x70a08231"), addrPadded...)

	balanceData, err := i.rpc.CallContract(ctx, tokenAddress, callData)
	if err != nil {
		return
	}

	if len(balanceData) < 32 {
		return
	}

	balance := new(big.Int).SetBytes(balanceData)

	balanceRecord := &types.Balance{
		Address:      addr.Hex(),
		TokenAddress: tokenAddress.Hex(),
		BlockNumber:  blockNumber,
		Balance:      types.JSONString(balance.String()),
	}

	if err := i.db.InsertBalance(ctx, balanceRecord); err != nil {
		log.Error("failed to insert balance", "address", addr.Hex(), "error", err)
	}
}

func (i *Indexer) maybeIndexToken(ctx context.Context, tokenAddress common.Address, blockNumber uint64, txHash string) {
	if i.tokenCache.Has(tokenAddress.Hex()) {
		return
	}

	existing, err := i.db.GetToken(ctx, tokenAddress.Hex())
	if err == nil && existing != nil {
		i.tokenCache.Add(tokenAddress.Hex())
		return
	}

	symbol, name, decimals, err := i.fetchTokenMetadata(ctx, tokenAddress)
	if err != nil {
		return
	}

	token := &types.Token{
		Address:     tokenAddress.Hex(),
		Symbol:      symbol,
		Name:        name,
		Decimals:    decimals,
		TokenType:   types.TokenTypeERC20,
		BlockNumber: blockNumber,
		CreationTx:  &txHash,
	}

	if err := i.db.InsertToken(ctx, token); err != nil {
		log.Error("failed to insert token", "address", tokenAddress.Hex(), "error", err)
	} else {
		i.tokenCache.Add(tokenAddress.Hex())
		log.Info("indexed token", "symbol", symbol, "address", tokenAddress.Hex())
	}
}

func (i *Indexer) fetchTokenMetadata(ctx context.Context, tokenAddress common.Address) (symbol string, name *string, decimals int, err error) {
	symbolData, err := i.rpc.CallContract(ctx, tokenAddress, common.FromHex("0x95d89b41"))
	if err != nil {
		return "", nil, 0, err
	}
	symbol = parseStringResult(symbolData)
	if symbol == "" {
		symbol = "UNKNOWN"
	}

	nameData, err := i.rpc.CallContract(ctx, tokenAddress, common.FromHex("0x06fdde03"))
	if err == nil {
		n := parseStringResult(nameData)
		if n != "" {
			name = &n
		}
	}

	decimalsData, err := i.rpc.CallContract(ctx, tokenAddress, common.FromHex("0x313ce567"))
	if err == nil && len(decimalsData) >= 32 {
		decimals = int(new(big.Int).SetBytes(decimalsData).Int64())
	} else {
		decimals = 18 // Default to 18 decimals
	}

	return symbol, name, decimals, nil
}

func parseStringResult(data []byte) string {
	if len(data) < 64 {
		return strings.TrimRight(string(data), "\x00")
	}

	offset := new(big.Int).SetBytes(data[:32]).Uint64()
	if offset >= uint64(len(data)) {
		return ""
	}

	if offset+32 > uint64(len(data)) {
		return ""
	}

	length := new(big.Int).SetBytes(data[offset : offset+32]).Uint64()
	if offset+32+length > uint64(len(data)) {
		return ""
	}

	return strings.TrimRight(string(data[offset+32:offset+32+length]), "\x00")
}
