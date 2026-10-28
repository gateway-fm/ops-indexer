package indexer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/events"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

type CatchupConfig struct {
	Workers   int // Number of parallel block processing workers
	BatchSize int // Number of blocks to fetch in each batch
	QueueSize int // Size of the work queue
}

type CatchupIndexer struct {
	db       Database
	rpc      RPCClient
	config   *CatchupConfig
	idxCfg   *Config // Main indexer config for RPC settings
	eventBus *events.Bus

	// Missing range collector (shared with main indexer)
	collector *MissingRangeCollector

	// realtimeBehind, when set, is how far realtime is behind the node; catch-up yields to it (yieldToRealtime).
	realtimeBehind func() uint64

	// Caches (shared with main indexer)
	tokenCache    *TokenCache
	contractCache *ContractCache

	// Balance workers (shared with main indexer)
	balanceWorkers *BalanceWorkerPool

	// Tracing support
	tracingSupported bool

	// Shared with the main indexer (see Indexer.dailyStats); nil = no daily
	// stats from the catchup path, which is what a stale aggregate during a
	// backfill deserves anyway.
	dailyStats *dailyStatsThrottle

	// Work queue
	workQueue chan uint64

	// Progress tracking
	processedBlocks int64
	totalMissing    int64
	lastLogTime     time.Time

	// Coordination
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// Completion callback
	onComplete func()

	pollInterval time.Duration
}

func NewCatchupIndexer(
	database Database,
	rpcClient RPCClient,
	cfg *CatchupConfig,
	idxCfg *Config,
	tokenCache *TokenCache,
	contractCache *ContractCache,
	balanceWorkers *BalanceWorkerPool,
	tracingSupported bool,
) *CatchupIndexer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CatchupIndexer{
		db:               database,
		rpc:              rpcClient,
		config:           cfg,
		idxCfg:           idxCfg,
		tokenCache:       tokenCache,
		contractCache:    contractCache,
		balanceWorkers:   balanceWorkers,
		tracingSupported: tracingSupported,
		workQueue:        make(chan uint64, cfg.QueueSize),
		ctx:              ctx,
		cancel:           cancel,
		lastLogTime:      time.Now(),
		pollInterval:     5 * time.Second,
	}
}

// SetDailyStatsThrottle shares the main indexer's day-aggregate throttle with
// the per-block Indexer values this catchup builds. Must be called before Start.
func (c *CatchupIndexer) SetDailyStatsThrottle(t *dailyStatsThrottle) {
	c.dailyStats = t
}

func (c *CatchupIndexer) SetEventBus(bus *events.Bus) {
	c.eventBus = bus
}

func (c *CatchupIndexer) SetOnComplete(fn func()) {
	c.onComplete = fn
}

func (c *CatchupIndexer) SetCollector(collector *MissingRangeCollector) {
	c.collector = collector
}

// Start runs continuously, polling for missing ranges from the collector.
// It processes existing gaps and continues monitoring for new gaps
// that appear when the chain head advances.
// SetRealtimeBehind makes catch-up yield to realtime: see yieldToRealtime.
func (c *CatchupIndexer) SetRealtimeBehind(behind func() uint64) {
	c.realtimeBehind = behind
}

// Catch-up pauses while realtime is more than catchupYieldAbove blocks behind the node, until it is within
// catchupResumeBelow.
const (
	catchupYieldAbove  = 64
	catchupResumeBelow = 16
)

// yieldToRealtime holds a catch-up worker while realtime is behind. Catch-up's workers otherwise take every pool
// connection: in a load test after a 20-min stall they committed ~45 blocks/s while realtime managed 1.6, so the head
// fell behind again and realtime jumped three more times. Realtime alone is the head; catch-up gets the rest.
func (c *CatchupIndexer) yieldToRealtime(id int) {
	if c.realtimeBehind == nil || c.realtimeBehind() <= catchupYieldAbove {
		return
	}
	log.Debug("catchup: yielding to realtime", "worker", id, "realtime_behind", c.realtimeBehind())
	for c.realtimeBehind() > catchupResumeBelow {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (c *CatchupIndexer) Start(ctx context.Context, fromBlock, toBlock uint64) error {
	if c.collector != nil {
		total, _ := c.collector.GetTotalMissingBlocks(ctx)
		c.totalMissing = total
	} else {
		c.totalMissing = int64(toBlock - fromBlock) // This is okay as long as delta < int64_max, but safer to check or use uint64 if possible. For now, matching similar casts.
	}

	log.Info("catchup: starting (continuous mode)",
		"initial_missing", c.totalMissing,
		"workers", c.config.Workers)

	for i := 0; i < c.config.Workers; i++ {
		c.wg.Add(1)
		go c.worker(i)
	}

	c.wg.Add(1)
	go c.blockProducerFromRanges()

	return nil
}

func (c *CatchupIndexer) blockProducerFromRanges() {
	defer c.wg.Done()
	defer close(c.workQueue)

	batchSize := c.config.BatchSize
	if batchSize == 0 {
		batchSize = 100
	}

	idleCount := 0
	completionCalled := false
	pollInterval := c.pollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		var ranges []db.BlockRange
		var err error

		if c.collector != nil {
			ranges, err = c.collector.GetMissingRangesBatch(c.ctx, batchSize)
		} else {
			ranges, err = c.db.GetMissingRangesBatch(c.ctx, batchSize)
		}

		if err != nil {
			log.Error("catchup: failed to get missing ranges", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(ranges) == 0 {
			// No missing ranges right now - but keep polling!
			// New ranges may be added when chain head advances
			idleCount++

			// Call completion callback once after being idle for a while
			if !completionCalled && idleCount >= 3 {
				processed := atomic.LoadInt64(&c.processedBlocks)
				if processed > 0 {
					log.Info("catchup: initial sync complete, continuing to monitor for new gaps",
						"processed", processed)

					if c.onComplete != nil {
						c.onComplete()
					}
				}
				completionCalled = true
			}

			// Log periodically when idle
			if idleCount%12 == 1 { // Every minute (12 * 5s)
				log.Debug("catchup: waiting for new missing ranges")
			}

			// Wait before polling again
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}

		if idleCount > 0 {
			log.Info("catchup: new missing ranges detected, resuming",
				"ranges", len(ranges))
		}
		idleCount = 0

		for _, r := range ranges {
			for blockNum := r.FromNumber; blockNum <= r.ToNumber; blockNum++ {
				select {
				case <-c.ctx.Done():
					return
				case c.workQueue <- blockNum:
				}
			}
		}
	}
}

func (c *CatchupIndexer) Stop() {
	c.cancel()
	c.wg.Wait()
}

func (c *CatchupIndexer) QueueDepth() int {
	return len(c.workQueue)
}

func (c *CatchupIndexer) Progress() (processed int64, total uint64, percentComplete float64) {
	processed = atomic.LoadInt64(&c.processedBlocks)

	if c.collector != nil {
		remaining, _ := c.collector.GetTotalMissingBlocks(c.ctx)
		total = uint64(processed) + uint64(remaining)
	} else {
		total = uint64(c.totalMissing)
	}

	if total > 0 {
		percentComplete = float64(processed) / float64(total) * 100
	}
	return
}

func (c *CatchupIndexer) IsRunning() bool {
	select {
	case <-c.ctx.Done():
		return false
	default:
		return true
	}
}

// catchupRetryWait is the first wait before a dequeued block is tried again; each failure doubles it, up to
// catchupRetryWaitMax. After catchupAttempts tries in memory the block is only recorded as missing again, which
// catch-up's next poll picks up.
var catchupRetryWait = 100 * time.Millisecond

const (
	catchupRetryWaitMax = 5 * time.Second
	catchupAttempts     = 5
)

func (c *CatchupIndexer) worker(id int) {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case blockNum, ok := <-c.workQueue:
			if !ok {
				return
			}
			// Every dequeue, not just the successful ones: the paths below that
			// skip a block still drain the queue.
			metrics.SetQueueDepth(metrics.QueueCatchup, len(c.workQueue))

			c.yieldToRealtime(id)
			c.indexBlock(id, blockNum)
		}
	}
}

// indexBlock stores a dequeued block, or records it as missing again, and does not return before one of the two has
// happened (or the indexer stops). Its range row is already gone and the collector's scans do not come back to it, so
// a block dropped here would stay missing until the next start. An existence check or a write that fails is tried
// again in memory; after catchupAttempts tries the block is only recorded as missing, retried until that succeeds.
func (c *CatchupIndexer) indexBlock(id int, blockNum uint64) {
	wait := catchupRetryWait
	for attempt := 1; ; attempt++ {
		if attempt > catchupAttempts {
			err := c.db.RequeueMissingBlock(c.ctx, blockNum)
			if err == nil {
				return
			}
			log.Error("catchup: failed to record block as missing; retrying", "block", blockNum, "error", err)
		} else if c.tryBlock(id, blockNum, attempt) {
			return
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(2*wait, catchupRetryWaitMax)
	}
}

// tryBlock checks, writes and accounts for one block once, and reports whether the block is done with: stored (by
// this worker or another writer), or recorded as missing again after a failed write.
func (c *CatchupIndexer) tryBlock(id int, blockNum uint64, attempt int) bool {
	// Block may have been indexed by another process in the meantime
	exists, err := c.db.HasBlock(c.ctx, blockNum)
	if err != nil {
		log.Error("catchup: failed to check block existence; retrying", "block", blockNum, "attempt", attempt, "error", err)
		return false
	}
	if exists {
		if c.collector != nil {
			c.collector.MarkBlockProcessed(c.ctx, blockNum)
		}
		atomic.AddInt64(&c.processedBlocks, 1)
		return true
	}

	if err := c.processBlock(blockNum); err != nil {
		log.Error("catchup: failed to process block", "worker", id, "block", blockNum, "error", err)
		// Re-insert into missing_block_ranges so the block is retried on next
		// poll. Without this, the block is permanently lost: the parent range
		// is already deleted from missing_block_ranges when the first sibling
		// block succeeds, so a failed block would never be retried.
		if requeueErr := c.db.RequeueMissingBlock(c.ctx, blockNum); requeueErr != nil {
			log.Error("catchup: failed to requeue block after error; retrying", "block", blockNum, "error", requeueErr)
			return false
		}
		return true
	}

	if c.collector != nil {
		c.collector.MarkBlockProcessed(c.ctx, blockNum)
	} else {
		c.db.DeleteMissingRangeByBlock(c.ctx, blockNum)
	}

	processed := atomic.AddInt64(&c.processedBlocks, 1)

	if time.Since(c.lastLogTime) > 5*time.Second {
		c.lastLogTime = time.Now()

		var remaining int64
		if c.collector != nil {
			remaining, _ = c.collector.GetTotalMissingBlocks(c.ctx)
		} else {
			remaining, _ = c.db.GetTotalMissingBlocks(c.ctx)
		}

		log.Info("catchup: queue drain",
			"blocks_processed_since_start", processed,
			"queue_remaining", remaining)
	}
	return true
}

func (c *CatchupIndexer) processBlock(number uint64) error {
	return c.processBlockRaw(number)
}

func (c *CatchupIndexer) processBlockRaw(number uint64) error {
	rawBlock, err := c.rpc.RawBlockByNumber(c.ctx, number)
	if err != nil {
		return err
	}

	idx := c.blockIndexer()
	if len(rawBlock.Transactions) > 0 {
		return idx.processBlockParallelRaw(c.ctx, rawBlock)
	}

	return idx.processBlockRaw(c.ctx, number)
}

func (c *CatchupIndexer) blockIndexer() *Indexer {
	catchupConfig := *c.idxCfg

	return &Indexer{
		db:               c.db,
		rpc:              c.rpc,
		config:           &catchupConfig,
		tokenCache:       c.tokenCache,
		contractCache:    c.contractCache,
		balanceWorkers:   c.balanceWorkers,
		tracingSupported: c.tracingSupported,
		eventBus:         c.eventBus,
		dailyStats:       c.dailyStats,
	}
}
