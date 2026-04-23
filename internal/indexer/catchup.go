package indexer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/events"
	"github.com/gateway-fm/chain-indexer/internal/log"
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

	// Caches (shared with main indexer)
	tokenCache    *TokenCache
	contractCache *ContractCache

	// Balance workers (shared with main indexer)
	balanceWorkers *BalanceWorkerPool

	// Tracing support
	tracingSupported bool

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
	}
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
	pollInterval := 5 * time.Second // Poll every 5 seconds when idle

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
			// This triggers address_stats rebuild but doesn't stop catchup
			if !completionCalled && idleCount >= 3 {
				processed := atomic.LoadInt64(&c.processedBlocks)
				if processed > 0 {
					log.Info("catchup: initial sync complete, continuing to monitor for new gaps",
						"processed", processed)

					// Rebuild address stats after initial catchup
					log.Info("catchup: rebuilding address_stats table")
					if err := c.db.RebuildAddressStats(context.Background()); err != nil {
						log.Error("catchup: failed to rebuild address_stats", "error", err)
					} else {
						log.Info("catchup: address_stats rebuild complete")
					}

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

		// If completion was called but new work appeared, we need to skip address stats
		// for this batch (they'll be updated incrementally or rebuilt later)
		if completionCalled {
			// Reset completion flag - we'll call it again when we go idle
			completionCalled = false
		}

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

			// Block may have been indexed by another process in the meantime
			exists, err := c.db.HasBlock(c.ctx, blockNum)
			if err != nil {
				log.Error("catchup: failed to check block existence", "block", blockNum, "error", err)
				continue
			}
			if exists {
				if c.collector != nil {
					c.collector.MarkBlockProcessed(c.ctx, blockNum)
				}
				atomic.AddInt64(&c.processedBlocks, 1)
				continue
			}

			if err := c.processBlock(blockNum); err != nil {
				log.Error("catchup: failed to process block", "worker", id, "block", blockNum, "error", err)
				// Re-insert into missing_block_ranges so the block is retried on next
				// poll. Without this, the block is permanently lost: the parent range
				// is already deleted from missing_block_ranges when the first sibling
				// block succeeds, so a failed block would never be retried.
				if requeueErr := c.db.RequeueMissingBlock(c.ctx, blockNum); requeueErr != nil {
					log.Error("catchup: failed to requeue block after error", "block", blockNum, "error", requeueErr)
				}
				continue
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

				total := processed + remaining
				percent := float64(0)
				if total > 0 {
					percent = float64(processed) / float64(total) * 100
				}

				log.Info("catchup: progress",
					"processed", processed,
					"remaining", remaining,
					"percent", percent)
			}
		}
	}
}

func (c *CatchupIndexer) processBlock(number uint64) error {
	return c.processBlockRaw(number)
}

func (c *CatchupIndexer) processBlockRaw(number uint64) error {
	rawBlock, err := c.rpc.RawBlockByNumber(c.ctx, number)
	if err != nil {
		return err
	}

	catchupConfig := *c.idxCfg
	catchupConfig.SkipAddressStats = true

	idx := &Indexer{
		db:               c.db,
		rpc:              c.rpc,
		config:           &catchupConfig,
		tokenCache:       c.tokenCache,
		contractCache:    c.contractCache,
		balanceWorkers:   c.balanceWorkers,
		tracingSupported: c.tracingSupported,
		eventBus:         c.eventBus,
	}

	if len(rawBlock.Transactions) > 0 {
		return idx.processBlockParallelRaw(c.ctx, rawBlock)
	}

	return idx.processBlockRaw(c.ctx, number)
}
