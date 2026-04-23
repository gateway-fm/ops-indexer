package indexer

import (
	"context"
	"sync"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
)

type MissingRangeCollectorConfig struct {
	// BatchSize is the number of blocks to scan in each batch
	BatchSize uint64
	// BackwardScanInterval is how often to scan backward (toward genesis)
	BackwardScanInterval time.Duration
	// ForwardScanInterval is how often to scan forward (toward head)
	ForwardScanInterval time.Duration
	// FirstBlock is the starting block for indexing (default 0 for genesis)
	FirstBlock uint64
	// LastBlock is the ending block (0 means chain head)
	LastBlock uint64
}

func DefaultMissingRangeCollectorConfig() *MissingRangeCollectorConfig {
	return &MissingRangeCollectorConfig{
		BatchSize:            100000,
		BackwardScanInterval: 10 * time.Millisecond, // Fast initial scan
		ForwardScanInterval:  1 * time.Minute,       // Slower for new blocks
		FirstBlock:           0,
		LastBlock:            0, // 0 means use chain head
	}
}

// MissingRangeCollector discovers and tracks missing block ranges using
// bidirectional scanning with persistent state (following Blockscout's pattern).
type MissingRangeCollector struct {
	db     Database
	rpc    RPCClient
	config *MissingRangeCollectorConfig

	// Scanning boundaries
	minFetchedBlock uint64 // Lowest block we've scanned (scanning backward)
	maxFetchedBlock uint64 // Highest block we've scanned (scanning forward)

	// Chain head
	chainHead uint64

	// State
	backfillComplete bool
	initialScanDone  bool

	// Coordination
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewMissingRangeCollector(database Database, rpcClient RPCClient, cfg *MissingRangeCollectorConfig) *MissingRangeCollector {
	if cfg == nil {
		cfg = DefaultMissingRangeCollectorConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &MissingRangeCollector{
		db:     database,
		rpc:    rpcClient,
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (c *MissingRangeCollector) Start(ctx context.Context) error {
	chainHead, err := c.rpc.BlockNumber(ctx)
	if err != nil {
		return err
	}
	c.chainHead = chainHead

	lastBlock := c.config.LastBlock
	if lastBlock == 0 || lastBlock > chainHead {
		lastBlock = chainHead
	}

	progress, err := c.db.GetIndexerProgress(ctx)
	if err != nil {
		log.Warn("failed to load indexer progress, starting fresh", "error", err)
	}

	if progress != nil && progress.MaxFetchedBlock > 0 {
		c.minFetchedBlock = progress.MinFetchedBlock
		c.maxFetchedBlock = progress.MaxFetchedBlock
		c.backfillComplete = progress.BackfillComplete

		log.Info("resuming missing range collection",
			"min_fetched", c.minFetchedBlock,
			"max_fetched", c.maxFetchedBlock,
			"backfill_complete", c.backfillComplete)
	} else {
		minIndexed, maxIndexed, err := c.db.GetMinMaxIndexedBlocks(ctx)
		if err != nil {
			return err
		}

		if maxIndexed > 0 {
			c.minFetchedBlock = minIndexed
			c.maxFetchedBlock = maxIndexed
			log.Info("initializing from existing blocks",
				"min_indexed", minIndexed,
				"max_indexed", maxIndexed)
		} else {
			c.minFetchedBlock = lastBlock
			c.maxFetchedBlock = lastBlock
			log.Info("empty database, starting from chain head",
				"chain_head", lastBlock)
		}

		c.db.UpdateIndexerProgress(ctx, c.minFetchedBlock, c.maxFetchedBlock, false)
	}

	if err := c.initialScan(ctx, lastBlock); err != nil {
		return err
	}

	c.wg.Add(2)
	go c.backwardScanner()
	go c.forwardScanner()

	log.Info("missing range collector started",
		"first_block", c.config.FirstBlock,
		"last_block", lastBlock,
		"chain_head", chainHead)

	return nil
}

func (c *MissingRangeCollector) Stop() {
	c.cancel()
	c.wg.Wait()
}

func (c *MissingRangeCollector) initialScan(ctx context.Context, lastBlock uint64) error {
	log.Info("starting initial gap scan", "from", c.config.FirstBlock, "to", lastBlock)

	batchSize := c.config.BatchSize
	totalRanges := 0

	for fromBlock := c.config.FirstBlock; fromBlock <= lastBlock; fromBlock += batchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		toBlock := fromBlock + batchSize - 1
		if toBlock > lastBlock {
			toBlock = lastBlock
		}

		ranges, err := c.db.FindMissingBlocksInRange(ctx, fromBlock, toBlock)
		if err != nil {
			log.Error("failed to find missing blocks", "from", fromBlock, "to", toBlock, "error", err)
			continue
		}

		if len(ranges) > 0 {
			if err := c.db.SaveMissingRanges(ctx, ranges); err != nil {
				log.Error("failed to save missing ranges", "error", err)
				continue
			}
			totalRanges += len(ranges)
		}

		if toBlock%100000 == 0 || toBlock == lastBlock {
			log.Info("initial scan progress",
				"scanned_to", toBlock,
				"total_ranges_found", totalRanges)
		}
	}

	totalMissing, _ := c.db.GetTotalMissingBlocks(ctx)
	log.Info("initial scan complete",
		"ranges_found", totalRanges,
		"total_missing_blocks", totalMissing)

	c.initialScanDone = true
	return nil
}

func (c *MissingRangeCollector) backwardScanner() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.BackwardScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.scanBackward()

			c.mu.RLock()
			if c.minFetchedBlock <= c.config.FirstBlock && !c.backfillComplete {
				c.mu.RUnlock()
				c.mu.Lock()
				c.backfillComplete = true
				c.db.UpdateIndexerProgress(c.ctx, c.minFetchedBlock, c.maxFetchedBlock, true)
				c.mu.Unlock()

				log.Info("backfill scan complete, switching to maintenance mode")
				// Switch to slower interval for maintenance
				ticker.Reset(1 * time.Minute)
			} else {
				c.mu.RUnlock()
			}
		}
	}
}

func (c *MissingRangeCollector) forwardScanner() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.ForwardScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.scanForward()
		}
	}
}

func (c *MissingRangeCollector) scanBackward() {
	c.mu.Lock()
	if c.minFetchedBlock <= c.config.FirstBlock {
		c.mu.Unlock()
		return
	}

	toBlock := c.minFetchedBlock - 1
	fromBlock := uint64(0)
	if toBlock > c.config.BatchSize {
		fromBlock = toBlock - c.config.BatchSize + 1
	}
	fromBlock = max(fromBlock, c.config.FirstBlock)

	c.mu.Unlock()

	if fromBlock > toBlock {
		return
	}

	ranges, err := c.db.FindMissingBlocksInRange(c.ctx, fromBlock, toBlock)
	if err != nil {
		log.Error("backward scan failed", "from", fromBlock, "to", toBlock, "error", err)
		return
	}

	if len(ranges) > 0 {
		if err := c.db.SaveMissingRanges(c.ctx, ranges); err != nil {
			log.Error("failed to save missing ranges", "error", err)
			return
		}

		missingCount := uint64(0)
		for _, r := range ranges {
			missingCount += r.ToNumber - r.FromNumber + 1
		}
		log.Debug("backward scan found gaps",
			"from", fromBlock,
			"to", toBlock,
			"ranges", len(ranges),
			"missing_blocks", missingCount)
	}

	c.mu.Lock()
	c.minFetchedBlock = fromBlock
	c.db.UpdateIndexerProgress(c.ctx, c.minFetchedBlock, c.maxFetchedBlock, c.backfillComplete)
	c.mu.Unlock()
}

func (c *MissingRangeCollector) scanForward() {
	chainHead, err := c.rpc.BlockNumber(c.ctx)
	if err != nil {
		log.Error("failed to get chain head", "error", err)
		return
	}

	c.mu.Lock()
	if chainHead <= c.maxFetchedBlock {
		c.mu.Unlock()
		return
	}

	fromBlock := c.maxFetchedBlock + 1
	toBlock := chainHead
	c.mu.Unlock()

	ranges, err := c.db.FindMissingBlocksInRange(c.ctx, fromBlock, toBlock)
	if err != nil {
		log.Error("forward scan failed", "from", fromBlock, "to", toBlock, "error", err)
		return
	}

	if len(ranges) > 0 {
		if err := c.db.SaveMissingRanges(c.ctx, ranges); err != nil {
			log.Error("failed to save missing ranges", "error", err)
			return
		}

		missingCount := uint64(0)
		for _, r := range ranges {
			missingCount += r.ToNumber - r.FromNumber + 1
		}
		log.Debug("forward scan found gaps",
			"from", fromBlock,
			"to", toBlock,
			"ranges", len(ranges),
			"missing_blocks", missingCount)
	}

	c.mu.Lock()
	c.maxFetchedBlock = toBlock
	c.chainHead = chainHead
	c.db.UpdateIndexerProgress(c.ctx, c.minFetchedBlock, c.maxFetchedBlock, c.backfillComplete)
	c.mu.Unlock()
}

func (c *MissingRangeCollector) GetProgress() (minFetched, maxFetched, chainHead uint64, backfillComplete bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.minFetchedBlock, c.maxFetchedBlock, c.chainHead, c.backfillComplete
}

func (c *MissingRangeCollector) GetMissingRangesBatch(ctx context.Context, batchSize int) ([]db.BlockRange, error) {
	return c.db.GetMissingRangesBatch(ctx, batchSize)
}

func (c *MissingRangeCollector) MarkBlockProcessed(ctx context.Context, blockNumber uint64) error {
	return c.db.DeleteMissingRangeByBlock(ctx, blockNumber)
}

func (c *MissingRangeCollector) GetTotalMissingBlocks(ctx context.Context) (int64, error) {
	return c.db.GetTotalMissingBlocks(ctx)
}

func (c *MissingRangeCollector) IsBackfillComplete() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.backfillComplete
}
