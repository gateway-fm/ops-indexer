package indexer

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/events"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
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

	SkipAddressStats    bool
	SkipReceiptTxTypes  map[int]bool // tx types to skip receipt fetching for (e.g. 126 for OP deposit)

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
	config        *Config

	tokenCache    *TokenCache
	contractCache *ContractCache

	balanceWorkers *BalanceWorkerPool

	tracingSupported bool
	eventBus         *events.Bus

	missingRangeCollector *MissingRangeCollector

	catchupIndexer  *CatchupIndexer
	realtimeIndexer *RealtimeIndexer

	catchupRunning bool
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
	}

	if cfg.EnableAsyncBalance && cfg.BalanceWorkers > 0 {
		idx.balanceWorkers = NewBalanceWorkerPool(database, rpcClient, cfg.BalanceWorkers, cfg.RPCRateLimit)
		// When balances actually land in the DB, refresh holder_count for the
		// affected tokens so the cached count on the tokens row matches the
		// new balance set without waiting for the next transfer. This is the
		// only path that can change holder_count, and therefore the only path
		// that recomputes it.
		idx.balanceWorkers.SetOnFlush(func(tokenAddresses []string) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for _, addr := range tokenAddresses {
				if err := database.RefreshTokenHolderCount(ctx, addr); err != nil {
					log.Warn("refresh holder count after balance flush failed", "token", addr, "error", err)
				}
			}
		})
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

func (i *Indexer) Start(ctx context.Context) error {
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

	// Backfill daily stats in background
	go func() {
		log.Info("starting daily stats backfill")
		if err := i.db.BackfillDailyStats(ctx); err != nil {
			log.Warn("daily stats backfill failed", "error", err)
		} else {
			log.Info("daily stats backfill completed")
		}
	}()

	i.db.UpdateSyncStatus(ctx, lastIndexed, true)
	catchupThreshold := uint64(100)
	useCatchup := i.config.CatchupEnabled && (hasGaps || blocksToSync > catchupThreshold)

	if useCatchup {
		return i.startWithMissingRangeCollector(ctx, latestOnChain)
	}

	return i.startStandardMode(ctx, lastIndexed)
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

	i.catchupIndexer = NewCatchupIndexer(
		i.db, i.rpc, catchupCfg, i.config,
		i.tokenCache, i.contractCache, i.balanceWorkers,
		i.tracingSupported,
	)
	i.catchupIndexer.SetEventBus(i.eventBus)

	realtimeCfg := &RealtimeConfig{
		ConfirmationBlocks: 1,
		PollInterval:       i.pollInterval,
	}

	i.realtimeIndexer = NewRealtimeIndexer(
		i.db, i.rpc, realtimeCfg, i.config,
		i.tokenCache, i.contractCache, i.balanceWorkers,
		i.tracingSupported,
	)
	i.realtimeIndexer.SetEventBus(i.eventBus)

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

	i.catchupIndexer = NewCatchupIndexer(
		i.db, i.rpc, catchupCfg, i.config,
		i.tokenCache, i.contractCache, i.balanceWorkers,
		i.tracingSupported,
	)
	i.catchupIndexer.SetEventBus(i.eventBus)
	i.catchupIndexer.SetCollector(i.missingRangeCollector)

	realtimeCfg := &RealtimeConfig{
		ConfirmationBlocks: 1,
		PollInterval:       i.pollInterval,
	}

	i.realtimeIndexer = NewRealtimeIndexer(
		i.db, i.rpc, realtimeCfg, i.config,
		i.tokenCache, i.contractCache, i.balanceWorkers,
		i.tracingSupported,
	)
	i.realtimeIndexer.SetEventBus(i.eventBus)

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
					log.Error("failed to check for reorg", "error", err)
				} else if reorgDepth > 0 {
					log.Warn("detected reorg, reverting blocks", "depth", reorgDepth)
					if err := i.handleReorg(ctx, lastIndexed-reorgDepth+1); err != nil {
						log.Error("failed to handle reorg", "error", err)
					} else {
						lastIndexed = lastIndexed - reorgDepth
					}
				}
			}

			for blockNum := lastIndexed + 1; blockNum <= latestOnChain; blockNum++ {
				select {
				case req := <-i.indexRequests:
					err := i.processBlock(ctx, req.blockNumber)
					req.done <- err
				default:
				}

				if err := i.processBlock(ctx, blockNum); err != nil {
					log.Error("failed to process block", "block", blockNum, "error", err)
					break
				}
				lastIndexed = blockNum

				if blockNum%10 == 0 {
					i.db.UpdateSyncStatus(ctx, lastIndexed, true)
				}
			}
		}
	}
}

func (i *Indexer) detectReorg(ctx context.Context, blockNumber uint64) (uint64, error) {
	maxReorgCheck := uint64(10)
	for depth := uint64(0); depth < maxReorgCheck && blockNumber > depth; depth++ {
		checkBlock := blockNumber - depth

		storedBlock, err := i.db.GetBlock(ctx, checkBlock)
		if err != nil || storedBlock == nil {
			continue
		}

		chainHash, err := i.rpc.RawBlockHash(ctx, checkBlock)
		if err != nil {
			return 0, err
		}

		if storedBlock.Hash != chainHash {
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
			return depth + 1, nil
		}
	}
	return 0, nil
}

func (i *Indexer) handleReorg(ctx context.Context, fromBlock uint64) error {
	log.Info("reverting blocks due to reorg", "from_block", fromBlock)

	lastIndexed, err := i.db.GetLatestBlockNumber(ctx)
	if err != nil {
		return err
	}

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

	return i.db.InsertBlock(ctx, b)
}

func (i *Indexer) processBlockParallelRaw(ctx context.Context, rawBlock *rpc.RawBlock) error {
	start := time.Now()
	blockNumber := rawBlock.NumberU64()
	rawTxs := rawBlock.Transactions
	blockTimestamp := uint64(rawBlock.Timestamp)

	// Build receipt fetch list, skipping tx types that are known to fail
	// (e.g. type 126 OP deposit system transactions on op-reth devnets)
	txHashes := make([]common.Hash, 0, len(rawTxs))
	for _, tx := range rawTxs {
		if i.config.SkipReceiptTxTypes != nil && i.config.SkipReceiptTxTypes[int(tx.Type)] {
			continue
		}
		txHashes = append(txHashes, tx.Hash)
	}

	receipts, err := i.rpc.FetchReceiptsBatch(ctx, txHashes, i.config.RPCWorkers, i.config.RPCRateLimit, blockNumber)
	if err != nil {
		return err
	}

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

		i.updateAddressStatsDelta(blockData.AddressStats, from, blockNumber, false)

		if to != nil {
			isContract := i.contractCache.Has(*to)
			i.updateAddressStatsDelta(blockData.AddressStats, *to, blockNumber, isContract)
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
				i.updateAddressStatsDelta(blockData.AddressStats, contractAddr, blockNumber, true)
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
				i.bumpTokenTransferDelta(blockData.AddressStats, fromAddr, blockNumber)
				i.bumpTokenTransferDelta(blockData.AddressStats, toAddr, blockNumber)
			}
		}
	}

	if i.tracingSupported && len(txHashes) > 0 {
		internalTxs, err := i.rpc.FetchTracesBatch(ctx, txHashes, blockNumber, blockTimestamp,
			i.config.TraceWorkers, i.config.TraceRateLimit)
		if err != nil {
			log.Warn("failed to fetch traces for block", "block", blockNumber, "error", err)
		} else if len(internalTxs) > 0 {
			blockData.InternalTransactions = internalTxs

			for _, it := range internalTxs {
				i.updateAddressStatsDeltaInternal(blockData.AddressStats, it.From, blockNumber)
				if it.To != nil {
					i.updateAddressStatsDeltaInternal(blockData.AddressStats, *it.To, blockNumber)
				}
			}
		}
	}

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
				i.tokenCache.Add(addr.Hex())
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

	if err := i.db.InsertBlockDataBatch(ctx, blockData); err != nil {
		return err
	}

	// Refresh transfer_count and total_supply for each token touched in this
	// block, synchronously, so the new counts are visible on the next read.
	// holder_count is not refreshed here: it is a function of balances, which
	// this block has not written yet -- the balance work is queued below and
	// fetched asynchronously over RPC. It is refreshed on that path instead.
	if len(blockData.Transfers) > 0 {
		touched := make(map[string]struct{}, len(blockData.Transfers))
		for _, t := range blockData.Transfers {
			touched[strings.ToLower(t.TokenAddress)] = struct{}{}
		}
		for tokenAddr := range touched {
			if err := i.db.RefreshTokenTransferStats(ctx, tokenAddr); err != nil {
				log.Warn("refresh token transfer stats failed", "token", tokenAddr, "error", err)
			}
		}
	}

	if i.balanceWorkers != nil && len(balanceWork) > 0 {
		queued := i.balanceWorkers.QueueWorkBatch(balanceWork)
		if queued < len(balanceWork) {
			log.Warn("balance queue full, dropped items", "dropped", len(balanceWork)-queued)
		}
	}

	if i.eventBus != nil {
		i.eventBus.PublishNewBlock(blockData.Block)
		for _, tx := range blockData.Transactions {
			i.eventBus.PublishNewTransaction(tx)
		}
	}

	elapsed := time.Since(start)
	if blockNumber%100 == 0 || elapsed > 2*time.Second {
		log.Info("indexed block (raw)",
			"block", blockNumber,
			"txs", len(rawTxs),
			"transfers", len(blockData.Transfers),
			"new_tokens", len(blockData.Tokens),
			"elapsed", elapsed)
	}

	// Update daily stats every 100 blocks
	if blockNumber%100 == 0 {
		go i.updateDailyStatsForDate(ctx, blockTimestamp)
	}

	return nil
}

func (i *Indexer) updateAddressStatsDelta(stats map[string]*db.AddressStatsDelta, address string, blockNumber uint64, isContract bool) {
	key := strings.ToLower(address)
	if delta, ok := stats[key]; ok {
		delta.TxCountDelta++
		if isContract {
			delta.IsContract = true
		}
	} else {
		// Store lowercased so the row PK matches `WHERE address = LOWER($1)` reads.
		stats[key] = &db.AddressStatsDelta{
			Address:      key,
			TxCountDelta: 1,
			IsContract:   isContract,
			BlockNumber:  blockNumber,
		}
	}
}

func (i *Indexer) updateDailyStatsForDate(ctx context.Context, blockTimestamp uint64) {
	date := time.Unix(int64(blockTimestamp), 0).UTC().Truncate(24 * time.Hour)
	stats, err := i.db.ComputeDailyStats(ctx, date)
	if err != nil {
		log.Warn("failed to compute daily stats", "date", date.Format("2006-01-02"), "error", err)
		return
	}
	if err := i.db.UpsertDailyStats(ctx, stats); err != nil {
		log.Warn("failed to upsert daily stats", "date", date.Format("2006-01-02"), "error", err)
	}
}

// bumpTokenTransferDelta records this address as a participant in a Transfer
// event. Creates the address_stats entry if absent so transfer-only
// recipients (who never sent a tx of their own) are still indexed. Skips
// the zero address (mint source / burn sink isn't a holder).
//
// When creating a new entry, we also seed TxCountDelta=1: this transfer's
// underlying tx genuinely touches the address (it shows up in their
// Transactions list via the same log-join we do in GetTransactionsByAddress).
// If the entry already exists, the address was already credited as tx-from
// or tx-to and we mustn't double-count. This is approximate when the same
// address appears in transfers across multiple txs in one block (the rebuild
// SQL is authoritative for those edge cases), but it eliminates the common
// pure-recipient drift of badge=0 vs list=N.
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

func (i *Indexer) bumpTokenTransferDelta(stats map[string]*db.AddressStatsDelta, addr common.Address, blockNumber uint64) {
	if addr == zeroAddress {
		return
	}
	key := strings.ToLower(addr.Hex())
	delta, ok := stats[key]
	if !ok {
		delta = &db.AddressStatsDelta{
			Address:      key,
			TxCountDelta: 1,
			BlockNumber:  blockNumber,
		}
		stats[key] = delta
	}
	delta.TokenTransferDelta++
}

func (i *Indexer) updateAddressStatsDeltaInternal(stats map[string]*db.AddressStatsDelta, address string, blockNumber uint64) {
	key := strings.ToLower(address)
	if delta, ok := stats[key]; ok {
		delta.InternalTxCountDelta++
	} else {
		stats[key] = &db.AddressStatsDelta{
			Address:              key,
			InternalTxCountDelta: 1,
			BlockNumber:          blockNumber,
		}
	}
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
