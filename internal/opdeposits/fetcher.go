package opdeposits

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/crypto"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

var transactionDepositedTopic = crypto.Keccak256Hash([]byte("TransactionDeposited(address,address,uint256,bytes)"))

type FetcherConfig struct {
	L1RPCURL              string
	OptimismPortalAddress common.Address
	PollInterval          time.Duration
	BatchSize             int
	StartBlock            uint64
	ConfirmationBlocks    uint64
}

type Fetcher struct {
	db     *db.DB
	config *FetcherConfig

	l1Client *rpclient.Client
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewFetcher(database *db.DB, cfg *FetcherConfig) *Fetcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Fetcher{
		db:     database,
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (f *Fetcher) Start(ctx context.Context) error {
	f.l1Client = rpclient.Dial(f.config.L1RPCURL)

	go f.run(ctx)
	return nil
}

func (f *Fetcher) Stop() {
	f.cancel()
}

func (f *Fetcher) run(ctx context.Context) {
	ticker := time.NewTicker(f.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			if err := f.poll(ctx); err != nil {
				log.Error("op_deposits: poll error", "error", err)
			}
		}
	}
}

func (f *Fetcher) poll(ctx context.Context) error {
	fromBlock, err := f.getStartBlock(ctx)
	if err != nil {
		return err
	}

	l1Head, err := f.l1Client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get L1 block number: %w", err)
	}

	safeHead := l1Head
	if safeHead > f.config.ConfirmationBlocks {
		safeHead -= f.config.ConfirmationBlocks
	} else {
		return nil
	}

	if fromBlock > safeHead {
		return nil
	}

	batchSize := uint64(f.config.BatchSize)
	for batchStart := fromBlock; batchStart <= safeHead; batchStart += batchSize {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > safeHead {
			batchEnd = safeHead
		}

		deposits, err := f.fetchDepositEvents(ctx, batchStart, batchEnd)
		if err != nil {
			return fmt.Errorf("failed to fetch deposit events [%d, %d]: %w", batchStart, batchEnd, err)
		}

		if len(deposits) > 0 {
			if err := f.db.InsertOPDepositsBatch(ctx, deposits); err != nil {
				return fmt.Errorf("failed to insert deposits: %w", err)
			}
			log.Info("op_deposits: indexed deposits",
				"l1_from", batchStart,
				"l1_to", batchEnd,
				"count", len(deposits))
		}
	}

	return nil
}

func (f *Fetcher) getStartBlock(ctx context.Context) (uint64, error) {
	lastIndexed, err := f.db.GetLastIndexedL1Block(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get last indexed L1 block: %w", err)
	}

	if lastIndexed > 0 {
		return lastIndexed + 1, nil
	}

	if f.config.StartBlock > 0 {
		return f.config.StartBlock, nil
	}

	l1Head, err := f.l1Client.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get L1 block number: %w", err)
	}
	if l1Head > f.config.ConfirmationBlocks {
		return l1Head - f.config.ConfirmationBlocks, nil
	}
	return 0, nil
}

func (f *Fetcher) fetchDepositEvents(ctx context.Context, fromBlock, toBlock uint64) ([]*types.OPDeposit, error) {
	query := rpclient.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{f.config.OptimismPortalAddress},
		Topics:    [][]common.Hash{{transactionDepositedTopic}},
	}

	logs, err := f.l1Client.FilterLogs(ctx, query)
	if err != nil {
		return nil, err
	}

	var deposits []*types.OPDeposit

	for _, logEntry := range logs {
		if len(logEntry.Topics) < 4 {
			continue
		}

		fromAddr := common.BytesToAddress(logEntry.Topics[1].Bytes())
		toAddr := common.BytesToAddress(logEntry.Topics[2].Bytes())
		if len(logEntry.Data) < 64 {
			continue
		}

		offset := new(big.Int).SetBytes(logEntry.Data[:32]).Uint64()
		if offset+32 > uint64(len(logEntry.Data)) {
			continue
		}
		length := new(big.Int).SetBytes(logEntry.Data[offset : offset+32]).Uint64()
		if offset+32+length > uint64(len(logEntry.Data)) {
			continue
		}
		opaqueData := logEntry.Data[offset+32 : offset+32+length]

		mint, value, gasLimit, isCreation, data, err := ParseOpaqueData(opaqueData)
		if err != nil {
			log.Warn("op_deposits: failed to parse opaque data",
				"tx", logEntry.TxHash.Hex(),
				"error", err)
			continue
		}

		var depositTo *common.Address
		if !isCreation {
			depositTo = &toAddr
		}

		sourceHash := ComputeSourceHash(logEntry.BlockHash, uint64(logEntry.Index))

		l2TxHash, err := ComputeL2DepositTxHash(sourceHash, fromAddr, depositTo, mint, value, gasLimit, false, data)
		if err != nil {
			log.Warn("op_deposits: failed to compute L2 tx hash",
				"l1_tx", logEntry.TxHash.Hex(),
				"error", err)
			continue
		}

		l1Block, err := f.l1Client.HeaderByNumber(ctx, new(big.Int).SetUint64(logEntry.BlockNumber))
		var l1Timestamp *uint64
		if err == nil && l1Block != nil {
			ts := l1Block.Time
			l1Timestamp = &ts
		}

		deposits = append(deposits, &types.OPDeposit{
			L2TxHash:        l2TxHash.Hex(),
			L1BlockNumber:   logEntry.BlockNumber,
			L1BlockTimestamp: l1Timestamp,
			L1TxHash:        logEntry.TxHash.Hex(),
			L1TxOrigin:      fromAddr.Hex(),
		})
	}

	return deposits, nil
}
