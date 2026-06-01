package indexer

import (
	"context"
	"math/big"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

type Database interface {
	GetLatestBlockNumber(ctx context.Context) (uint64, error)
	GetBlockCount(ctx context.Context) (int64, error)
	GetBlock(ctx context.Context, number uint64) (*types.Block, error)
	DeleteBlock(ctx context.Context, number uint64) error
	InsertBlock(ctx context.Context, b *types.Block) error
	InsertBlockDataBatch(ctx context.Context, b *db.BlockData) error
	UpdateSyncStatus(ctx context.Context, lastIndexed uint64, isSyncing bool) error
	HasBlock(ctx context.Context, number uint64) (bool, error)
	DeleteMissingRangeByBlock(ctx context.Context, blockNum uint64) error
	RequeueMissingBlock(ctx context.Context, blockNum uint64) error
	GetMissingRangesBatch(ctx context.Context, batchSize int) ([]db.BlockRange, error)
	RebuildAddressStats(ctx context.Context) error

	InsertTransaction(ctx context.Context, tx *types.Transaction) error
	InsertContract(ctx context.Context, c *types.Contract) error
	UpsertAddressStats(ctx context.Context, address string, blockNumber uint64, isContract bool) error
	IsContract(ctx context.Context, address string) (bool, error)
	InsertLog(ctx context.Context, l *types.Log) error
	InsertTokenTransfer(ctx context.Context, t *types.TokenTransfer) error
	UpdateAddressStatsCounters(ctx context.Context, address string, internalTxCount int, tokenTransferCount int) error
	InsertBalance(ctx context.Context, b *types.Balance) error
	GetToken(ctx context.Context, address string) (*types.Token, error)
	InsertToken(ctx context.Context, t *types.Token) error

	GetMinMaxIndexedBlocks(ctx context.Context) (uint64, uint64, error)
	GetTotalMissingBlocks(ctx context.Context) (int64, error)
	GetIndexerProgress(ctx context.Context) (*db.IndexerProgress, error)
	UpdateIndexerProgress(ctx context.Context, minBlock, maxBlock uint64, backfillComplete bool) error
	FindMissingBlocksInRange(ctx context.Context, fromBlock, toBlock uint64) ([]db.BlockRange, error)
	SaveMissingRanges(ctx context.Context, ranges []db.BlockRange) error

	GetAllTokenAddresses(ctx context.Context) ([]string, error)
	InsertBalancesBatch(ctx context.Context, balances []*types.Balance) error
	RefreshTokenStats(ctx context.Context, tokenAddress string) error

	ComputeDailyStats(ctx context.Context, date time.Time) (*types.DailyStats, error)
	UpsertDailyStats(ctx context.Context, stats *types.DailyStats) error
	BackfillDailyStats(ctx context.Context) error

	WipeAllData(ctx context.Context) error
}

type RPCClient interface {
	BlockNumber(ctx context.Context) (uint64, error)
	RawBlockByNumber(ctx context.Context, number uint64) (*rpc.RawBlock, error)
	RawBlockHash(ctx context.Context, number uint64) (string, error)
	FetchReceiptsBatch(ctx context.Context, txHashes []common.Hash, workers int, rateLimit int, blockNumber ...uint64) (map[common.Hash]*rpclient.Receipt, error)
	GetTotalDifficulty(ctx context.Context, blockNumber uint64) string
	CheckTracingSupport(ctx context.Context) (bool, error)
	SubscribeNewHead(ctx context.Context, ch chan<- *rpclient.Header) (rpclient.Subscription, error)
	CallContract(ctx context.Context, to common.Address, data []byte) ([]byte, error)

	GetCode(ctx context.Context, address common.Address) ([]byte, error)
	FetchTracesBatch(ctx context.Context, txHashes []common.Hash, startBlock, endBlock uint64, workers, rateLimit int) ([]*types.InternalTransaction, error)
	FetchTokenMetadataBatch(ctx context.Context, addresses []common.Address, workers int, rateLimit int) (map[common.Address]*rpc.TokenMetadataResult, error)
	FetchTokenURIsBatch(ctx context.Context, reqs []rpc.NFTURIRequest, workers int, rateLimit int) map[int]string
	ChainID(ctx context.Context) (*big.Int, error)
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*rpclient.Receipt, error)
}
