package indexer

import (
	"context"
	"errors"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockDatabase is a mock of the Database interface
type MockDatabase struct {
	mock.Mock
}

func (m *MockDatabase) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockDatabase) GetBlockCount(ctx context.Context) (int64, error) {
	args := m.Called(ctx)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDatabase) GetBlockCountInRange(ctx context.Context, from, to uint64) (int64, error) {
	args := m.Called(ctx, from, to)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDatabase) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	args := m.Called(ctx, number)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*types.Block), args.Error(1)
}

func (m *MockDatabase) DeleteBlock(ctx context.Context, number uint64) error {
	args := m.Called(ctx, number)
	return args.Error(0)
}

func (m *MockDatabase) InsertBlock(ctx context.Context, b *types.Block) error {
	args := m.Called(ctx, b)
	return args.Error(0)
}

func (m *MockDatabase) InsertBlockDataBatch(ctx context.Context, b *db.BlockData) error {
	args := m.Called(ctx, b)
	return args.Error(0)
}

func (m *MockDatabase) UpdateSyncStatus(ctx context.Context, lastIndexed uint64, isSyncing bool) error {
	args := m.Called(ctx, lastIndexed, isSyncing)
	return args.Error(0)
}

func (m *MockDatabase) HasBlock(ctx context.Context, number uint64) (bool, error) {
	args := m.Called(ctx, number)
	return args.Bool(0), args.Error(1)
}

func (m *MockDatabase) DeleteMissingRangeByBlock(ctx context.Context, blockNum uint64) error {
	args := m.Called(ctx, blockNum)
	return args.Error(0)
}

func (m *MockDatabase) RequeueMissingBlock(ctx context.Context, blockNum uint64) error {
	args := m.Called(ctx, blockNum)
	return args.Error(0)
}

func (m *MockDatabase) GetMissingRangesBatch(ctx context.Context, batchSize int) ([]db.BlockRange, error) {
	args := m.Called(ctx, batchSize)
	return args.Get(0).([]db.BlockRange), args.Error(1)
}

func (m *MockDatabase) RebuildAddressStats(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabase) InsertTransaction(ctx context.Context, tx *types.Transaction) error {
	args := m.Called(ctx, tx)
	return args.Error(0)
}

func (m *MockDatabase) InsertContract(ctx context.Context, l *types.Contract) error {
	args := m.Called(ctx, l)
	return args.Error(0)
}

func (m *MockDatabase) UpsertAddressStats(ctx context.Context, address string, blockNumber uint64, isContract bool) error {
	args := m.Called(ctx, address, blockNumber, isContract)
	return args.Error(0)
}

func (m *MockDatabase) IsContract(ctx context.Context, address string) (bool, error) {
	args := m.Called(ctx, address)
	return args.Bool(0), args.Error(1)
}

func (m *MockDatabase) InsertLog(ctx context.Context, l *types.Log) error {
	args := m.Called(ctx, l)
	return args.Error(0)
}

func (m *MockDatabase) InsertTokenTransfer(ctx context.Context, t *types.TokenTransfer) error {
	args := m.Called(ctx, t)
	return args.Error(0)
}

func (m *MockDatabase) UpdateAddressStatsCounters(ctx context.Context, address string, internalTxCount int, tokenTransferCount int) error {
	args := m.Called(ctx, address, internalTxCount, tokenTransferCount)
	return args.Error(0)
}

func (m *MockDatabase) InsertBalance(ctx context.Context, b *types.Balance) error {
	args := m.Called(ctx, b)
	return args.Error(0)
}

func (m *MockDatabase) GetToken(ctx context.Context, address string) (*types.Token, error) {
	args := m.Called(ctx, address)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*types.Token), args.Error(1)
}

func (m *MockDatabase) InsertToken(ctx context.Context, t *types.Token) error {
	args := m.Called(ctx, t)
	return args.Error(0)
}

func (m *MockDatabase) GetMinMaxIndexedBlocks(ctx context.Context) (uint64, uint64, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint64), args.Get(1).(uint64), args.Error(2)
}

func (m *MockDatabase) GetTotalMissingBlocks(ctx context.Context) (int64, error) {
	args := m.Called(ctx)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDatabase) GetIndexerProgress(ctx context.Context) (*db.IndexerProgress, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*db.IndexerProgress), args.Error(1)
}

func (m *MockDatabase) UpdateIndexerProgress(ctx context.Context, minBlock, maxBlock uint64, backfillComplete bool) error {
	args := m.Called(ctx, minBlock, maxBlock, backfillComplete)
	return args.Error(0)
}

func (m *MockDatabase) FindMissingBlocksInRange(ctx context.Context, fromBlock, toBlock uint64) ([]db.BlockRange, error) {
	args := m.Called(ctx, fromBlock, toBlock)
	return args.Get(0).([]db.BlockRange), args.Error(1)
}

func (m *MockDatabase) SaveMissingRanges(ctx context.Context, ranges []db.BlockRange) error {
	args := m.Called(ctx, ranges)
	return args.Error(0)
}

func (m *MockDatabase) GetAllTokenAddresses(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockDatabase) InsertBalancesBatch(ctx context.Context, balances []*types.Balance) error {
	args := m.Called(ctx, balances)
	return args.Error(0)
}

func (m *MockDatabase) RefreshTokenStats(ctx context.Context, tokenAddress string) error {
	args := m.Called(ctx, tokenAddress)
	return args.Error(0)
}

func (m *MockDatabase) ComputeDailyStats(ctx context.Context, date time.Time) (*types.DailyStats, error) {
	args := m.Called(ctx, date)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*types.DailyStats), args.Error(1)
}

func (m *MockDatabase) UpsertDailyStats(ctx context.Context, stats *types.DailyStats) error {
	args := m.Called(ctx, stats)
	return args.Error(0)
}

func (m *MockDatabase) BackfillDailyStats(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabase) WipeAllData(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockRPCClient is a mock of the RPCClient interface
type MockRPCClient struct {
	mock.Mock
}

func (m *MockRPCClient) BlockNumber(ctx context.Context) (uint64, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockRPCClient) RawBlockByNumber(ctx context.Context, number uint64) (*rpc.RawBlock, error) {
	args := m.Called(ctx, number)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rpc.RawBlock), args.Error(1)
}

func (m *MockRPCClient) RawBlockHash(ctx context.Context, number uint64) (string, error) {
	args := m.Called(ctx, number)
	return args.String(0), args.Error(1)
}

func (m *MockRPCClient) FetchReceiptsBatch(ctx context.Context, txHashes []common.Hash, workers int, rateLimit int, blockNumber ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	args := m.Called(ctx, txHashes, workers, rateLimit)
	return args.Get(0).(map[common.Hash]*rpclient.Receipt), args.Error(1)
}

func (m *MockRPCClient) GetTotalDifficulty(ctx context.Context, blockNumber uint64) string {
	args := m.Called(ctx, blockNumber)
	return args.String(0)
}

func (m *MockRPCClient) CheckTracingSupport(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}

func (m *MockRPCClient) SubscribeNewHead(ctx context.Context, ch chan<- *rpclient.Header) (rpclient.Subscription, error) {
	args := m.Called(ctx, ch)
	return args.Get(0).(rpclient.Subscription), args.Error(1)
}

func (m *MockRPCClient) CallContract(ctx context.Context, to common.Address, data []byte) ([]byte, error) {
	args := m.Called(ctx, to, data)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockRPCClient) GetCode(ctx context.Context, address common.Address) ([]byte, error) {
	args := m.Called(ctx, address)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockRPCClient) FetchTracesBatch(ctx context.Context, txHashes []common.Hash, startBlock, endBlock uint64, workers, rateLimit int) ([]*types.InternalTransaction, error) {
	args := m.Called(ctx, txHashes, startBlock, endBlock, workers, rateLimit)
	return args.Get(0).([]*types.InternalTransaction), args.Error(1)
}

func (m *MockRPCClient) FetchTokenMetadataBatch(ctx context.Context, addresses []common.Address, workers int, rateLimit int) (map[common.Address]*rpc.TokenMetadataResult, error) {
	args := m.Called(ctx, addresses, workers, rateLimit)
	return args.Get(0).(map[common.Address]*rpc.TokenMetadataResult), args.Error(1)
}

func (m *MockRPCClient) FetchTokenURIsBatch(ctx context.Context, reqs []rpc.NFTURIRequest, workers int, rateLimit int) map[int]string {
	args := m.Called(ctx, reqs, workers, rateLimit)
	return args.Get(0).(map[int]string)
}

func (m *MockRPCClient) ChainID(ctx context.Context) (*big.Int, error) {
	args := m.Called(ctx)
	return args.Get(0).(*big.Int), args.Error(1)
}

func (m *MockRPCClient) TransactionReceipt(ctx context.Context, txHash common.Hash) (*rpclient.Receipt, error) {
	args := m.Called(ctx, txHash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rpclient.Receipt), args.Error(1)
}

func TestIndexer_GetCatchupProgress(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)
	idx := New(mockDB, mockRPC, time.Second, 0)

	processed, total, percent, isRunning := idx.GetCatchupProgress()
	assert.Equal(t, int64(0), processed)
	assert.Equal(t, uint64(0), total)
	assert.Equal(t, 100.0, percent)
	assert.False(t, isRunning)
}

func TestIndexer_DetectReorg(t *testing.T) {
	ctx := context.Background()

	t.Run("no reorg", func(t *testing.T) {
		mockDB := new(MockDatabase)
		mockRPC := new(MockRPCClient)
		idx := New(mockDB, mockRPC, time.Second, 0)

		blockNum := uint64(100)
		expectedHash := "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

		mockDB.On("GetBlock", ctx, mock.Anything).Return(&types.Block{Hash: expectedHash}, nil)
		mockRPC.On("RawBlockHash", ctx, mock.Anything).Return(expectedHash, nil)

		reorgAt, err := idx.detectReorg(ctx, blockNum)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), reorgAt)
		mockDB.AssertExpectations(t)
		mockRPC.AssertExpectations(t)
	})

	t.Run("reorg detected at parent", func(t *testing.T) {
		mockDB := new(MockDatabase)
		mockRPC := new(MockRPCClient)
		idx := New(mockDB, mockRPC, time.Second, 0)

		blockNum := uint64(100)

		// Current block matches
		chainHash100 := "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
		mockDB.On("GetBlock", ctx, uint64(100)).Return(&types.Block{Hash: chainHash100}, nil).Once()
		mockRPC.On("RawBlockHash", ctx, uint64(100)).Return(chainHash100, nil).Once()

		// Parent doesn't match
		mockDB.On("GetBlock", ctx, uint64(99)).Return(&types.Block{Hash: "0xdb_parent_hash_00000000000000000000000000000000000000000000000000"}, nil).Once()
		chainHash99 := "0x1111111111111111111111111111111111111111111111111111111111111111"
		mockRPC.On("RawBlockHash", ctx, uint64(99)).Return(chainHash99, nil).Once()

		reorgAt, err := idx.detectReorg(ctx, blockNum)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), reorgAt) // depth 1 (parent) + 1
		mockDB.AssertExpectations(t)
		mockRPC.AssertExpectations(t)
	})
}

func TestIndexer_ProcessBlock(t *testing.T) {
	ctx := context.Background()

	t.Run("standard block (empty)", func(t *testing.T) {
		mockDB := new(MockDatabase)
		mockRPC := new(MockRPCClient)
		idx := New(mockDB, mockRPC, time.Second, 0)

		blockNum := uint64(200)
		blockNumber := hexutil.Big(*big.NewInt(int64(blockNum)))

		rawBlock := &rpc.RawBlock{
			Number:     &blockNumber,
			Hash:       common.HexToHash("0xaaa"),
			ParentHash: common.HexToHash("0x123"),
			Timestamp:  hexutil.Uint64(time.Now().Unix()),
			GasUsed:    hexutil.Uint64(0),
			GasLimit:   hexutil.Uint64(8000000),
			Miner:      common.HexToAddress("0x0"),
			Transactions: []rpc.RawTransaction{},
		}

		mockRPC.On("RawBlockByNumber", ctx, blockNum).Return(rawBlock, nil).Once()

		mockDB.On("InsertBlock", ctx, mock.MatchedBy(func(b *types.Block) bool {
			return b.Number == blockNum
		})).Return(nil).Once()

		err := idx.processBlock(ctx, blockNum)
		assert.NoError(t, err)

		mockDB.AssertExpectations(t)
		mockRPC.AssertExpectations(t)
	})

	t.Run("block with transactions", func(t *testing.T) {
		mockDB := new(MockDatabase)
		mockRPC := new(MockRPCClient)
		idx := New(mockDB, mockRPC, time.Second, 0)

		blockNum := uint64(201)
		blockNumber := hexutil.Big(*big.NewInt(int64(blockNum)))
		toAddr := common.HexToAddress("0x222")
		txHash := common.HexToHash("0xdeadbeef")

		rawBlock := &rpc.RawBlock{
			Number:     &blockNumber,
			Hash:       common.HexToHash("0xbbb"),
			ParentHash: common.HexToHash("0x000"),
			Timestamp:  hexutil.Uint64(time.Now().Unix()),
			GasUsed:    hexutil.Uint64(21000),
			GasLimit:   hexutil.Uint64(8000000),
			Miner:      common.HexToAddress("0x0"),
			Transactions: []rpc.RawTransaction{
				{
					Hash:             txHash,
					BlockHash:        common.HexToHash("0xbbb"),
					BlockNumber:      &blockNumber,
					TransactionIndex: hexutil.Uint64(0),
					From:             common.HexToAddress("0x111"),
					To:               &toAddr,
					Value:            func() *hexutil.Big { v := hexutil.Big(*big.NewInt(0)); return &v }(),
					Gas:              hexutil.Uint64(21000),
					GasPrice:         func() *hexutil.Big { v := hexutil.Big(*big.NewInt(1)); return &v }(),
					Input:            hexutil.Bytes{},
					Nonce:            func() *hexutil.Uint64 { v := hexutil.Uint64(0); return &v }(),
					Type:             hexutil.Uint64(0),
				},
			},
		}

		mockRPC.On("RawBlockByNumber", ctx, blockNum).Return(rawBlock, nil).Once()
		mockRPC.On("FetchReceiptsBatch", ctx, []common.Hash{txHash}, mock.Anything, mock.Anything).Return(map[common.Hash]*rpclient.Receipt{
			txHash: {Status: 1, TxHash: txHash},
		}, nil).Once()

		mockDB.On("InsertBlockDataBatch", ctx, mock.MatchedBy(func(b *db.BlockData) bool {
			return b.Block.Number == blockNum && len(b.Transactions) == 1
		})).Return(nil).Once()

		err := idx.processBlock(ctx, blockNum)
		assert.NoError(t, err)

		mockDB.AssertExpectations(t)
		mockRPC.AssertExpectations(t)
	})
}

// TestIndexer_TokenCacheNotPoisonedByFailedBatch pins the ordering that keeps
// the new-token counter seed correct.
//
// tokenCache.Add used to run as soon as metadata came back, before
// InsertBlockDataBatch. Two things followed. A batch that then failed left the
// cache claiming a token whose row was never written. And, worse, another
// worker parsing a transfer of that token inside the window saw a cache hit,
// omitted the token from its own data.Tokens, and so neither seeded the row nor
// was covered by the seed -- its delta UPDATE found no visible row and silently
// counted nothing, permanently short.
//
// The concurrent interleaving cannot be provoked on demand, but the ordering it
// depends on can: after a failed batch the token must still be unknown, so the
// next attempt re-queues it and blocks on the tokens primary key instead of
// skipping it.
func TestIndexer_TokenCacheNotPoisonedByFailedBatch(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)
	idx := New(mockDB, mockRPC, time.Second, 0)

	blockNum := uint64(4931)
	blockNumber := hexutil.Big(*big.NewInt(int64(blockNum)))
	txHash := common.HexToHash("0xfeed01")
	tokenAddr := common.HexToAddress("0x00000000000000000000000000000000000c7ac1")
	toAddr := common.HexToAddress("0x222")

	rawBlock := &rpc.RawBlock{
		Number:     &blockNumber,
		Hash:       common.HexToHash("0xccc"),
		ParentHash: common.HexToHash("0x000"),
		Timestamp:  hexutil.Uint64(time.Now().Unix()),
		GasUsed:    hexutil.Uint64(21000),
		GasLimit:   hexutil.Uint64(8000000),
		Miner:      common.HexToAddress("0x0"),
		Transactions: []rpc.RawTransaction{{
			Hash:             txHash,
			BlockHash:        common.HexToHash("0xccc"),
			BlockNumber:      &blockNumber,
			TransactionIndex: hexutil.Uint64(0),
			From:             common.HexToAddress("0x111"),
			To:               &toAddr,
			Value:            func() *hexutil.Big { v := hexutil.Big(*big.NewInt(0)); return &v }(),
			Gas:              hexutil.Uint64(21000),
			GasPrice:         func() *hexutil.Big { v := hexutil.Big(*big.NewInt(1)); return &v }(),
			Input:            hexutil.Bytes{},
			Nonce:            func() *hexutil.Uint64 { v := hexutil.Uint64(0); return &v }(),
			Type:             hexutil.Uint64(0),
		}},
	}

	// One ERC20 Transfer log, so the token is discovered.
	holder := common.HexToHash("0x0000000000000000000000001111111111111111111111111111111111111111")
	mockRPC.On("RawBlockByNumber", ctx, blockNum).Return(rawBlock, nil).Once()
	mockRPC.On("FetchReceiptsBatch", ctx, []common.Hash{txHash}, mock.Anything, mock.Anything).
		Return(map[common.Hash]*rpclient.Receipt{
			txHash: {Status: 1, TxHash: txHash, Logs: []*rpclient.Log{{
				Address: tokenAddr,
				Topics:  []common.Hash{transferTopic, common.Hash{}, holder},
				Data:    hexutil.Bytes(common.LeftPadBytes(big.NewInt(1000).Bytes(), 32)),
				TxHash:  txHash,
			}}},
		}, nil).Once()

	name := "Cache Token"
	mockRPC.On("FetchTokenMetadataBatch", ctx, mock.Anything, mock.Anything, mock.Anything).
		Return(map[common.Address]*rpc.TokenMetadataResult{
			tokenAddr: {Address: tokenAddr, Symbol: "CT", Name: &name, Decimals: 18},
		}, nil).Once()

	// The batch fails, so nothing was committed.
	mockDB.On("InsertBlockDataBatch", ctx, mock.Anything).
		Return(errors.New("commit failed")).Once()

	err := idx.processBlock(ctx, blockNum)
	require.Error(t, err, "a failed batch must surface as an error")

	assert.False(t, idx.tokenCache.Has(tokenAddr.Hex()),
		"the cache must not claim a token whose row was never committed: another worker would "+
			"then omit it from its own data.Tokens and its counter delta would find no row")

	mockDB.AssertExpectations(t)
	mockRPC.AssertExpectations(t)
}

func TestChainResetDetection(t *testing.T) {
	t.Run("detects chain reset and returns error without FORCE_REINDEX", func(t *testing.T) {
		mockDB := new(MockDatabase)
		mockRPC := new(MockRPCClient)
		idx := New(mockDB, mockRPC, time.Second, 0)

		// Simulate: DB says we indexed up to block 25000, but chain head is at 5.
		mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(25000), nil)
		mockDB.On("GetBlockCount", mock.Anything).Return(int64(25000), nil)
		mockDB.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(25000), nil).Maybe()
		mockRPC.On("BlockNumber", mock.Anything).Return(uint64(5), nil)
		mockRPC.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
		mockDB.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()

		// Ensure FORCE_REINDEX is not set
		os.Unsetenv("FORCE_REINDEX")

		err := idx.Start(context.Background())
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "chain reset detected"),
			"error should mention chain reset, got: %s", err.Error())

		// WipeAllData should NOT have been called
		mockDB.AssertNotCalled(t, "WipeAllData", mock.Anything)
	})

	t.Run("detects chain reset and wipes with FORCE_REINDEX=true", func(t *testing.T) {
		mockDB := new(MockDatabase)
		mockRPC := new(MockRPCClient)
		idx := New(mockDB, mockRPC, time.Second, 0)

		// Simulate: DB says we indexed up to block 25000, but chain head is at 5.
		mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(25000), nil)
		mockDB.On("GetBlockCount", mock.Anything).Return(int64(25000), nil)
		mockDB.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(25000), nil).Maybe()
		mockRPC.On("BlockNumber", mock.Anything).Return(uint64(5), nil)
		mockRPC.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
		mockDB.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()

		// WipeAllData succeeds
		mockDB.On("WipeAllData", mock.Anything).Return(nil).Once()

		// After wipe, lastIndexed becomes 0, blocksToSync is 5 < catchupThreshold (100),
		// so it goes into startStandardMode. We need UpdateSyncStatus and the poll loop.
		mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDB.On("BackfillDailyStats", mock.Anything).Return(nil).Maybe()

		// Standard mode will poll; we just need to let it run briefly and cancel.
		mockRPC.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(newMockSubscription(), nil).Maybe()

		os.Setenv("FORCE_REINDEX", "true")
		defer os.Unsetenv("FORCE_REINDEX")

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := idx.Start(ctx)
		// Should exit with context.DeadlineExceeded (from the poll loop), not chain reset error
		assert.Error(t, err)
		assert.False(t, strings.Contains(err.Error(), "chain reset detected"),
			"should not return chain reset error when FORCE_REINDEX=true, got: %s", err.Error())

		mockDB.AssertCalled(t, "WipeAllData", mock.Anything)
	})

	t.Run("no chain reset when delta is within reorg depth", func(t *testing.T) {
		mockDB := new(MockDatabase)
		mockRPC := new(MockRPCClient)
		// Use NewWithConfig to disable catchup — this test only validates that
		// the chain reset detection does NOT fire for small deltas.
		cfg := &Config{
			RPCWorkers:         50,
			RPCRateLimit:       500,
			DBBatchSize:        500,
			BalanceWorkers:     0,
			EnableAsyncBalance: false,
			CatchupEnabled:     false,
		}
		idx := NewWithConfig(mockDB, mockRPC, time.Second, 0, cfg)

		// Chain head at 900, last indexed at 1000 — delta of 100, within maxReorgDepth (128)
		mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(1000), nil)
		mockDB.On("GetBlockCount", mock.Anything).Return(int64(1000), nil)
		mockDB.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(1000), nil).Maybe()
		mockRPC.On("BlockNumber", mock.Anything).Return(uint64(900), nil)
		mockRPC.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
		mockDB.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()
		mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDB.On("BackfillDailyStats", mock.Anything).Return(nil).Maybe()

		os.Unsetenv("FORCE_REINDEX")

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := idx.Start(ctx)
		// Should not fail with chain reset — it'll enter standard mode and time out
		assert.Error(t, err)
		assert.False(t, strings.Contains(err.Error(), "chain reset detected"),
			"should NOT detect chain reset for small delta, got: %s", err.Error())

		mockDB.AssertNotCalled(t, "WipeAllData", mock.Anything)
	})

	t.Run("realtime handleReorg rejects massive revert", func(t *testing.T) {
		mockDB := new(MockDatabase)
		mockRPC := new(MockRPCClient)

		cfg := &RealtimeConfig{
			ConfirmationBlocks: 1,
			PollInterval:       100 * time.Millisecond,
		}
		idxCfg := &Config{}

		rt := NewRealtimeIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)

		// lastIndexed=25000, fromBlock=5 → need to revert 24995 blocks
		mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(25000), nil)

		err := rt.handleReorg(context.Background(), 5)
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "chain reset detected"),
			"should detect chain reset in handleReorg, got: %s", err.Error())

		// Should NOT have deleted any blocks
		mockDB.AssertNotCalled(t, "DeleteBlock", mock.Anything, mock.Anything)
	})
}

func TestMissingBlockCount(t *testing.T) {
	tests := []struct {
		name       string
		startBlock uint64
		head       uint64
		count      int64
		want       int64
	}{
		{"from genesis, fully caught up", 0, 99, 100, 0},
		{"from genesis, holes behind the tip", 0, 99, 60, 40},
		// Without accounting for START_BLOCK this reports 10000 missing forever
		// on a database that is completely caught up.
		{"start_block offset, fully caught up", 10000, 10099, 100, 0},
		{"start_block offset, holes behind the tip", 10000, 10099, 90, 10},
		{"head below start_block", 10000, 500, 0, 0},
		{"count ahead of range never goes negative", 0, 10, 50, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := missingBlockCount(tc.startBlock, tc.head, tc.count); got != tc.want {
				t.Errorf("missingBlockCount(%d, %d, %d) = %d, want %d",
					tc.startBlock, tc.head, tc.count, got, tc.want)
			}
		})
	}
}

// The chain has not reached the configured start yet, so there is nothing to
// index and no reason to run a COUNT(*) over blocks.
func TestRefreshMissingBlocksSkipsQueryBelowStartBlock(t *testing.T) {
	mockDB := new(MockDatabase)
	idx := &Indexer{db: mockDB, startBlock: 10000}

	idx.refreshMissingBlocks(context.Background(), 500)

	mockDB.AssertNotCalled(t, "GetBlockCount", mock.Anything)
}

// The count must be bounded by the indexed range. A whole-table count lets rows
// outside [startBlock, head] cancel genuine holes, making the gauge read zero.
func TestRefreshMissingBlocksCountsOnlyTheIndexedRange(t *testing.T) {
	mockDB := new(MockDatabase)
	mockDB.On("GetBlockCountInRange", mock.Anything, uint64(10000), uint64(10099)).
		Return(int64(90), nil)

	idx := &Indexer{db: mockDB, startBlock: 10000}
	idx.refreshMissingBlocks(context.Background(), 10099)

	mockDB.AssertExpectations(t)
	mockDB.AssertNotCalled(t, "GetBlockCount", mock.Anything)
}
