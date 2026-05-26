package indexer

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockSubscription struct {
	errCh chan error
}

func newMockSubscription() *mockSubscription {
	return &mockSubscription{errCh: make(chan error, 1)}
}

func (s *mockSubscription) Unsubscribe() {}
func (s *mockSubscription) Err() <-chan error { return s.errCh }

func newTestCatchupIndexer(mockDB *MockDatabase, mockRPC *MockRPCClient) *CatchupIndexer {
	cfg := &CatchupConfig{
		Workers:   1,
		BatchSize: 100,
		QueueSize: 100,
	}
	idxCfg := &Config{
		RPCWorkers:   1,
		RPCRateLimit: 100,
	}
	return NewCatchupIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)
}

func makeRawBlock(number uint64) *rpc.RawBlock {
	return &rpc.RawBlock{
		Number:    (*hexutil.Big)(big.NewInt(int64(number))),
		Timestamp: hexutil.Uint64(time.Now().Unix()),
	}
}

func TestCatchupIndexer_WorkerProcessesBlock(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	catchup := newTestCatchupIndexer(mockDB, mockRPC)

	blockNum := uint64(5)

	mockDB.On("HasBlock", mock.Anything, blockNum).Return(false, nil).Once()

	rawBlock := makeRawBlock(blockNum)

	mockRPC.On("RawBlockByNumber", mock.Anything, blockNum).Return(rawBlock, nil)
	mockDB.On("InsertBlock", mock.Anything, mock.MatchedBy(func(b *types.Block) bool {
		return b.Number == blockNum
	})).Return(nil)
	mockDB.On("InsertBlockDataBatch", mock.Anything, mock.Anything).Return(nil).Maybe()

	mockDB.On("DeleteMissingRangeByBlock", mock.Anything, blockNum).Return(nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	catchup.ctx = ctx
	catchup.cancel = cancel

	go func() {
		catchup.workQueue <- blockNum
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	catchup.wg.Add(1)
	catchup.worker(0)

	mockDB.AssertExpectations(t)
	mockRPC.AssertExpectations(t)
}

func TestCatchupIndexer_WorkerSkipsExistingBlock(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	catchup := newTestCatchupIndexer(mockDB, mockRPC)

	blockNum := uint64(5)

	mockDB.On("HasBlock", mock.Anything, blockNum).Return(true, nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	catchup.ctx = ctx
	catchup.cancel = cancel

	go func() {
		catchup.workQueue <- blockNum
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	catchup.wg.Add(1)
	catchup.worker(0)

	assert.Equal(t, int64(1), atomic.LoadInt64(&catchup.processedBlocks))

	mockRPC.AssertNotCalled(t, "RawBlockByNumber", mock.Anything, mock.Anything)
	mockDB.AssertExpectations(t)
}

func TestCatchupIndexer_WorkerSkipsExistingBlock_WithCollector(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	catchup := newTestCatchupIndexer(mockDB, mockRPC)

	collectorDB := new(MockDatabase)
	collectorRPC := new(MockRPCClient)
	collector := NewMissingRangeCollector(collectorDB, collectorRPC, nil)
	catchup.collector = collector

	blockNum := uint64(5)

	mockDB.On("HasBlock", mock.Anything, blockNum).Return(true, nil).Once()

	collectorDB.On("DeleteMissingRangeByBlock", mock.Anything, blockNum).Return(nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	catchup.ctx = ctx
	catchup.cancel = cancel

	go func() {
		catchup.workQueue <- blockNum
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	catchup.wg.Add(1)
	catchup.worker(0)

	assert.Equal(t, int64(1), atomic.LoadInt64(&catchup.processedBlocks))
	mockRPC.AssertNotCalled(t, "RawBlockByNumber", mock.Anything, mock.Anything)
	collectorDB.AssertCalled(t, "DeleteMissingRangeByBlock", mock.Anything, blockNum)
}

func TestCatchupIndexer_WorkerRetriesOnError(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	catchup := newTestCatchupIndexer(mockDB, mockRPC)

	blockNum := uint64(5)

	mockDB.On("HasBlock", mock.Anything, blockNum).Return(false, nil).Once()

	mockRPC.On("RawBlockByNumber", mock.Anything, blockNum).Return(nil, fmt.Errorf("connection refused")).Once()

	mockDB.On("RequeueMissingBlock", mock.Anything, blockNum).Return(nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	catchup.ctx = ctx
	catchup.cancel = cancel

	go func() {
		catchup.workQueue <- blockNum
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	catchup.wg.Add(1)
	catchup.worker(0)

	assert.Equal(t, int64(0), atomic.LoadInt64(&catchup.processedBlocks))

	mockDB.AssertNotCalled(t, "DeleteMissingRangeByBlock", mock.Anything, mock.Anything)

	mockDB.AssertCalled(t, "RequeueMissingBlock", mock.Anything, blockNum)
}

func TestCatchupIndexer_BlockProducer_QueuesAllBlocksInRange(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &CatchupConfig{
		Workers:   2,
		BatchSize: 100,
		QueueSize: 100,
	}
	idxCfg := &Config{RPCWorkers: 1, RPCRateLimit: 100}
	catchup := NewCatchupIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)

	collectorDB := new(MockDatabase)
	collectorRPC := new(MockRPCClient)
	collector := NewMissingRangeCollector(collectorDB, collectorRPC, nil)
	catchup.collector = collector

	collectorDB.On("GetMissingRangesBatch", mock.Anything, 100).
		Return([]db.BlockRange{{FromNumber: 1, ToNumber: 9}}, nil).Once()
	collectorDB.On("GetMissingRangesBatch", mock.Anything, 100).
		Return([]db.BlockRange{}, nil)

	for blockNum := uint64(1); blockNum <= 9; blockNum++ {
		mockDB.On("HasBlock", mock.Anything, blockNum).Return(false, nil).Once()

		rawBlock := makeRawBlock(blockNum)

		mockRPC.On("RawBlockByNumber", mock.Anything, blockNum).Return(rawBlock, nil)
		mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDB.On("InsertBlockDataBatch", mock.Anything, mock.Anything).Return(nil).Maybe()

		collectorDB.On("DeleteMissingRangeByBlock", mock.Anything, blockNum).Return(nil).Once()
	}

	collectorDB.On("GetTotalMissingBlocks", mock.Anything).Return(int64(9), nil).Maybe()

	mockDB.On("RebuildAddressStats", mock.Anything).Return(nil).Maybe()

	err := catchup.Start(context.Background(), 1, 9)
	assert.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&catchup.processedBlocks) >= 9 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	processed := atomic.LoadInt64(&catchup.processedBlocks)
	assert.Equal(t, int64(9), processed, "all 9 blocks should be processed")

	catchup.Stop()

	for blockNum := uint64(1); blockNum <= 9; blockNum++ {
		collectorDB.AssertCalled(t, "DeleteMissingRangeByBlock", mock.Anything, blockNum)
	}
}

func TestCatchupIndexer_BlockProducer_MultipleRanges(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &CatchupConfig{
		Workers:   2,
		BatchSize: 100,
		QueueSize: 200,
	}
	idxCfg := &Config{RPCWorkers: 1, RPCRateLimit: 100}
	catchup := NewCatchupIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)

	collectorDB := new(MockDatabase)
	collectorRPC := new(MockRPCClient)
	collector := NewMissingRangeCollector(collectorDB, collectorRPC, nil)
	catchup.collector = collector

	collectorDB.On("GetMissingRangesBatch", mock.Anything, 100).
		Return([]db.BlockRange{
			{FromNumber: 100, ToNumber: 110},
			{FromNumber: 1, ToNumber: 9},
		}, nil).Once()
	collectorDB.On("GetMissingRangesBatch", mock.Anything, 100).
		Return([]db.BlockRange{}, nil)

	allBlocks := make([]uint64, 0)
	for b := uint64(100); b <= 110; b++ {
		allBlocks = append(allBlocks, b)
	}
	for b := uint64(1); b <= 9; b++ {
		allBlocks = append(allBlocks, b)
	}

	for _, blockNum := range allBlocks {
		mockDB.On("HasBlock", mock.Anything, blockNum).Return(false, nil).Once()

		rawBlock := makeRawBlock(blockNum)

		mockRPC.On("RawBlockByNumber", mock.Anything, blockNum).Return(rawBlock, nil)
		mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDB.On("InsertBlockDataBatch", mock.Anything, mock.Anything).Return(nil).Maybe()
		collectorDB.On("DeleteMissingRangeByBlock", mock.Anything, blockNum).Return(nil).Once()
	}

	collectorDB.On("GetTotalMissingBlocks", mock.Anything).Return(int64(20), nil).Maybe()
	mockDB.On("RebuildAddressStats", mock.Anything).Return(nil).Maybe()

	totalExpected := int64(11 + 9)

	err := catchup.Start(context.Background(), 1, 110)
	assert.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&catchup.processedBlocks) >= totalExpected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	processed := atomic.LoadInt64(&catchup.processedBlocks)
	assert.Equal(t, totalExpected, processed, "all 20 blocks should be processed")

	catchup.Stop()
}

func TestCatchupIndexer_BlockProducer_GoesIdleWhenNoRanges(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &CatchupConfig{
		Workers:   1,
		BatchSize: 100,
		QueueSize: 100,
	}
	idxCfg := &Config{RPCWorkers: 1, RPCRateLimit: 100}
	catchup := NewCatchupIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)

	mockDB.On("GetMissingRangesBatch", mock.Anything, 100).Return([]db.BlockRange{}, nil)
	mockDB.On("GetTotalMissingBlocks", mock.Anything).Return(int64(0), nil).Maybe()

	completeCalled := make(chan struct{}, 1)
	catchup.SetOnComplete(func() {
		select {
		case completeCalled <- struct{}{}:
		default:
		}
	})

	err := catchup.Start(context.Background(), 0, 0)
	assert.NoError(t, err)

	select {
	case <-completeCalled:
		t.Fatal("onComplete should not be called when no blocks were processed")
	case <-time.After(200 * time.Millisecond):
	}

	catchup.Stop()
}

// scriptedCollectorDB feeds the catchup producer a controlled range sequence:
// block 1, then empties, then (once releaseBlock2 is set) one new head block,
// then empties forever — counting the idle polls that follow block 2. This
// avoids coupling the test to magic poll counts or wall-clock timing.
type scriptedCollectorDB struct {
	*MockDatabase
	block1Sent     atomic.Bool
	releaseBlock2  atomic.Bool
	block2Sent     atomic.Bool
	idlePollsAfter atomic.Int64
}

func (s *scriptedCollectorDB) GetMissingRangesBatch(ctx context.Context, batchSize int) ([]db.BlockRange, error) {
	if !s.block1Sent.Swap(true) {
		return []db.BlockRange{{FromNumber: 1, ToNumber: 1}}, nil
	}
	if s.releaseBlock2.Load() && !s.block2Sent.Swap(true) {
		return []db.BlockRange{{FromNumber: 2, ToNumber: 2}}, nil
	}
	if s.block2Sent.Load() {
		s.idlePollsAfter.Add(1)
	}
	return nil, nil
}

func TestCatchupIndexer_RebuildAddressStats_FiresOncePerProcess(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &CatchupConfig{Workers: 1, BatchSize: 100, QueueSize: 100}
	idxCfg := &Config{RPCWorkers: 1, RPCRateLimit: 100}
	catchup := NewCatchupIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)
	catchup.pollInterval = 2 * time.Millisecond

	scripted := &scriptedCollectorDB{MockDatabase: new(MockDatabase)}
	collector := NewMissingRangeCollector(scripted, new(MockRPCClient), nil)
	catchup.collector = collector

	rebuilt := make(chan struct{}, 8)
	catchup.SetOnComplete(func() { rebuilt <- struct{}{} })

	for _, blockNum := range []uint64{1, 2} {
		mockDB.On("HasBlock", mock.Anything, blockNum).Return(false, nil)
		mockRPC.On("RawBlockByNumber", mock.Anything, blockNum).Return(makeRawBlock(blockNum), nil)
		scripted.On("DeleteMissingRangeByBlock", mock.Anything, blockNum).Return(nil)
	}
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("InsertBlockDataBatch", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("RebuildAddressStats", mock.Anything).Return(nil)
	scripted.On("GetTotalMissingBlocks", mock.Anything).Return(int64(0), nil).Maybe()
	mockDB.On("GetTotalMissingBlocks", mock.Anything).Return(int64(0), nil).Maybe()

	require.NoError(t, catchup.Start(context.Background(), 1, 2))
	defer catchup.Stop()

	// Initial-sync rebuild must fire once; sync on the completion signal, not a
	// wall-clock sleep.
	select {
	case <-rebuilt:
	case <-time.After(5 * time.Second):
		t.Fatal("initial address_stats rebuild did not fire")
	}

	// A steady-state chain-head block now arrives after initial sync.
	scripted.releaseBlock2.Store(true)

	// Wait until the producer has idled several cycles past block 2 — enough that
	// a latch-resetting (buggy) implementation would have re-fired the rebuild.
	require.Eventually(t, func() bool { return scripted.idlePollsAfter.Load() >= 5 },
		5*time.Second, 2*time.Millisecond)

	// HasBlock=false, so processedBlocks only advances via real processing —
	// guards against a vacuous pass where blocks were never indexed.
	require.Eventually(t, func() bool { return atomic.LoadInt64(&catchup.processedBlocks) >= 2 },
		5*time.Second, 2*time.Millisecond)

	mockDB.AssertNumberOfCalls(t, "RebuildAddressStats", 1)
}

// --- Realtime Indexer Tests ---

func TestRealtimeIndexer_DetectReorg_BlockNotInDB_Continues(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &RealtimeConfig{
		ConfirmationBlocks: 1,
		PollInterval:       100 * time.Millisecond,
	}
	idxCfg := &Config{}

	rt := NewRealtimeIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)

	blockNum := uint64(50)

	mockDB.On("GetBlock", mock.Anything, mock.Anything).Return(nil, nil)

	reorgDepth, err := rt.detectReorg(context.Background(), blockNum)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), reorgDepth, "no reorg should be detected when blocks are not in DB")
}

func TestRealtimeIndexer_DetectReorg_RPCError_DoesNotTriggerDeletion(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &RealtimeConfig{
		ConfirmationBlocks: 1,
		PollInterval:       100 * time.Millisecond,
	}
	idxCfg := &Config{}

	rt := NewRealtimeIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)

	blockNum := uint64(50)

	mockDB.On("GetBlock", mock.Anything, blockNum).Return(&types.Block{
		Number: blockNum,
		Hash:   "0xabc123def456789a",
	}, nil).Once()

	mockRPC.On("RawBlockHash", mock.Anything, blockNum).Return("", fmt.Errorf("connection refused")).Once()

	reorgDepth, err := rt.detectReorg(context.Background(), blockNum)

	assert.Error(t, err)
	assert.Equal(t, uint64(0), reorgDepth)

	mockDB.AssertNotCalled(t, "DeleteBlock", mock.Anything, mock.Anything)
}

func TestRealtimeIndexer_ProcessingLoop_SkipsBlock0(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &RealtimeConfig{
		ConfirmationBlocks: 1,
		PollInterval:       50 * time.Millisecond,
	}
	idxCfg := &Config{}

	rt := NewRealtimeIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(0)

	mockRPC.On("BlockNumber", mock.Anything).Return(uint64(10), nil)

	for blockNum := uint64(1); blockNum <= 9; blockNum++ {
		mockDB.On("GetBlock", mock.Anything, blockNum-1).Return(nil, nil).Maybe()

		rawBlock := makeRawBlock(blockNum)

		mockRPC.On("RawBlockByNumber", mock.Anything, blockNum).Return(rawBlock, nil)
		mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDB.On("InsertBlockDataBatch", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	}

	mockRPC.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(newMockSubscription(), fmt.Errorf("not supported"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go rt.Start(ctx, 0)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if rt.GetLastProcessedBlock() >= 9 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mockRPC.AssertNotCalled(t, "RawBlockByNumber", mock.Anything, uint64(0))

	lastProcessed := rt.GetLastProcessedBlock()
	assert.GreaterOrEqual(t, lastProcessed, uint64(9),
		"blocks 1-9 should have been processed, but lastProcessed=%d", lastProcessed)

	rt.Stop()
}

func TestRealtimeIndexer_ReorgError_DoesNotDeleteBlocks(t *testing.T) {
	mockDB := new(MockDatabase)
	mockRPC := new(MockRPCClient)

	cfg := &RealtimeConfig{
		ConfirmationBlocks: 1,
		PollInterval:       50 * time.Millisecond,
	}
	idxCfg := &Config{}

	rt := NewRealtimeIndexer(mockDB, mockRPC, cfg, idxCfg, NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(4)

	mockRPC.On("BlockNumber", mock.Anything).Return(uint64(6), nil)

	mockDB.On("GetBlock", mock.Anything, uint64(4)).Return(&types.Block{
		Number: 4,
		Hash:   "0xstoredblock4hash",
	}, nil)
	mockRPC.On("RawBlockHash", mock.Anything, uint64(4)).Return("", fmt.Errorf("RPC unavailable")).Once()

	rawBlock5 := makeRawBlock(5)

	mockRPC.On("RawBlockByNumber", mock.Anything, uint64(5)).Return(rawBlock5, nil)
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("InsertBlockDataBatch", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	mockRPC.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(newMockSubscription(), fmt.Errorf("not supported"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go rt.Start(ctx, 4)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if rt.GetLastProcessedBlock() >= 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mockDB.AssertNotCalled(t, "DeleteBlock", mock.Anything, mock.Anything)

	assert.GreaterOrEqual(t, rt.GetLastProcessedBlock(), uint64(5))

	rt.Stop()
}
