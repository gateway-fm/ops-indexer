package indexer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// staleHeadChain stores blocks 1..100 and serves the chain's hashes; the stored hashes of the top stale blocks differ.
func staleHeadChain(stale uint64) (*MockDatabase, *MockRPCClient) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	for n := uint64(1); n <= 100; n++ {
		chain := fmt.Sprintf("0x%064x", 1000+n)
		stored := chain
		if n > 100-stale {
			stored = fmt.Sprintf("0x%064x", 9000+n)
		}
		mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: stored}, nil).Maybe()
		mockRPC.On("RawBlockHash", mock.Anything, n).Return(chain, nil).Maybe()
	}
	return mockDB, mockRPC
}

// A stored head on a stale fork (a reorg while the indexer was down, or a revert that kept failing until
// a restart) is reverted before the start mode is chosen, however deep within the reorg limit (here past detectReorg's
// ten-block window): collector mode starts
// realtime at the node head, and nothing below it would ever compare the stale blocks again.
func TestStartRevertsAStaleStoredHead(t *testing.T) {
	ctx := context.Background()
	mockDB, mockRPC := staleHeadChain(13)
	// 88..100 no longer match; 87 does: the 13 blocks are measured first, then reverted in one go.
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(100), nil).Once()
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(nil)
	idx := newPrefetchIndexer(mockDB, mockRPC)

	head, _, err := idx.revertStaleHead(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(87), head)
	mockDB.AssertNumberOfCalls(t, "DeleteBlock", 13)
	mockDB.AssertNotCalled(t, "DeleteBlock", mock.Anything, uint64(87))
}

// A stale head that cannot be reverted refuses the start, instead of starting on top of it.
func TestStartRefusesAStaleStoredHeadItCannotRevert(t *testing.T) {
	mockDB, mockRPC := staleHeadChain(3)
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(100), nil).Maybe()
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(errors.New("daily_stats_invalidate: day partly pruned"))
	idx := newPrefetchIndexer(mockDB, mockRPC)

	_, _, err := idx.revertStaleHead(context.Background(), 100)
	require.ErrorContains(t, err, "stale fork")
}

// Start itself reverts the stale head before it chooses a mode.
func TestStartChecksTheStoredHeadBeforeChoosingAMode(t *testing.T) {
	mockDB, mockRPC := staleHeadChain(3)
	idx := NewWithConfig(mockDB, mockRPC, time.Second, 0, &Config{RPCWorkers: 4, RPCRateLimit: 100, DBBatchSize: 100})
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(100), nil)
	mockDB.On("GetBlockCount", mock.Anything).Return(int64(100), nil)
	mockDB.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(100), nil).Maybe()
	mockDB.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(errors.New("refused"))
	mockRPC.On("BlockNumber", mock.Anything).Return(uint64(100), nil)
	mockRPC.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()

	err := idx.Start(context.Background())
	require.ErrorContains(t, err, "stale fork")
	mockDB.AssertCalled(t, "DeleteBlock", mock.Anything, uint64(100))
}

// In the standard loop, a revert that fails ends the tick on the stale block. Indexing on would write the new fork on
// top of the stale blocks; only the stored-head check of the first prefetched block happened to stop it.
func TestStandardTickStaysOnTheStaleBlockWhenTheRevertFails(t *testing.T) {
	mockDB, mockRPC := staleHeadChain(3)
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(100), nil).Maybe()
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(errors.New("refused"))
	mockRPC.On("RawBlockByNumber", mock.Anything, mock.Anything).Return(nil, errors.New("not read")).Maybe()
	idx := newPrefetchIndexer(mockDB, mockRPC)

	require.Equal(t, uint64(100), idx.standardTick(context.Background(), 100, 120, &receiptSkip{}))
	mockRPC.AssertNotCalled(t, "RawBlockByNumber", mock.Anything, mock.Anything)
}

// A devnet reset whose new chain already reached the old height looks like a stale fork deeper than any
// reorg, not like a reset. The start is refused with the advice to set FORCE_REINDEX, and with it set it wipes.
func TestStartWipesAStaleForkDeeperThanAnyReorgWithForceReindex(t *testing.T) {
	start := func(force bool) (*MockDatabase, error) {
		mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
		for n := uint64(1); n <= 300; n++ {
			mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: fmt.Sprintf("0x%064x", 9000+n)}, nil).Maybe()
			mockRPC.On("RawBlockHash", mock.Anything, n).Return(fmt.Sprintf("0x%064x", 1000+n), nil).Maybe()
		}
		mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(300), nil)
		mockDB.On("GetBlockCount", mock.Anything).Return(int64(300), nil)
		mockDB.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(300), nil).Maybe()
		mockDB.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()
		mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDB.On("WipeAllData", mock.Anything).Return(nil).Maybe()
		mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockRPC.On("BlockNumber", mock.Anything).Return(uint64(305), nil)
		mockRPC.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
		idx := NewWithConfig(mockDB, mockRPC, time.Second, 0, &Config{RPCWorkers: 4, RPCRateLimit: 100, DBBatchSize: 100})
		if force {
			t.Setenv("FORCE_REINDEX", "true")
		} else {
			t.Setenv("FORCE_REINDEX", "")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		return mockDB, idx.Start(ctx)
	}

	mockDB, err := start(false)
	require.ErrorContains(t, err, "FORCE_REINDEX=true")
	mockDB.AssertNotCalled(t, "WipeAllData", mock.Anything)

	mockDB, err = start(true)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the start goes on after the wipe")
	mockDB.AssertCalled(t, "WipeAllData", mock.Anything)
}

// A stale fork deeper than any reorg (a reset chain, or RPC_URL pointing at another chain) is refused
// before anything is deleted. Reverting ten blocks a round down to the limit first cost ~122 stored blocks.
func TestStartRefusesATooDeepStaleForkWithoutDeleting(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	for n := uint64(1); n <= 300; n++ {
		mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: fmt.Sprintf("0x%064x", 9000+n)}, nil).Maybe()
		mockRPC.On("RawBlockHash", mock.Anything, n).Return(fmt.Sprintf("0x%064x", 1000+n), nil).Maybe()
	}
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(300), nil).Maybe()
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
	idx := newPrefetchIndexer(mockDB, mockRPC)

	_, _, err := idx.revertStaleHead(context.Background(), 300)
	require.ErrorIs(t, err, errStaleForkTooDeep)
	mockDB.AssertNotCalled(t, "DeleteBlock", mock.Anything, mock.Anything)
}

// A stale block under a matching head (a partial revert left by an earlier build) is reverted at start
// too: the check reads the whole ten-block window, as the per-tick check does, and reverts down to the deepest
// mismatch.
func TestStartRevertsAStaleBlockUnderAMatchingHead(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	for n := uint64(1); n <= 100; n++ {
		chain := fmt.Sprintf("0x%064x", 1000+n)
		stored := chain
		if n == 97 {
			stored = fmt.Sprintf("0x%064x", 9000+n)
		}
		mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: stored}, nil).Maybe()
		mockRPC.On("RawBlockHash", mock.Anything, n).Return(chain, nil).Maybe()
	}
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(100), nil).Maybe()
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(nil)
	idx := newPrefetchIndexer(mockDB, mockRPC)

	head, _, err := idx.revertStaleHead(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, uint64(96), head)
	mockDB.AssertNumberOfCalls(t, "DeleteBlock", 4)
}
