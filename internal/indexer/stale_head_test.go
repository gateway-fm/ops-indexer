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
// a restart) is reverted before the start mode is chosen, however deep within the reorg limit: collector mode starts
// realtime at the node head, and nothing below it would ever compare the stale blocks again.
func TestStartRevertsAStaleStoredHead(t *testing.T) {
	ctx := context.Background()
	mockDB, mockRPC := staleHeadChain(13)
	// The check covers ten blocks: 91..100 are reverted first, then 88..90 below them.
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(100), nil).Once()
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(90), nil).Once()
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(nil)
	idx := newPrefetchIndexer(mockDB, mockRPC)

	head, err := idx.revertStaleHead(ctx, 100)
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

	_, err := idx.revertStaleHead(context.Background(), 100)
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
