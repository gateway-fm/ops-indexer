package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// PRST-5438: realtime read up to ten stored hashes back from the node before every block (detectReorg) and walked
// one block at a time, ~3.8 blocks/s in a load test against a chain at ~8.4. It now checks once per batch and reads
// ahead while it commits, each block checked against its parent; the cursor and Behind move with every block.
func TestRealtimeChecksForAReorgOncePerBatch(t *testing.T) {
	ctx := context.Background()
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	chain := chainOfEmptyBlocks(101, 120)
	for n, b := range chain {
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Once()
	}
	parent := common.BytesToHash(new(big.Int).SetUint64(1000 + 100).Bytes()).Hex()
	mockDB.On("GetBlock", mock.Anything, uint64(100)).Return(&types.Block{Number: 100, Hash: parent}, nil).Once()
	for n, b := range chain {
		mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: b.Hash.Hex()}, nil).Maybe()
	}
	mockDB.On("GetBlock", mock.Anything, mock.Anything).Return(nil, nil)
	mockRPC.On("RawBlockHash", mock.Anything, uint64(100)).Return(parent, nil).Once()
	rt := realtimeAt(mockDB, 100, 0)
	rt.rpc = mockRPC
	cursorAtInsert := map[uint64]uint64{}
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		b := args.Get(1).(*types.Block)
		cursorAtInsert[b.Number] = rt.GetLastProcessedBlock()
	}).Return(nil)
	mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rt.indexUpTo(ctx, 100, 120)
	assert.Equal(t, uint64(120), rt.GetLastProcessedBlock())
	assert.Zero(t, rt.behind.Load())
	mockRPC.AssertNumberOfCalls(t, "RawBlockHash", 1)
	mockRPC.AssertNumberOfCalls(t, "RawBlockByNumber", 20)
	mockDB.AssertNumberOfCalls(t, "InsertBlock", 20)
	for n := uint64(101); n <= 120; n++ {
		assert.Equal(t, n-1, cursorAtInsert[n], "the cursor moves with every block, not at the end of the batch")
	}
}

// PRST-5320 with PRST-5438: realtime checks for a reorg once per batch, and that one check still reverts every
// mismatching block below the head, not only the head. Blocks 98-100 are stale: all three are deleted and the
// batch resumes at 97.
func TestRealtimeBatchRevertsADepthThreeReorg(t *testing.T) {
	ctx := context.Background()
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	for n := uint64(91); n <= 100; n++ {
		stored := fmt.Sprintf("0x%064x", 1000+n)
		chain := stored
		if n >= 98 {
			stored = fmt.Sprintf("0x%064x", 9000+n)
		}
		mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: stored}, nil).Maybe()
		mockRPC.On("RawBlockHash", mock.Anything, n).Return(chain, nil).Maybe()
	}
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(100), nil)
	var deleted []uint64
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		deleted = append(deleted, args.Get(1).(uint64))
	}).Return(nil)

	rt := realtimeAt(mockDB, 100, 0)
	rt.rpc = mockRPC
	rt.indexUpTo(ctx, 100, 120)
	assert.Equal(t, []uint64{100, 99, 98}, deleted, "every stale block, not only the head")
	assert.Equal(t, uint64(97), rt.GetLastProcessedBlock())
	mockRPC.AssertNotCalled(t, "RawBlockByNumber", mock.Anything, mock.Anything)
}

// When the revert fails, realtime stays on the stale block and retries. Moving the cursor below the fork
// would hide the reorg from the next check, and the new fork's blocks would then be dropped as already stored.
func TestRealtimeKeepsItsCursorWhenTheRevertFails(t *testing.T) {
	ctx := context.Background()
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	for n := uint64(91); n <= 100; n++ {
		stored := fmt.Sprintf("0x%064x", 1000+n)
		chain := stored
		if n >= 98 {
			stored = fmt.Sprintf("0x%064x", 9000+n)
		}
		mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: stored}, nil).Maybe()
		mockRPC.On("RawBlockHash", mock.Anything, n).Return(chain, nil).Maybe()
	}
	mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(100), nil)
	mockDB.On("DeleteBlock", mock.Anything, mock.Anything).Return(errors.New("daily_stats_invalidate: day partly pruned"))

	rt := realtimeAt(mockDB, 100, 0)
	rt.rpc = mockRPC
	rt.indexUpTo(ctx, 100, 120)
	assert.Equal(t, uint64(100), rt.GetLastProcessedBlock(), "the cursor stays on the stale block until the revert succeeds")
	mockRPC.AssertNotCalled(t, "RawBlockByNumber", mock.Anything, mock.Anything)
}
