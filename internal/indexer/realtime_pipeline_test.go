package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
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

// With COMMIT_PIPELINE above 1, realtime commits through the pipeline as the standard loop does. It used
// to commit one block at a time whatever the setting, ~2.8 blocks/s on 1 ms storage, so a deployment that came back
// behind in catch-up mode jumped and never caught up.
func TestRealtimeUsesTheCommitPipeline(t *testing.T) {
	ctx := context.Background()
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	chain := chainOfEmptyBlocks(101, 104)
	receipts := map[common.Hash]*rpclient.Receipt{}
	for n, b := range chain {
		h := common.BytesToHash(new(big.Int).SetUint64(5000 + n).Bytes())
		b.Transactions = []rpc.RawTransaction{{Hash: h}}
		receipts[h] = &rpclient.Receipt{Status: 1, TxHash: h, BlockNumber: new(big.Int).SetUint64(n)}
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Maybe()
	}
	mockRPC.On("FetchReceiptsBatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(receipts, nil).Maybe()
	parent := common.BytesToHash(new(big.Int).SetUint64(1000 + 100).Bytes()).Hex()
	mockDB.On("GetBlock", mock.Anything, uint64(100)).Return(&types.Block{Number: 100, Hash: parent}, nil).Maybe()
	mockDB.On("GetBlock", mock.Anything, mock.Anything).Return(nil, nil)
	mockRPC.On("RawBlockHash", mock.Anything, uint64(100)).Return(parent, nil).Maybe()
	mockDB.On("BeginBlockData", mock.Anything, mock.Anything).Return(nil, errors.New("stop after the first begin")).Maybe()
	mockDB.On("InsertBlockDataBatch", mock.Anything, mock.Anything).Return(errors.New("serial commit")).Maybe()
	mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	rt := realtimeAt(mockDB, 100, 0)
	rt.rpc = mockRPC
	rt.idxCfg.CommitPipelineDepth = 4
	rt.indexUpTo(ctx, 100, 104)
	mockDB.AssertCalled(t, "BeginBlockData", mock.Anything, mock.Anything)
	mockDB.AssertNotCalled(t, "InsertBlockDataBatch", mock.Anything, mock.Anything)
}

// Through the pipeline too, realtime's cursor moves with every committed block, as catch-up's ceiling and
// the jump read it.
func TestRealtimeCursorMovesPerBlockThroughThePipeline(t *testing.T) {
	ctx := context.Background()
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	chain := chainOfEmptyBlocks(101, 110)
	for n, b := range chain {
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Maybe()
	}
	parent := common.BytesToHash(new(big.Int).SetUint64(1000 + 100).Bytes()).Hex()
	mockDB.On("GetBlock", mock.Anything, uint64(100)).Return(&types.Block{Number: 100, Hash: parent}, nil).Maybe()
	mockDB.On("GetBlock", mock.Anything, mock.Anything).Return(nil, nil)
	mockRPC.On("RawBlockHash", mock.Anything, uint64(100)).Return(parent, nil).Maybe()
	rt := realtimeAt(mockDB, 100, 0)
	rt.rpc = mockRPC
	rt.idxCfg.CommitPipelineDepth = 4
	cursorAtInsert := map[uint64]uint64{}
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		cursorAtInsert[args.Get(1).(*types.Block).Number] = rt.GetLastProcessedBlock()
	}).Return(nil)
	mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rt.indexUpTo(ctx, 100, 110)
	assert.Equal(t, uint64(110), rt.GetLastProcessedBlock())
	for n := uint64(101); n <= 110; n++ {
		assert.Equal(t, n-1, cursorAtInsert[n], "the cursor moves with every block")
	}
}

// While a revert keeps failing, realtime does not jump to the head however far behind it falls. The jump
// moves the cursor past the stale fork, which catch-up never compares, so the fork would stay stored after all.
func TestRealtimeDoesNotJumpWhileARevertIsPending(t *testing.T) {
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

	mockDB.On("SaveMissingRanges", mock.Anything, mock.Anything).Return(nil).Maybe()

	rt := realtimeAt(mockDB, 100, 1_000)
	rt.rpc = mockRPC
	rt.indexUpTo(ctx, rt.jumpIfFarBehind(ctx, 120), 120)
	assert.Equal(t, uint64(100), rt.jumpIfFarBehind(ctx, 2_100), "no jump while the revert is pending")
	assert.Equal(t, uint64(100), rt.GetLastProcessedBlock())
	mockDB.AssertNotCalled(t, "SaveMissingRanges", mock.Anything, mock.Anything)
}

// Realtime does not jump past a stored head the node has replaced. The jump moves the cursor above it,
// and catch-up never compares it, so the stale block would stay stored.
func TestRealtimeDoesNotJumpPastAReorgedHead(t *testing.T) {
	ctx := context.Background()
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	for n := uint64(91); n <= 100; n++ {
		stored := fmt.Sprintf("0x%064x", 1000+n)
		chain := stored
		if n == 100 {
			chain = fmt.Sprintf("0x%064x", 9000+n)
		}
		mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: stored}, nil).Maybe()
		mockRPC.On("RawBlockHash", mock.Anything, n).Return(chain, nil).Maybe()
	}
	mockDB.On("SaveMissingRanges", mock.Anything, mock.Anything).Return(nil).Maybe()

	rt := realtimeAt(mockDB, 100, 1_000)
	rt.rpc = mockRPC
	assert.Equal(t, uint64(100), rt.jumpIfFarBehind(ctx, 2_100), "no jump over a reorged head")
	mockDB.AssertNotCalled(t, "SaveMissingRanges", mock.Anything, mock.Anything)
}
