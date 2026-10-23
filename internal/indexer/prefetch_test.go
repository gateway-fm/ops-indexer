package indexer

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

func chainOfEmptyBlocks(from, to uint64) map[uint64]*rpc.RawBlock {
	blocks := make(map[uint64]*rpc.RawBlock)
	for n := from; n <= to; n++ {
		blocks[n] = &rpc.RawBlock{
			Number:     (*hexutil.Big)(new(big.Int).SetUint64(n)),
			Hash:       common.BytesToHash(new(big.Int).SetUint64(1000 + n).Bytes()),
			ParentHash: common.BytesToHash(new(big.Int).SetUint64(1000 + n - 1).Bytes()),
		}
	}
	return blocks
}

func newPrefetchIndexer(mockDB *MockDatabase, mockRPC *MockRPCClient) *Indexer {
	return NewWithConfig(mockDB, mockRPC, time.Second, 0, &Config{
		RPCWorkers: 50, RPCRateLimit: 500, DBBatchSize: 500, CatchupEnabled: false,
	})
}

func TestPrefetchBlocksDeliversInBlockOrderWhenReadsFinishOutOfOrder(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	blocks := chainOfEmptyBlocks(1, 12)
	for n, b := range blocks {
		mockRPC.On("RawBlockByNumber", mock.Anything, n).After(time.Duration(13-n)*5*time.Millisecond).Return(b, nil)
	}
	idx := newPrefetchIndexer(mockDB, mockRPC)

	var got []uint64
	for slot := range idx.prefetchBlocks(context.Background(), 1, 12, 4) {
		b := <-slot
		if b.err != nil {
			t.Fatalf("block %d: %v", b.number, b.err)
		}
		got = append(got, b.raw.NumberU64())
	}
	for i, n := range got {
		if n != uint64(i+1) {
			t.Fatalf("delivered %v, want 1..12 in order", got)
		}
	}
	if len(got) != 12 {
		t.Fatalf("delivered %d blocks, want 12", len(got))
	}
}

func TestIndexPrefetchedStopsBeforeABlockThatDoesNotExtendTheLastOne(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	blocks := chainOfEmptyBlocks(1, 6)
	blocks[4].ParentHash = common.BytesToHash(big.NewInt(42).Bytes())
	for n, b := range blocks {
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Maybe()
	}
	var inserted []uint64
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		inserted = append(inserted, args.Get(1).(*types.Block).Number)
	}).Return(nil)
	mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	idx := newPrefetchIndexer(mockDB, mockRPC)

	last := idx.indexPrefetched(context.Background(), 0, 6, &receiptSkip{})

	if last != 3 {
		t.Fatalf("last indexed %d, want 3: block 4's parent is not block 3", last)
	}
	if len(inserted) != 3 || inserted[0] != 1 || inserted[2] != 3 {
		t.Fatalf("inserted %v, want [1 2 3]", inserted)
	}
}

// The prefetching loop moves past a block whose receipts never come once it is recorded as missing, like the
// sequential loop it replaced (receipt_skip.go); until then it stops at that block on every tick.
func TestIndexPrefetchedSkipsABlockWhoseReceiptsNeverCome(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	blocks := chainOfEmptyBlocks(1, 6)
	blocks[3].Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash([]byte{3})}}
	for n, b := range blocks {
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Maybe()
	}
	mockRPC.On("FetchReceiptsBatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(map[common.Hash]*rpclient.Receipt(nil), errors.New("receipts not found"))
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("RequeueMissingBlock", mock.Anything, uint64(3)).Return(nil).Once()
	mockDB.On("GetBlock", mock.Anything, uint64(2)).Return(&types.Block{Number: 2, Hash: blocks[2].Hash.Hex()}, nil)
	idx := newPrefetchIndexer(mockDB, mockRPC)

	var skip receiptSkip
	for attempt := 1; attempt < maxReceiptAttempts; attempt++ {
		if last := idx.indexPrefetched(context.Background(), 2, 6, &skip); last != 2 {
			t.Fatalf("attempt %d: last indexed %d, want 2 (stopped before block 3)", attempt, last)
		}
	}
	if last := idx.indexPrefetched(context.Background(), 2, 6, &skip); last != 6 {
		t.Fatalf("after %d attempts: last indexed %d, want 6 (block 3 recorded as missing and skipped)", maxReceiptAttempts, last)
	}
	mockDB.AssertExpectations(t)
}

// The pipelined loop moves past such a block too, after committing the blocks before it.
func TestIndexPipelinedSkipsABlockWhoseReceiptsNeverCome(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	blocks := chainOfEmptyBlocks(1, 6)
	blocks[3].Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash([]byte{3})}}
	for n, b := range blocks {
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Maybe()
	}
	mockRPC.On("FetchReceiptsBatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(map[common.Hash]*rpclient.Receipt(nil), errors.New("receipts not found"))
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockDB.On("RequeueMissingBlock", mock.Anything, uint64(3)).Return(nil).Once()
	mockDB.On("GetBlock", mock.Anything, uint64(2)).Return(&types.Block{Number: 2, Hash: blocks[2].Hash.Hex()}, nil)
	idx := newPrefetchIndexer(mockDB, mockRPC)

	var skip receiptSkip
	for attempt := 1; attempt < maxReceiptAttempts; attempt++ {
		if last := idx.indexPipelined(context.Background(), 2, 6, 4, &skip); last != 2 {
			t.Fatalf("attempt %d: last indexed %d, want 2", attempt, last)
		}
	}
	if last := idx.indexPipelined(context.Background(), 2, 6, 4, &skip); last != 6 {
		t.Fatalf("after %d attempts: last indexed %d, want 6", maxReceiptAttempts, last)
	}
	mockDB.AssertExpectations(t)
}

// A block request that arrives while the pipelined loop runs is answered, not left waiting until the loop ends.
func TestIndexPipelinedServesBlockRequests(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	blocks := chainOfEmptyBlocks(1, 5)
	blocks[99] = chainOfEmptyBlocks(99, 99)[99]
	for n, b := range blocks {
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Maybe()
	}
	var inserted []uint64
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		inserted = append(inserted, args.Get(1).(*types.Block).Number)
	}).Return(nil)
	mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	idx := newPrefetchIndexer(mockDB, mockRPC)

	done := make(chan error, 1)
	idx.indexRequests <- indexRequest{blockNumber: 99, done: done}
	last := idx.indexPipelined(context.Background(), 0, 5, 2, &receiptSkip{})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("request for block 99 failed: %v", err)
		}
	default:
		t.Fatal("the request for block 99 was never answered")
	}
	if last != 5 {
		t.Fatalf("last indexed %d, want 5", last)
	}
	saw99 := false
	for _, n := range inserted {
		saw99 = saw99 || n == 99
	}
	if !saw99 {
		t.Fatalf("inserted %v, want block 99 among them", inserted)
	}
}

// The first block of a tick must extend the stored head. Read after a reorg replaced the head, it is
// discarded, as every later block already is; written, it and the blocks after it would bury the stale head below
// the next tick's ten-block reorg check.
func TestIndexPrefetchedStopsWhenTheFirstBlockDoesNotExtendTheStoredHead(t *testing.T) {
	for _, pipelined := range []bool{false, true} {
		mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
		for n, b := range chainOfEmptyBlocks(4, 8) {
			mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Maybe()
		}
		mockDB.On("GetBlock", mock.Anything, uint64(3)).Return(&types.Block{Number: 3, Hash: common.BytesToHash([]byte{0xde, 0xad}).Hex()}, nil)
		mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		idx := newPrefetchIndexer(mockDB, mockRPC)

		var last uint64
		if pipelined {
			last = idx.indexPipelined(context.Background(), 3, 8, 2, &receiptSkip{})
		} else {
			last = idx.indexPrefetched(context.Background(), 3, 8, &receiptSkip{})
		}
		if last != 3 {
			t.Fatalf("pipelined=%v: indexed up to %d over a stale head, want 3", pipelined, last)
		}
		mockDB.AssertNotCalled(t, "InsertBlock", mock.Anything, mock.Anything)
	}
}
