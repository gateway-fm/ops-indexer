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
