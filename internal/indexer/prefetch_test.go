package indexer

import (
	"context"
	"errors"
	"fmt"
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

// A block whose receipts never come is checked against the block before it first. One from a reorged fork is not
// recorded as missing and moved past, which would make it the parent of the blocks indexed after it; the loop stops
// there, as for any block that does not extend the last one.
func TestPrefetchDoesNotSkipABlockFromAnotherForkWhoseReceiptsNeverCome(t *testing.T) {
	for _, pipelined := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "pipelined"}[pipelined], func(t *testing.T) {
			mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
			blocks := chainOfEmptyBlocks(1, 6)
			blocks[3].Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash([]byte{3})}}
			blocks[3].ParentHash = common.BytesToHash([]byte{0xf0, 0x2c})
			for n, b := range blocks {
				mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(b, nil).Maybe()
			}
			mockRPC.On("FetchReceiptsBatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(map[common.Hash]*rpclient.Receipt(nil), errors.New("receipts not found"))
			mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
			mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			mockDB.On("RequeueMissingBlock", mock.Anything, mock.Anything).Return(nil).Maybe()
			mockDB.On("GetBlock", mock.Anything, uint64(2)).Return(&types.Block{Number: 2, Hash: blocks[2].Hash.Hex()}, nil)
			idx := newPrefetchIndexer(mockDB, mockRPC)

			var skip receiptSkip
			for attempt := 1; attempt <= maxReceiptAttempts+1; attempt++ {
				var last uint64
				if pipelined {
					last = idx.indexPipelined(context.Background(), 2, 6, 2, &skip)
				} else {
					last = idx.indexPrefetched(context.Background(), 2, 6, &skip)
				}
				if last != 2 {
					t.Fatalf("attempt %d: last indexed %d, want 2: block 3's parent is not the stored block 2", attempt, last)
				}
			}
			mockDB.AssertNotCalled(t, "RequeueMissingBlock", mock.Anything, mock.Anything)
			mockDB.AssertNotCalled(t, "InsertBlock", mock.Anything, mock.Anything)
		})
	}
}

// The block a tick starts from must be stored for the first fetched block to be checked against it. When it is not
// (a revert deleted it and reported an error), the tick starts from the highest stored block below instead of
// skipping the check: here block 100 is missing, the stored 99 is stale, and nothing is written on top of it.
func TestPrefetchWithoutItsStartBlockStoredStillChecksTheParent(t *testing.T) {
	for _, pipelined := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "pipelined"}[pipelined], func(t *testing.T) {
			node := newReorgNode(99, 103)
			store := newReorgStore(99, node.tick)
			idx := newReorgStandard(store, node)

			var last uint64
			if pipelined {
				last = idx.indexPipelined(context.Background(), 100, 103, 2, &receiptSkip{})
			} else {
				last = idx.indexPrefetched(context.Background(), 100, 103, &receiptSkip{})
			}
			if w := writtenNumbers(store.written()); len(w) != 0 {
				t.Fatalf("written %v on top of a stale block 99 while block 100 was not stored", w)
			}
			if last != 99 {
				t.Fatalf("last indexed %d, want 99, the highest stored block", last)
			}
		})
	}
}

// receiptlessNode is a reorgNode whose receipts never come.
type receiptlessNode struct{ *reorgNode }

func (receiptlessNode) FetchReceiptsBatch(context.Context, []common.Hash, int, int, ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	return nil, errors.New("receipts not found")
}

// A block skipped for its receipts at the end of a tick is not stored, and the next tick starts after it: it checks
// the next block against the skipped block's hash and goes on, instead of starting again below it.
func TestPrefetchContinuesAfterABlockSkippedAtTheEndOfATick(t *testing.T) {
	node := newReorgNode(1000, 8)
	node.chain[6].Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash([]byte{6})}}
	store := newReorgStore(5, node.tick)
	store.MockDatabase.On("RequeueMissingBlock", mock.Anything, uint64(6)).Return(nil)
	idx := NewWithConfig(store, receiptlessNode{node}, 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})

	var skip receiptSkip
	last := uint64(5)
	for attempt := 0; attempt < maxReceiptAttempts; attempt++ {
		last = idx.indexPrefetched(context.Background(), last, 6, &skip)
	}
	if last != 6 {
		t.Fatalf("last indexed %d, want 6 once block 6 is recorded as missing", last)
	}
	if last = idx.indexPrefetched(context.Background(), last, 8, &skip); last != 8 {
		t.Fatalf("next tick indexed up to %d, want 8: it starts after the skipped block 6", last)
	}
	if got := writtenNumbers(store.written()); fmt.Sprint(got) != "[7 8]" {
		t.Fatalf("written %v, want [7 8]", got)
	}
}

// skipThenReorg skips block 100 for its receipts (the chain's old block, which has a transaction), then replaces
// 100 with a new block that 101 and 102 extend, as a reorg at the skipped block does.
func skipThenReorg(t *testing.T, pipelined bool) (*reorgNode, *reorgStore, *Indexer, *receiptSkip, func(last, latest uint64) uint64) {
	t.Helper()
	node := newReorgNode(1000, 100)
	node.chain[100].Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash([]byte{100})}}
	store := newReorgStore(99, node.tick)
	store.MockDatabase.On("RequeueMissingBlock", mock.Anything, uint64(100)).Return(nil)
	idx := NewWithConfig(store, receiptlessNode{node}, 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
	skip := &receiptSkip{}
	tick := func(last, latest uint64) uint64 {
		if pipelined {
			return idx.indexPipelined(context.Background(), last, latest, 2, skip)
		}
		return idx.indexPrefetched(context.Background(), last, latest, skip)
	}
	last := uint64(99)
	for attempt := 0; attempt < maxReceiptAttempts; attempt++ {
		last = tick(last, 100)
	}
	if last != 100 || skip.skipped != 100 {
		t.Fatalf("setup: last %d, skipped %d; want block 100 skipped", last, skip.skipped)
	}
	node.mu.Lock()
	for n := uint64(100); n <= 102; n++ {
		node.chain[n] = &rpc.RawBlock{Number: node.chain[99].Number, Hash: newHash(n), ParentHash: newHash(n - 1)}
		node.chain[n].Number = (*hexutil.Big)(new(big.Int).SetUint64(n))
	}
	node.chain[100].ParentHash = oldHash(99)
	node.head = 102
	node.mu.Unlock()
	return node, store, idx, skip, tick
}

// The hash remembered for a block skipped for its receipts is what the chain had then. When the chain replaces that
// block, the next block does not extend the remembered hash; the loop re-reads the skipped block from the node, drops
// the stale hash and indexes the new chain from the highest stored block, instead of stopping at it forever.
func TestARememberedSkipThatTheChainReplacedIsCheckedAgain(t *testing.T) {
	for _, pipelined := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "pipelined"}[pipelined], func(t *testing.T) {
			node, store, _, _, tick := skipThenReorg(t, pipelined)
			last := uint64(100)
			for i := 0; i < 3 && last < 102; i++ {
				last = tick(last, 102)
			}
			if last != 102 {
				t.Fatalf("last indexed %d after 3 ticks, want 102", last)
			}
			for n, h := range store.hashes(100, 102) {
				if h != node.chain[n].Hash.Hex() {
					t.Fatalf("block %d stored as %q, want the new chain's %s", n, h, node.chain[n].Hash.Hex())
				}
			}
		})
	}
}

// When the node still has the remembered block, a next block that does not extend it is a real mismatch: the loop
// stops there, as for any block that does not extend the one before it.
func TestARememberedSkipTheChainStillHasStopsAMismatchingBlock(t *testing.T) {
	node, store, _, skip, tick := skipThenReorg(t, false)
	node.mu.Lock()
	node.chain[100] = &rpc.RawBlock{Number: node.chain[100].Number, Hash: skip.skippedHash, ParentHash: oldHash(99)}
	node.mu.Unlock()
	if last := tick(100, 102); last != 100 {
		t.Fatalf("last indexed %d, want 100: 101 does not extend the skipped block the node still has", last)
	}
	if w := writtenNumbers(store.written()); len(w) != 0 {
		t.Fatalf("written %v, want nothing", w)
	}
}

// After a revert whose last delete committed but reported an error, the standard loop indexes the deleted blocks
// again at once, even when the chain has not grown past them.
func TestFailedRevertReplaysWithoutWaitingForANewBlock(t *testing.T) {
	node := newReorgNode(98, 100)
	store := newReorgStore(100, node.tick)
	store.faults = map[uint64]string{98: "ack"}
	idx := newReorgStandard(store, node)

	runUntil(t, func(ctx context.Context) { _ = idx.startStandardMode(ctx, 100) }, node.tick, 1, func() bool {
		for n, h := range store.hashes(98, 100) {
			if h != node.chain[n].Hash.Hex() {
				return false
			}
		}
		return true
	})
	for n, h := range store.hashes(98, 100) {
		if h != node.chain[n].Hash.Hex() {
			t.Fatalf("block %d stored as %q, want the new chain's: replayed while the head stays at 100", n, h)
		}
	}
}
