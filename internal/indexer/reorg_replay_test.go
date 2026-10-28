package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// reorgNode is a chain whose blocks forkAt..head were replaced: the stored database holds the old blocks below
// storedHead, and the node now serves the new ones. Each BlockNumber call starts a polling tick.
type reorgNode struct {
	*MockRPCClient
	mu      sync.Mutex
	chain   map[uint64]*rpc.RawBlock
	head    uint64
	ticks   int
	hashErr map[uint64]int
}

func oldHash(n uint64) common.Hash {
	return common.BytesToHash(new(big.Int).SetUint64(1000 + n).Bytes())
}
func newHash(n uint64) common.Hash {
	return common.BytesToHash(new(big.Int).SetUint64(5000 + n).Bytes())
}

func newReorgNode(forkAt, head uint64) *reorgNode {
	chain := make(map[uint64]*rpc.RawBlock)
	for n := uint64(1); n <= head; n++ {
		hash, parent := oldHash(n), oldHash(n-1)
		if n >= forkAt {
			hash = newHash(n)
		}
		if n > forkAt {
			parent = newHash(n - 1)
		}
		chain[n] = &rpc.RawBlock{Number: (*hexutil.Big)(new(big.Int).SetUint64(n)), Hash: hash, ParentHash: parent}
	}
	return &reorgNode{MockRPCClient: new(MockRPCClient), chain: chain, head: head, hashErr: map[uint64]int{}}
}

func (c *reorgNode) BlockNumber(context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ticks++
	return c.head, nil
}

func (c *reorgNode) tick() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ticks
}

func (c *reorgNode) RawBlockByNumber(_ context.Context, n uint64) (*rpc.RawBlock, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.chain[n]; ok {
		return b, nil
	}
	return nil, errors.New("block not found")
}

// RawBlockHash fails for a block while hashErr still counts failures for it.
func (c *reorgNode) RawBlockHash(_ context.Context, n uint64) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hashErr[n] > 0 {
		c.hashErr[n]--
		return "", errors.New("connection refused")
	}
	return c.chain[n].Hash.Hex(), nil
}

func (c *reorgNode) SubscribeNewHead(context.Context, chan<- *rpclient.Header) (rpclient.Subscription, error) {
	return nil, errors.New("not supported")
}

type storedWrite struct {
	number uint64
	hash   string
	tick   int
}

// reorgStore holds blocks by number and records every block written, with the tick it was written in.
type reorgStore struct {
	*MockDatabase
	mu        sync.Mutex
	blocks    map[uint64]string
	writes    []storedWrite
	tick      func() int
	deleteErr error
	keepBelow uint64
	getErr    map[uint64]bool
	deleted   []uint64
	// faults makes the next DeleteBlock of a block fail once: "ack" commits the delete and then reports an error, as a
	// lost COMMIT acknowledgement does; "fail" reports an error with nothing deleted.
	faults map[uint64]string
}

func newReorgStore(storedHead uint64, tick func() int) *reorgStore {
	blocks := make(map[uint64]string)
	for n := uint64(1); n <= storedHead; n++ {
		blocks[n] = oldHash(n).Hex()
	}
	return &reorgStore{MockDatabase: new(MockDatabase), blocks: blocks, tick: tick, getErr: map[uint64]bool{}}
}

func (s *reorgStore) GetBlock(_ context.Context, n uint64) (*types.Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr[n] {
		return nil, errors.New("connection reset")
	}
	if h, ok := s.blocks[n]; ok {
		return &types.Block{Number: n, Hash: h}, nil
	}
	return nil, nil
}

func (s *reorgStore) HighestBlockAtOrBelow(_ context.Context, n uint64) (*types.Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *types.Block
	for m, h := range s.blocks {
		if m <= n && (best == nil || m > best.Number) {
			best = &types.Block{Number: m, Hash: h}
		}
	}
	return best, nil
}

func (s *reorgStore) GetLatestBlockNumber(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest uint64
	for n := range s.blocks {
		latest = max(latest, n)
	}
	return latest, nil
}

func (s *reorgStore) DeleteBlock(_ context.Context, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil || n < s.keepBelow {
		return errors.New("delete failed")
	}
	fault := s.faults[n]
	delete(s.faults, n)
	if fault == "fail" {
		return errors.New("delete failed")
	}
	delete(s.blocks, n)
	s.deleted = append(s.deleted, n)
	if fault == "ack" {
		return errors.New("connection reset while committing")
	}
	return nil
}

func (s *reorgStore) InsertBlock(_ context.Context, b *types.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks[b.Number] = b.Hash
	s.writes = append(s.writes, storedWrite{number: b.Number, hash: b.Hash, tick: s.tick()})
	return nil
}

func (s *reorgStore) UpdateSyncStatus(context.Context, uint64, bool) error { return nil }

func (s *reorgStore) deletedBlocks() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.deleted...)
}

func (s *reorgStore) written() []storedWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storedWrite(nil), s.writes...)
}

func (s *reorgStore) hashes(from, to uint64) map[uint64]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uint64]string)
	for n := from; n <= to; n++ {
		out[n] = s.blocks[n]
	}
	return out
}

func newReorgRealtime(store *reorgStore, node *reorgNode, lastProcessed uint64) *RealtimeIndexer {
	rt := NewRealtimeIndexer(store, node, &RealtimeConfig{ConfirmationBlocks: 1, PollInterval: 10 * time.Millisecond},
		&Config{RPCWorkers: 1, RPCRateLimit: 100}, NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(lastProcessed)
	return rt
}

// runRealtime runs the realtime loop in polling or subscription mode until done reports true or the deadline
// passes. In subscription mode it delivers one header for the node's head per tick.
func runRealtime(t *testing.T, rt *RealtimeIndexer, node *reorgNode, subscription bool, ticks int, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan struct{})
	headers := make(chan *rpclient.Header)
	go func() {
		defer close(finished)
		if subscription {
			_ = rt.runSubscriptionMode(ctx, headers, newMockSubscription())
		} else {
			_ = rt.runPollingMode(ctx)
		}
	}()
	for ctx.Err() == nil {
		if subscription && node.tick() < ticks {
			node.mu.Lock()
			node.ticks++
			head := node.head
			node.mu.Unlock()
			select {
			case headers <- &rpclient.Header{Number: new(big.Int).SetUint64(head)}:
			case <-ctx.Done():
			}
		}
		if node.tick() >= ticks && done() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-finished
}

func writtenNumbers(ws []storedWrite) []uint64 {
	out := make([]uint64, len(ws))
	for i, w := range ws {
		out[i] = w.number
	}
	return out
}

// After a reorg the realtime loop reverts the stale blocks in one tick and indexes from the fork point on the next:
// stored 1..100, the chain replaced 98..100, safe head 102, so 98..102 are written from the new chain, in order, in
// tick 2, with no block left behind for the gap collector.
func TestRealtimeReorgReplaysFromTheForkPointInTheNextTick(t *testing.T) {
	for _, mode := range []struct {
		name         string
		subscription bool
	}{{"polling", false}, {"subscription", true}} {
		t.Run(mode.name, func(t *testing.T) {
			node := newReorgNode(98, 103)
			store := newReorgStore(100, node.tick)
			rt := newReorgRealtime(store, node, 100)

			runRealtime(t, rt, node, mode.subscription, 2, func() bool { return len(store.written()) >= 5 })

			ws := store.written()
			got := writtenNumbers(ws)
			want := []uint64{98, 99, 100, 101, 102}
			if len(got) != len(want) {
				t.Fatalf("written %v, want %v", got, want)
			}
			for i, w := range ws {
				if w.number != want[i] || w.hash != node.chain[w.number].Hash.Hex() || w.tick != 2 {
					t.Fatalf("write %d = block %d hash %s in tick %d, want block %d from the new chain in tick 2 (all writes %v)",
						i, w.number, w.hash, w.tick, want[i], got)
				}
			}
			for n, h := range store.hashes(98, 102) {
				if h != node.chain[n].Hash.Hex() {
					t.Fatalf("stored block %d is %q, want the new chain's %s", n, h, node.chain[n].Hash.Hex())
				}
			}
			if last := rt.GetLastProcessedBlock(); last != 102 {
				t.Fatalf("last processed %d, want 102", last)
			}
		})
	}
}

// When the stale blocks cannot be reverted, the loop does not move past the fork point: nothing is written and the
// next tick tries again.
func TestRealtimeReorgThatCannotBeRevertedWritesNothingPastTheFork(t *testing.T) {
	for _, mode := range []struct {
		name         string
		subscription bool
	}{{"polling", false}, {"subscription", true}} {
		t.Run(mode.name, func(t *testing.T) {
			node := newReorgNode(98, 103)
			store := newReorgStore(100, node.tick)
			store.deleteErr = errors.New("delete failed")
			rt := newReorgRealtime(store, node, 100)

			runRealtime(t, rt, node, mode.subscription, 3, func() bool { return true })

			if got := writtenNumbers(store.written()); len(got) != 0 {
				sort.Slice(got, func(a, b int) bool { return got[a] < got[b] })
				t.Fatalf("written %v, want nothing while the stale blocks 98..100 cannot be reverted", got)
			}
			if last := rt.GetLastProcessedBlock(); last > 100 {
				t.Fatalf("last processed %d, want at most 100", last)
			}
		})
	}
}

func runStandard(t *testing.T, idx *Indexer, node *reorgNode, lastIndexed uint64, ticks int, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = idx.startStandardMode(ctx, lastIndexed)
	}()
	for ctx.Err() == nil && (node.tick() < ticks || !done()) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-finished
}

func newReorgStandard(store *reorgStore, node *reorgNode) *Indexer {
	return NewWithConfig(store, node, 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
}

// The standard loop does not index past a reorg it could only partly revert: with block 100 deleted and 99 left, the
// next block would otherwise be written on top of the stale 98 and 99.
func TestStandardReorgThatCannotBeRevertedWritesNothingPastTheFork(t *testing.T) {
	node := newReorgNode(98, 103)
	store := newReorgStore(100, node.tick)
	store.keepBelow = 100
	idx := newReorgStandard(store, node)

	runStandard(t, idx, node, 100, 3, func() bool { return true })

	if got := writtenNumbers(store.written()); len(got) != 0 {
		t.Fatalf("written %v, want nothing while the stale blocks 98 and 99 cannot be reverted", got)
	}
}

// A detection walk that finds stale blocks and then fails deeper down reports what it found together with the error.
func TestDetectReorgKeepsTheMismatchFoundBeforeAnError(t *testing.T) {
	for _, c := range []struct {
		name string
		fail func(*reorgNode, *reorgStore)
	}{
		{"node", func(n *reorgNode, _ *reorgStore) { n.hashErr[99] = 1 }},
		{"store", func(_ *reorgNode, s *reorgStore) { s.getErr[99] = true }},
	} {
		t.Run(c.name, func(t *testing.T) {
			detectors := map[string]func(*reorgStore, *reorgNode) (uint64, error){
				"indexer": func(s *reorgStore, n *reorgNode) (uint64, error) {
					return newReorgStandard(s, n).detectReorg(context.Background(), 100)
				},
				"realtime": func(s *reorgStore, n *reorgNode) (uint64, error) {
					return newReorgRealtime(s, n, 100).detectReorg(context.Background(), 100)
				},
			}
			for name, detect := range detectors {
				node := newReorgNode(98, 103)
				store := newReorgStore(100, node.tick)
				c.fail(node, store)
				depth, err := detect(store, node)
				if err == nil || depth != 1 {
					t.Fatalf("%s: depth %d, error %v; want depth 1 (block 100 is stale) and the error from block 99", name, depth, err)
				}
			}
		})
	}
}

// A reorg check that fails ends the tick before any block is indexed. With the node healthy again, realtime reverts
// the stale blocks on the next tick and indexes the new chain on the one after; the standard loop reverts and indexes
// in the same tick.
func TestReorgCheckErrorEndsTheTick(t *testing.T) {
	for _, c := range []struct {
		name                       string
		forkAt                     uint64
		want                       []uint64
		realtimeTick, standardTick int
	}{
		{"stale head", 98, []uint64{98, 99, 100, 101, 102}, 3, 2},
		{"no mismatch", 200, []uint64{101, 102}, 2, 2},
	} {
		for _, mode := range []struct {
			name         string
			subscription bool
		}{{"polling", false}, {"subscription", true}} {
			t.Run(c.name+"/realtime/"+mode.name, func(t *testing.T) {
				node := newReorgNode(c.forkAt, 103)
				node.hashErr[99] = 1
				store := newReorgStore(100, node.tick)
				rt := newReorgRealtime(store, node, 100)

				runRealtime(t, rt, node, mode.subscription, c.realtimeTick, func() bool { return len(store.written()) >= len(c.want) })

				assertWrittenInTick(t, store, node, c.want, c.realtimeTick)
			})
		}
		t.Run(c.name+"/standard", func(t *testing.T) {
			node := newReorgNode(c.forkAt, 103)
			node.hashErr[99] = 1
			store := newReorgStore(100, node.tick)
			want := append(append([]uint64(nil), c.want...), 103)

			runStandard(t, newReorgStandard(store, node), node, 100, c.standardTick, func() bool { return len(store.written()) >= len(want) })

			assertWrittenInTick(t, store, node, want, c.standardTick)
		})
	}
}

func assertWrittenInTick(t *testing.T, store *reorgStore, node *reorgNode, want []uint64, tick int) {
	t.Helper()
	ws := store.written()
	if len(ws) != len(want) {
		t.Fatalf("written %v, want %v in tick %d", writtenNumbers(ws), want, tick)
	}
	for i, w := range ws {
		if w.number != want[i] || w.hash != node.chain[w.number].Hash.Hex() || w.tick != tick {
			t.Fatalf("write %d = block %d hash %s in tick %d, want block %d from the chain in tick %d (all writes %v)",
				i, w.number, w.hash, w.tick, want[i], tick, writtenNumbers(ws))
		}
	}
}

// A revert whose last delete reported an error may still have deleted the blocks: the COMMIT's acknowledgement can
// be lost. Whatever the deletes did, the next ticks index the new chain from the fork with no block missing: a deleted
// block is indexed again, and a stale block still stored is reverted again.
func TestFailedRevertReplaysTheForkWhateverTheDeletesDid(t *testing.T) {
	for _, c := range []struct {
		name   string
		faults map[uint64]string
	}{
		{"acknowledgement lost on the last delete", map[uint64]string{98: "ack"}},
		{"last delete rolled back", map[uint64]string{98: "fail"}},
		{"partial: 100 deleted, 99 fails", map[uint64]string{99: "fail"}},
	} {
		for _, loop := range []string{"realtime", "standard"} {
			t.Run(c.name+"/"+loop, func(t *testing.T) {
				node := newReorgNode(98, 103)
				store := newReorgStore(100, node.tick)
				store.faults = map[uint64]string{}
				for n, f := range c.faults {
					store.faults[n] = f
				}
				top := uint64(102)
				var run func(ctx context.Context)
				if loop == "realtime" {
					rt := newReorgRealtime(store, node, 100)
					run = func(ctx context.Context) { _ = rt.runPollingMode(ctx) }
				} else {
					top = 103
					idx := newReorgStandard(store, node)
					run = func(ctx context.Context) { _ = idx.startStandardMode(ctx, 100) }
				}
				replayed := func() bool {
					for n, h := range store.hashes(98, top) {
						if h != node.chain[n].Hash.Hex() {
							return false
						}
					}
					return true
				}
				runUntil(t, run, node.tick, 1, replayed)

				for n, h := range store.hashes(1, top) {
					if h != node.chain[n].Hash.Hex() {
						t.Fatalf("block %d stored as %q, want %s: every block up to %d from the new chain (writes %v, deletes %v)",
							n, h, node.chain[n].Hash.Hex(), top, writtenNumbers(store.written()), store.deletedBlocks())
					}
				}
			})
		}
	}
}

// skipNode is a reorgNode whose receipts never come, and skipStore records the blocks recorded as missing.
type skipNode struct{ *reorgNode }

func (skipNode) FetchReceiptsBatch(context.Context, []common.Hash, int, int, ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	return nil, errors.New("receipts not found")
}

type skipStore struct {
	*reorgStore
	requeued []uint64
}

func (s *skipStore) RequeueMissingBlock(_ context.Context, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requeued = append(s.requeued, n)
	return nil
}

// Realtime indexes through the prefetch loop, so a block whose receipts never come is checked against its parent
// before it can be recorded as missing: one from another fork is never moved past, however many ticks it fails.
func TestRealtimeDoesNotSkipABlockFromAnotherForkWhoseReceiptsNeverCome(t *testing.T) {
	node := newReorgNode(1000, 7)
	node.chain[3].ParentHash = common.BytesToHash([]byte{0xf0, 0x2c})
	node.chain[3].Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash([]byte{3})}}
	store := &skipStore{reorgStore: newReorgStore(2, node.tick)}
	rt := NewRealtimeIndexer(store, skipNode{node}, &RealtimeConfig{ConfirmationBlocks: 1, PollInterval: 10 * time.Millisecond},
		&Config{RPCWorkers: 1, RPCRateLimit: 100}, NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(2)

	runRealtime(t, rt, node, false, maxReceiptAttempts+2, func() bool { return true })

	if got := writtenNumbers(store.written()); len(got) != 0 {
		t.Fatalf("written %v, want nothing: block 3's parent is not the stored block 2", got)
	}
	store.mu.Lock()
	requeued := store.requeued
	store.mu.Unlock()
	if len(requeued) != 0 {
		t.Fatalf("recorded %v as missing, want nothing", requeued)
	}
	if last := rt.GetLastProcessedBlock(); last != 2 {
		t.Fatalf("last processed %d, want 2", last)
	}
}

// Realtime starts at the node head, and a jump moves it there, with nothing stored at its cursor or for a long way
// below: catch-up fills that. Its first batch goes on from the cursor; it does not start again after the highest
// stored block, far below.
func TestRealtimeAtAnUnstoredCursorDoesNotStartFromTheHighestStoredBlock(t *testing.T) {
	node := newReorgNode(1000, 503)
	store := newReorgStore(10, node.tick)
	rt := newReorgRealtime(store, node, 500)

	rt.indexUpTo(context.Background(), 500, 502)

	if got := writtenNumbers(store.written()); fmt.Sprint(got) != "[501 502]" {
		t.Fatalf("written %d blocks from %v, want [501 502]", len(got), got[:min(len(got), 3)])
	}
}

// Realtime runs through the prefetch loop too, with the same remembered hash for a block skipped for its receipts.
// When the chain replaces that block, realtime drops the stale hash and goes on with the new chain; the skipped
// block stays recorded as missing for catch-up.
func TestRealtimeGoesOnAfterTheChainReplacesASkippedBlock(t *testing.T) {
	node := newReorgNode(1000, 100)
	node.chain[100].Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash([]byte{100})}}
	store := &skipStore{reorgStore: newReorgStore(99, node.tick)}
	rt := NewRealtimeIndexer(store, skipNode{node}, &RealtimeConfig{ConfirmationBlocks: 1, PollInterval: 10 * time.Millisecond},
		&Config{RPCWorkers: 1, RPCRateLimit: 100}, NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(99)
	for attempt := 0; attempt < maxReceiptAttempts; attempt++ {
		rt.indexUpTo(context.Background(), rt.GetLastProcessedBlock(), 100)
	}
	if last := rt.GetLastProcessedBlock(); last != 100 || fmt.Sprint(store.requeued) != "[100]" {
		t.Fatalf("setup: cursor %d, recorded missing %v; want block 100 skipped", last, store.requeued)
	}

	node.mu.Lock()
	for n := uint64(100); n <= 102; n++ {
		node.chain[n] = &rpc.RawBlock{Number: (*hexutil.Big)(new(big.Int).SetUint64(n)), Hash: newHash(n), ParentHash: newHash(n - 1)}
	}
	node.chain[100].ParentHash = oldHash(99)
	node.mu.Unlock()
	for i := 0; i < 3 && rt.GetLastProcessedBlock() < 102; i++ {
		rt.indexUpTo(context.Background(), rt.GetLastProcessedBlock(), 102)
	}

	if last := rt.GetLastProcessedBlock(); last != 102 {
		t.Fatalf("cursor %d after 3 ticks, want 102", last)
	}
	for n, h := range store.hashes(101, 102) {
		if h != node.chain[n].Hash.Hex() {
			t.Fatalf("block %d stored as %q, want the new chain's", n, h)
		}
	}
}
