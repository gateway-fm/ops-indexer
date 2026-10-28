package indexer

import (
	"context"
	"errors"
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
}

func newReorgStore(storedHead uint64, tick func() int) *reorgStore {
	blocks := make(map[uint64]string)
	for n := uint64(1); n <= storedHead; n++ {
		blocks[n] = oldHash(n).Hex()
	}
	return &reorgStore{MockDatabase: new(MockDatabase), blocks: blocks, tick: tick}
}

func (s *reorgStore) GetBlock(_ context.Context, n uint64) (*types.Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h, ok := s.blocks[n]; ok {
		return &types.Block{Number: n, Hash: h}, nil
	}
	return nil, nil
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
	delete(s.blocks, n)
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

// After a reorg the realtime loop reverts the stale blocks and indexes again from the fork point in the same tick:
// stored 1..100, the chain replaced 98..100, safe head 102, so 98..102 are written from the new chain, in order.
func TestRealtimeReorgReplaysFromTheForkPointInTheSameTick(t *testing.T) {
	for _, mode := range []struct {
		name         string
		subscription bool
	}{{"polling", false}, {"subscription", true}} {
		t.Run(mode.name, func(t *testing.T) {
			node := newReorgNode(98, 103)
			store := newReorgStore(100, node.tick)
			rt := newReorgRealtime(store, node, 100)

			runRealtime(t, rt, node, mode.subscription, 1, func() bool { return len(store.written()) >= 5 })

			ws := store.written()
			got := writtenNumbers(ws)
			want := []uint64{98, 99, 100, 101, 102}
			if len(got) != len(want) {
				t.Fatalf("written %v, want %v", got, want)
			}
			for i, w := range ws {
				if w.number != want[i] || w.hash != node.chain[w.number].Hash.Hex() || w.tick != 1 {
					t.Fatalf("write %d = block %d hash %s in tick %d, want block %d from the new chain in tick 1 (all writes %v)",
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
