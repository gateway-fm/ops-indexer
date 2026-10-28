package indexer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// laggingReorgNode is a reorgNode that does not have some blocks yet: it answers their header lookups with
// rpc.ErrBlockNotAvailable, as RawBlockHash does for a null result.
type laggingReorgNode struct {
	*reorgNode
	missing map[uint64]bool
}

func (n *laggingReorgNode) RawBlockHash(ctx context.Context, number uint64) (string, error) {
	n.mu.Lock()
	missing := n.missing[number]
	n.mu.Unlock()
	if missing {
		return "", fmt.Errorf("block %d %w", number, rpc.ErrBlockNotAvailable)
	}
	return n.reorgNode.RawBlockHash(ctx, number)
}

func (n *laggingReorgNode) catchUp() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.missing = nil
}

// At start, a stored block the node does not have yet is not a mismatch and not a reason to fail the start: the
// check goes on below it. A stale block below it that the node does serve is still reverted.
func TestStartCheckSkipsStoredBlocksTheNodeDoesNotHaveYet(t *testing.T) {
	t.Run("nothing stale", func(t *testing.T) {
		node := &laggingReorgNode{reorgNode: newReorgNode(1000, 100), missing: map[uint64]bool{98: true, 99: true, 100: true}}
		store := newReorgStore(100, node.tick)
		head, err := NewWithConfig(store, node, 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100}).revertStaleHead(context.Background(), 100)
		if err != nil || head != 100 {
			t.Fatalf("head %d, error %v; want 100 and no error", head, err)
		}
		if d := store.deletedBlocks(); len(d) != 0 {
			t.Fatalf("deleted %v, want nothing", d)
		}
	})
	t.Run("stale below the missing blocks", func(t *testing.T) {
		node := &laggingReorgNode{reorgNode: newReorgNode(96, 100), missing: map[uint64]bool{99: true, 100: true}}
		store := newReorgStore(100, node.tick)
		head, err := NewWithConfig(store, node, 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100}).revertStaleHead(context.Background(), 100)
		if err != nil || head != 95 {
			t.Fatalf("head %d, error %v; want 95: 96..98 are stale on the node's chain", head, err)
		}
	})
}

// A stale stored head the start could not compare (the node was behind) is reverted by the standard loop once the
// node serves those blocks, and the new chain is indexed.
func TestStaleHeadTheStartCouldNotCompareIsRevertedOnceTheNodeServesIt(t *testing.T) {
	node := &laggingReorgNode{reorgNode: newReorgNode(98, 103), missing: map[uint64]bool{98: true, 99: true, 100: true}}
	store := newReorgStore(100, node.tick)
	idx := NewWithConfig(store, node, 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
	head, err := idx.revertStaleHead(context.Background(), 100)
	if err != nil || head != 100 {
		t.Fatalf("start: head %d, error %v; want 100 and no error", head, err)
	}
	node.catchUp()

	runStandard(t, idx, node.reorgNode, head, 1, func() bool {
		for n, h := range store.hashes(98, 103) {
			if h != node.chain[n].Hash.Hex() {
				return false
			}
		}
		return true
	})
	for n, h := range store.hashes(98, 103) {
		if h != node.chain[n].Hash.Hex() {
			t.Fatalf("block %d stored as %q, want the chain's %s", n, h, node.chain[n].Hash.Hex())
		}
	}
}
