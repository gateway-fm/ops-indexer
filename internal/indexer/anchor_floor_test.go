package indexer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

// floorStore is a real database that records the blocks written, with the tick they were written in, and can lose
// the acknowledgement of a delete: the delete commits and then reports an error.
type floorStore struct {
	*db.DB
	mu      sync.Mutex
	tick    func() int
	writes  []storedWrite
	lostAck map[uint64]bool
}

func (s *floorStore) InsertBlock(ctx context.Context, b *types.Block) error {
	if err := s.DB.InsertBlock(ctx, b); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, storedWrite{number: b.Number, hash: b.Hash, tick: s.tick()})
	return nil
}

func (s *floorStore) DeleteBlock(ctx context.Context, n uint64) error {
	if err := s.DB.DeleteBlock(ctx, n); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lostAck[n] {
		delete(s.lostAck, n)
		return errors.New("connection reset while committing")
	}
	return nil
}

func (s *floorStore) written() []storedWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storedWrite(nil), s.writes...)
}

func newFloorStore(t *testing.T, node *reorgNode, stored ...uint64) *floorStore {
	t.Helper()
	d, _ := refillTestDB(t)
	for _, n := range stored {
		require.NoError(t, d.InsertBlock(context.Background(), &types.Block{Number: n, Hash: oldHash(n).Hex(),
			ParentHash: oldHash(n - 1).Hex(), Timestamp: 1_700_000_000 + n}))
	}
	return &floorStore{DB: d, tick: node.tick, lostAck: map[uint64]bool{}}
}

func storedHashes(t *testing.T, s *floorStore, from, to uint64) map[uint64]string {
	t.Helper()
	out := map[uint64]string{}
	for n := from; n <= to; n++ {
		b, err := s.GetBlock(context.Background(), n)
		require.NoError(t, err)
		if b != nil {
			out[n] = b.Hash
		}
	}
	return out
}

func requireIndexedFromInOrder(t *testing.T, s *floorStore, node *reorgNode, from, top uint64) {
	t.Helper()
	for n := from; n <= top; n++ {
		b, err := s.GetBlock(context.Background(), n)
		require.NoError(t, err)
		require.NotNil(t, b, "block %d is stored (writes %v)", n, writtenNumbers(s.written()))
		require.Equal(t, node.chain[n].Hash.Hex(), b.Hash, "block %d is the new chain's", n)
	}
	ws := s.written()
	require.NotEmpty(t, ws)
	require.Equal(t, from, ws[0].number, "nothing above %d is written before it (writes %v)", from, writtenNumbers(ws))
	for k := 1; k < len(ws); k++ {
		require.Equal(t, ws[k-1].number+1, ws[k].number, "written in order with no gap (writes %v)", writtenNumbers(ws))
	}
}

// Indexing started at block 98, so only 98..100 are stored. A reorg replaces all three and the last delete's
// acknowledgement is lost: nothing is stored at or below the cursor any more. The next ticks index 98..100 from the
// new chain, in order, before anything above them.
func TestRevertOfEveryStoredBlockIndexesAgainFromTheStartBlock(t *testing.T) {
	for _, mode := range []struct {
		name  string
		depth int
	}{{"serial", 1}, {"pipelined", 4}} {
		t.Run("standard/"+mode.name, func(t *testing.T) {
			node := newReorgNode(98, 103)
			store := newFloorStore(t, node, 98, 99, 100)
			store.lostAck[98] = true
			idx := NewWithConfig(store, node, 10*time.Millisecond, 98,
				&Config{RPCWorkers: 1, RPCRateLimit: 100, CommitPipelineDepth: mode.depth})

			runStandard(t, idx, node, 100, 1, func() bool { return len(storedHashes(t, store, 98, 103)) == 6 })

			requireIndexedFromInOrder(t, store, node, 98, 103)
		})
	}
	t.Run("realtime", func(t *testing.T) {
		node := newReorgNode(98, 103)
		store := newFloorStore(t, node, 98, 99, 100)
		store.lostAck[98] = true
		rt := NewRealtimeIndexer(store, node, &RealtimeConfig{ConfirmationBlocks: 1, PollInterval: 10 * time.Millisecond},
			&Config{RPCWorkers: 1, RPCRateLimit: 100}, NewTokenCache(), NewContractCache(), nil, false)
		rt.SetLastProcessedBlock(100)

		runRealtime(t, rt, node, false, 1, func() bool { return len(storedHashes(t, store, 98, 102)) == 5 })

		requireIndexedFromInOrder(t, store, node, 98, 102)
	})
}

// A fresh database with the start block at 98 indexes from 98, in both standard modes.
func TestFreshDatabaseIndexesFromTheStartBlock(t *testing.T) {
	for _, mode := range []struct {
		name  string
		depth int
	}{{"serial", 1}, {"pipelined", 4}} {
		t.Run(mode.name, func(t *testing.T) {
			node := newReorgNode(1000, 103)
			store := newFloorStore(t, node)
			idx := NewWithConfig(store, node, 10*time.Millisecond, 98,
				&Config{RPCWorkers: 1, RPCRateLimit: 100, CommitPipelineDepth: mode.depth})

			runStandard(t, idx, node, 0, 1, func() bool { return len(storedHashes(t, store, 98, 103)) == 6 })

			requireIndexedFromInOrder(t, store, node, 98, 103)
		})
	}
}

type prunedFloorStore struct {
	*MockDatabase
	prunedThrough uint64
}

func (s prunedFloorStore) GetBlock(context.Context, uint64) (*types.Block, error) { return nil, nil }

func (s prunedFloorStore) HighestBlockAtOrBelow(context.Context, uint64) (*types.Block, error) {
	return nil, nil
}

func (s prunedFloorStore) PrunedThrough(context.Context) (uint64, int64, error) {
	return s.prunedThrough, 0, nil
}

// With nothing stored at or below the cursor, the tick starts at the higher of the start block and the lowest block
// retention keeps.
func TestUnstoredCursorStartsAtTheStartBlockOrAboveThePruneWatermark(t *testing.T) {
	for _, c := range []struct {
		name               string
		startBlock, pruned uint64
		cursor, wantAfter  uint64
	}{
		{"start block", 98, 0, 100, 97},
		{"prune watermark above the start block", 98, 500, 600, 500},
		{"start block above the prune watermark", 800, 500, 900, 799},
		{"genesis", 0, 0, 100, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			idx := NewWithConfig(prunedFloorStore{MockDatabase: new(MockDatabase), prunedThrough: c.pruned}, new(MockRPCClient),
				time.Second, c.startBlock, &Config{RPCWorkers: 1, RPCRateLimit: 100})
			from, hash, _, ok := idx.anchor(context.Background(), c.cursor, &receiptSkip{})
			require.True(t, ok)
			require.Equal(t, c.wantAfter, from)
			require.Zero(t, hash, "there is no stored block to check the first block against")
		})
	}
}
