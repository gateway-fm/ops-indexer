package nodelookup

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// splitNode answers hashes from one view of the chain and blocks by number from another: orphans maps a hash the
// node still keeps (an orphan, or the canonical hash a backend behind the number calls does not have) to its height,
// and forked replaces the block a number call returns at a height.
type splitNode struct {
	fakeNode
	orphans map[common.Hash]uint64
	forked  map[uint64]common.Hash
}

func (s *splitNode) BlockNumberByHash(ctx context.Context, hash string) (uint64, bool, error) {
	if n, ok := s.orphans[common.HexToHash(hash)]; ok {
		return n, true, nil
	}
	return s.fakeNode.BlockNumberByHash(ctx, hash)
}

func (s *splitNode) TxBlockNumber(ctx context.Context, hash string) (uint64, bool, error) {
	if n, ok := s.orphans[common.HexToHash(hash)]; ok {
		s.txCalls.Add(1)
		return n, true, nil
	}
	return s.fakeNode.TxBlockNumber(ctx, hash)
}

func (s *splitNode) BlockByNumber(ctx context.Context, n uint64) (*rpc.RawBlock, error) {
	raw, err := s.fakeNode.BlockByNumber(ctx, n)
	if h, ok := s.forked[n]; ok && raw != nil {
		raw.Hash = h
	}
	return raw, err
}

func orphanHash(n uint64) common.Hash {
	return common.BytesToHash(new(big.Int).SetUint64(0x0f00_0000 + n).Bytes())
}

func TestBlockByHashServesOnlyTheBlockWithThatHash(t *testing.T) {
	ctx := context.Background()

	t.Run("orphan hash", func(t *testing.T) {
		node := &splitNode{orphans: map[common.Hash]uint64{orphanHash(40): 40}}
		l := New(node, Config{})
		for range 2 {
			b, err := l.BlockByHash(ctx, orphanHash(40).Hex(), 49)
			require.NoError(t, err)
			assert.Nil(t, b, "the canonical block 40 has another hash")
		}
		b, err := l.BlockByHash(ctx, blockHash(40).Hex(), 49)
		require.NoError(t, err)
		require.NotNil(t, b)
		assert.Equal(t, blockHash(40).Hex(), b.Block.Hash)
	})

	t.Run("backends disagree", func(t *testing.T) {
		node := &splitNode{forked: map[uint64]common.Hash{40: orphanHash(40)}}
		l := New(node, Config{})
		b, err := l.BlockByHash(ctx, blockHash(40).Hex(), 49)
		require.NoError(t, err)
		assert.Nil(t, b, "the number call returned another block at 40")

		delete(node.forked, 40)
		b, err = l.BlockByHash(ctx, blockHash(40).Hex(), 49)
		require.NoError(t, err)
		require.NotNil(t, b, "the mismatched block was not kept: the next request asks the node again")
		assert.Equal(t, blockHash(40).Hex(), b.Block.Hash)
	})

	t.Run("orphan transaction", func(t *testing.T) {
		node := &splitNode{orphans: map[common.Hash]uint64{orphanHash(7): 40}}
		l := New(node, Config{})
		tx, err := l.Transaction(ctx, orphanHash(7).Hex(), 49)
		require.NoError(t, err)
		assert.Nil(t, tx, "block 40 does not hold it")
	})
}
