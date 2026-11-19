package main

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/stretchr/testify/require"
)

type identityTestNode struct {
	chain        *big.Int
	hash         string
	err          error
	genesisBlock uint64
}

type genesisTestNode struct{ block *rpc.RawBlock }

func (n genesisTestNode) RawBlockByNumber(_ context.Context, number uint64) (*rpc.RawBlock, error) {
	if number != 0 {
		return nil, errors.New("wrong block requested")
	}
	return n.block, nil
}

type genesisTestStore struct{ block *types.Block }

func (s *genesisTestStore) EnsureInitialGenesis(_ context.Context, b *types.Block) error {
	s.block = b
	return nil
}

func TestInitialGenesisRequiresActualEmptyBlockZero(t *testing.T) {
	zero := (*hexutil.Big)(big.NewInt(0))
	b := &rpc.RawBlock{Number: zero, Hash: common.HexToHash("0x" + strings.Repeat("ab", 32)), Timestamp: 10, GasLimit: 30000000}
	s := &genesisTestStore{}
	require.NoError(t, initializeGenesis(context.Background(), s, genesisTestNode{b}))
	require.Equal(t, b.Hash.Hex(), s.block.Hash)
	require.EqualValues(t, 10, s.block.Timestamp)
	require.EqualValues(t, 30000000, s.block.GasLimit)
	for _, invalid := range []*rpc.RawBlock{nil, {}, {Number: (*hexutil.Big)(big.NewInt(1))}, {Number: (*hexutil.Big)(new(big.Int).Lsh(big.NewInt(1), 64))}, {Number: zero, Transactions: []rpc.RawTransaction{{}}}} {
		s.block = nil
		require.Error(t, initializeGenesis(context.Background(), s, genesisTestNode{invalid}))
		require.Nil(t, s.block)
	}
}

func (n *identityTestNode) ChainID(context.Context) (*big.Int, error) { return n.chain, n.err }
func (n *identityTestNode) RawBlockHash(_ context.Context, block uint64) (string, error) {
	n.genesisBlock = block
	return n.hash, n.err
}

type identityTestStore struct {
	calls   int
	chain   string
	genesis []byte
	err     error
}

func (s *identityTestStore) EnsureDatasetIdentity(_ context.Context, chain string, genesis []byte) error {
	s.calls++
	s.chain, s.genesis = chain, genesis
	return s.err
}

func TestDatasetIdentityComesFromConfiguredNode(t *testing.T) {
	s := &identityTestStore{}
	n := &identityTestNode{chain: big.NewInt(31337), hash: "0x" + strings.Repeat("ab", 32), genesisBlock: 99}
	require.NoError(t, initializeDatasetIdentity(context.Background(), s, n))
	require.Equal(t, uint64(0), n.genesisBlock)
	require.Equal(t, "31337", s.chain)
	require.Len(t, s.genesis, 32)
	require.Equal(t, byte(0xab), s.genesis[0])
	for _, node := range []*identityTestNode{
		{chain: nil}, {chain: big.NewInt(-1)}, {chain: big.NewInt(1), hash: "not-a-hash"},
		{chain: big.NewInt(1), hash: "0xab"}, {err: errors.New("node unavailable")},
	} {
		s.calls = 0
		require.Error(t, initializeDatasetIdentity(context.Background(), s, node))
		require.Zero(t, s.calls, "invalid node evidence must not initialize the source")
	}
	s.err = errors.New("wrong existing chain")
	require.ErrorIs(t, initializeDatasetIdentity(context.Background(), s, n), s.err)
}
