package indexer

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
)

func staleForkHash(fork, n uint64) common.Hash {
	return common.BytesToHash(new(big.Int).SetUint64(fork*1_000_000 + n).Bytes())
}

// Blocks 1..100 are stored, 91..100 from a fork the chain has left, and the node comes back at head 90, as after a
// failover to a node that is behind. The start check reads only up to the node head, and in collector mode realtime
// starts there. When the chain grows past 100, realtime's writes of 91..100 find another block stored at each
// number: they revert the stale blocks and store the chain's, instead of passing over them as already stored.
func TestStaleBlocksAboveTheNodeHeadAtStartAreReverted(t *testing.T) {
	for _, depth := range []int{1, 4} {
		t.Run(fmt.Sprintf("COMMIT_PIPELINE=%d", depth), func(t *testing.T) { staleBlocksAboveTheNodeHead(t, depth) })
	}
}

func staleBlocksAboveTheNodeHead(t *testing.T, depth int) {
	ctx := context.Background()
	d, _ := refillTestDB(t)

	ts := uint64(time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).Unix())
	for n := uint64(1); n <= 100; n++ {
		fork := uint64(1)
		if n > 90 {
			fork = 9
		}
		parentFork := fork
		if n == 91 {
			parentFork = 1
		}
		require.NoError(t, d.InsertBlock(ctx, &types.Block{Number: n, Hash: staleForkHash(fork, n).Hex(),
			ParentHash: staleForkHash(parentFork, n-1).Hex(), Timestamp: ts + n, Difficulty: "0", TotalDifficulty: "0"}))
	}

	mockRPC := new(MockRPCClient)
	for n := uint64(1); n <= 131; n++ {
		raw := &rpc.RawBlock{
			Number:     (*hexutil.Big)(new(big.Int).SetUint64(n)),
			Hash:       staleForkHash(1, n),
			ParentHash: staleForkHash(1, n-1),
			Timestamp:  hexutil.Uint64(ts + n),
		}
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(raw, nil).Maybe()
		mockRPC.On("RawBlockHash", mock.Anything, n).Return(staleForkHash(1, n).Hex(), nil).Maybe()
	}

	idx := NewWithConfig(d, mockRPC, time.Second, 0, &Config{RPCWorkers: 4, RPCRateLimit: 100, DBBatchSize: 100, CommitPipelineDepth: depth})
	// Start: lastIndexed 100, node head 90.
	head, err := idx.revertStaleHead(ctx, min(uint64(100), uint64(90)))
	require.NoError(t, err)
	require.Equal(t, uint64(90), head, "start check sees nothing stale at or below the node head")

	// Collector mode: realtime starts at the node head.
	rt := NewRealtimeIndexer(d, mockRPC, &RealtimeConfig{ConfirmationBlocks: 1}, idx.config,
		NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(90)
	for tick := 0; tick < 3 && rt.GetLastProcessedBlock() < 131; tick++ {
		rt.indexUpTo(ctx, rt.jumpIfFarBehind(ctx, 131), 131)
	}
	require.Equal(t, uint64(131), rt.GetLastProcessedBlock(), "realtime reaches the head within three ticks")

	var stale []uint64
	for n := uint64(91); n <= 100; n++ {
		b, err := d.GetBlock(ctx, n)
		require.NoError(t, err)
		require.NotNil(t, b)
		if b.Hash != staleForkHash(1, n).Hex() {
			stale = append(stale, n)
		}
	}
	require.Empty(t, stale, fmt.Sprintf("blocks still on the stale fork after realtime passed them: %v", stale))
}
