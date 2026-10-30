package indexer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A revert failed and set revertPending. On the next tick the check finds no fork: the delete had committed
// (a lost acknowledgement) or the node went back to the fork realtime stored. Nothing is left to revert, so the
// flag is cleared and a later jump to the head is allowed again.
func TestRevertPendingIsClearedWhenTheRetriedCheckFindsNoFork(t *testing.T) {
	ctx := context.Background()
	mockDB, node := new(MockDatabase), new(MockRPCClient)
	hash := func(n uint64) string { return fmt.Sprintf("0x%064x", n) }
	for n := uint64(90); n <= 100; n++ {
		mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: hash(n)}, nil).Maybe()
		node.On("RawBlockHash", mock.Anything, n).Return(hash(n), nil).Maybe()
	}
	mockDB.On("HighestBlockAtOrBelow", mock.Anything, uint64(100)).Return(&types.Block{Number: 100, Hash: hash(100)}, nil)
	node.On("RawBlockByNumber", mock.Anything, mock.Anything).Return(nil, errors.New("node unavailable")).Maybe()
	rt := NewRealtimeIndexer(mockDB, node, &RealtimeConfig{ConfirmationBlocks: 1}, &Config{RealtimeMaxBehind: 1_000},
		NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(100)
	rt.revertPending.Store(true)
	rt.failedRevertFrom = 100

	rt.indexUpTo(ctx, 100, 101)
	require.False(t, rt.revertPending.Load(), "the retried check found no fork")

	mockDB.On("SaveMissingRanges", mock.Anything, mock.Anything).Return(nil).Once()
	require.Equal(t, uint64(49_999), rt.jumpIfFarBehind(ctx, 50_000), "the jump to the head is allowed again")
}
