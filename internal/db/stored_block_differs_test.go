package db

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// Writing a block whose number is already stored is a no-op when it is the same block (another writer committed it
// first), and an error naming the number when it is a different one: that block is from a fork the chain has left.
func TestWritingADifferentBlockAtAStoredNumberIsAnError(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	stored := &types.Block{Number: 7, Hash: "0xaaa7", ParentHash: "0xaaa6", Timestamp: 1_700_000_007}
	require.NoError(t, d.InsertBlock(ctx, stored))

	require.NoError(t, d.InsertBlock(ctx, stored), "the same block again is a no-op")
	w, err := d.BeginBlockData(ctx, &BlockData{Block: stored, SkipAddressStats: true})
	require.NoError(t, err, "the same block again is a no-op")
	require.NoError(t, w.Finish(ctx))

	other := &types.Block{Number: 7, Hash: "0xbbb7", ParentHash: "0xbbb6", Timestamp: 1_700_000_007}
	var differs *StoredBlockDiffersError
	err = d.InsertBlock(ctx, other)
	require.True(t, errors.As(err, &differs), "InsertBlock: %v", err)
	require.Equal(t, uint64(7), differs.Number)
	_, err = d.BeginBlockData(ctx, &BlockData{Block: other, SkipAddressStats: true})
	require.True(t, errors.As(err, &differs), "BeginBlockData: %v", err)

	b, err := d.GetBlock(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, "0xaaa7", b.Hash, "the stored block is left as it is")
}
