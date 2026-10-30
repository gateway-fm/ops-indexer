package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// After a revert, the key's reread is read from the node; a new block then moves the key and its own read is made
// later, but is flushed first (another balance worker, or the ingest's sync read while the reread loop is still
// reading the rest of its claim). That later read holds the newer value, so it must not be lost to the older reread.
func TestAReadMadeAfterTheRereadIsNotLostToIt(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, zeroAddress, reorgBalA, "10", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(2,
		[3]string{reorgBalA, reorgBalToken, "6"}, [3]string{reorgBalB, reorgBalToken, "4"})))
	require.NoError(t, d.DeleteBlock(ctx, 2))
	seq := rereads(t, d)[reorgBalToken+" "+reorgBalA]
	require.NotZero(t, seq)

	// The reread is claimed at head 1 and read: A holds 10 on the new chain.
	reread := reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})
	reread[0].Reread = seq

	// Then the new chain's block 2 sends 3 from A to X, and A's read for it (7) is flushed before the reread.
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalX, "3", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(2,
		[3]string{reorgBalA, reorgBalToken, "7"}, [3]string{reorgBalX, reorgBalToken, "3"})))
	require.NoError(t, d.InsertBalancesBatch(ctx, reread))

	b, _ := currentBalance(t, d, reorgBalToken, reorgBalA)
	require.Equal(t, "7", b, "A's balance after the new chain's block 2")
}

// A read made on the fork before the revert and flushed after the key's reread has answered it carries the fork's
// value and the reverted block's number, which is above the reread's: it must not replace the reread.
func TestAForkReadFlushedAfterTheRereadDoesNotReplaceIt(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(1,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, zeroAddress, reorgBalA, "10", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(1, [3]string{reorgBalA, reorgBalToken, "10"})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(2,
		reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "4", nil))))
	// B's read for the fork's block 2 is made (4) but is still in a worker's batch when the block is reverted.
	forkRead := reorgBalances(2, [3]string{reorgBalB, reorgBalToken, "4"})
	require.NoError(t, d.DeleteBlock(ctx, 2))
	seq := rereads(t, d)[reorgBalToken+" "+reorgBalB]
	require.NotZero(t, seq)

	answer := reorgBalances(1, [3]string{reorgBalB, reorgBalToken, "0"})
	answer[0].Reread = seq
	require.NoError(t, d.InsertBalancesBatch(ctx, answer))
	require.NoError(t, d.InsertBalancesBatch(ctx, forkRead))

	b, _ := currentBalance(t, d, reorgBalToken, reorgBalB)
	require.Equal(t, "0", b, "B holds nothing on the new chain")
	require.Equal(t, 1, holderCount(t, d, reorgBalToken), "A is the only holder")
}
