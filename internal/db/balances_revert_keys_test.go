package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A revert reads the keys of its block before it takes their balance locks. A balance write at the reverted block for a
// key outside that read (a reread answered at the head, which is the block being reverted) that commits in between
// has its history row deleted by the revert, which then neither restores the key's current balance nor asks for it to
// be read again: the fork's value stays current, at a block that is gone.
func TestARevertRestoresOrRereadsABalanceWrittenAtItsBlockAfterItReadItsKeys(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	other := "0x00000000000000000000000000000000000000c3"
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(9)))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(9, [3]string{other, reorgBalToken, "5"})))
	require.NoError(t, d.InsertBlockDataBatch(ctx,
		reorgBalBlock(10, reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "1", nil))))

	flush, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer flush.Rollback(context.Background())
	_, err = flush.Exec(ctx, `SELECT pg_advisory_xact_lock(4669, $1)`, tbcLockKey(reorgBalToken, reorgBalB))
	require.NoError(t, err, "a balance flush holds a key of block 10")

	reverted := make(chan error, 1)
	go func() { reverted <- d.DeleteBlock(ctx, 10) }()
	require.Eventually(t, func() bool {
		var waiting bool
		_ = d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
			AND classid = 4669)`).Scan(&waiting)
		return waiting
	}, 20*time.Second, 20*time.Millisecond, "the revert has read its keys and waits for their locks")

	epoch, err := d.CurrentBalanceEpoch(ctx)
	require.NoError(t, err)
	b := reorgBalances(10, [3]string{other, reorgBalToken, "99"})
	b[0].Epoch = epoch
	require.NoError(t, d.InsertBalancesBatch(ctx, b), "a read at the head, block 10, of a key block 10 did not touch")
	require.NoError(t, flush.Rollback(ctx))
	require.NoError(t, <-reverted)

	var balance string
	var block int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT balance::TEXT, block_number FROM token_balances_current
		WHERE token_address = $1 AND address = $2`, reorgBalToken, other).Scan(&balance, &block))
	var reread bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM balance_rereads
		WHERE token_address = $1 AND address = $2 AND open)`, reorgBalToken, other).Scan(&reread))
	require.True(t, block < 10 || reread,
		"the key's current balance is %s at the reverted block %d, with no request to read it again", balance, block)
}
