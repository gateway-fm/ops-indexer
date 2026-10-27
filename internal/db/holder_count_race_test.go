package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A balance flush for a token whose tokens row a block write is creating must count its holder once:
// either the block write's seed sees the flush's balance, or the flush's delta lands on the row. The
// block write is held after its seed and before its commit (on the chain_counters row it updates last),
// and the flush runs inside that window.
func TestBalanceFlushDuringTheBlockWriteThatCreatesItsTokenCountsTheHolder(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	const token = "0x70c2"
	ts := uint64(1_700_000_001)
	to := "0xb"

	holder, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer holder.Rollback(ctx)
	_, err = holder.Exec(ctx, `SELECT 1 FROM chain_counters WHERE name = 'blocks_total' FOR UPDATE`)
	require.NoError(t, err)

	blockDone := make(chan error, 1)
	go func() {
		blockDone <- d.InsertBlockDataBatch(ctx, &BlockData{
			Block:        &types.Block{Number: 1, Hash: "b1", ParentHash: "b0", Timestamp: ts},
			Transactions: []*types.Transaction{{Hash: "t1", BlockNumber: 1, From: "0xa", To: &to, Value: "0", Status: 1}},
			Tokens:       []*types.Token{{Address: token, Symbol: "T", Decimals: 18, TokenType: types.TokenTypeERC20, BlockNumber: 1}},
			Transfers: []*types.TokenTransfer{{TxHash: "t1", LogIndex: 0, TokenAddress: token, From: "0xa", To: "0xb",
				Value: "1", BlockNumber: 1, Timestamp: &ts, TokenType: types.TokenTypeERC20}},
			SkipAddressStats: true,
		})
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity
				WHERE wait_event_type = 'Lock' AND query LIKE '%chain_counters%' AND pid <> pg_backend_pid())`).Scan(&waiting))
		return waiting
	}, 10*time.Second, 20*time.Millisecond, "the block write did not reach its chain_counters update")

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- d.InsertBalancesBatch(ctx, []*types.Balance{{Address: "0xb", TokenAddress: token, BlockNumber: 1, Balance: "1"}})
	}()
	select {
	case err := <-flushDone:
		flushDone <- err
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, holder.Rollback(ctx))
	require.NoError(t, <-blockDone)
	require.NoError(t, <-flushDone)

	var stored int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT holder_count FROM tokens WHERE address = $1`, token).Scan(&stored))
	actual, err := d.countTokenHolders(ctx, token)
	require.NoError(t, err)
	require.Equal(t, int64(1), actual)
	require.Equal(t, actual, stored, "holder_count")
}
