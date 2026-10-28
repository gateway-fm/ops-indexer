package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// waitsWhileHeld holds lock in its own transaction, starts write, and requires that write waits for the lock and
// finishes once it is released.
func waitsWhileHeld(t *testing.T, d *DB, lock string, args []any, write func() error) {
	t.Helper()
	ctx := context.Background()
	holder, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer holder.Rollback(ctx)
	_, err = holder.Exec(ctx, lock, args...)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- write() }()
	select {
	case err := <-done:
		t.Fatalf("the write did not wait for the advisory lock (err %v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, holder.Rollback(ctx))
	require.NoError(t, <-done)
}

// A block write takes the advisory key of every token it touches, also one whose tokens row does
// not exist yet, so two writers creating the same token serialise instead of deadlocking.
func TestBlockWriteTakesTheTokenAdvisoryKey(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ts := uint64(1_700_000_001)
	to := "0xb"
	waitsWhileHeld(t, d, `SELECT pg_advisory_xact_lock($1)`, []any{tokenLockKeys([]string{"0x70c1"})[0]}, func() error {
		return d.InsertBlockDataBatch(context.Background(), &BlockData{
			Block:        &types.Block{Number: 1, Hash: "b1", ParentHash: "b0", Timestamp: ts},
			Transactions: []*types.Transaction{{Hash: "t1", BlockNumber: 1, From: "0xa", To: &to, Value: "0", Status: 1}},
			Transfers: []*types.TokenTransfer{{TxHash: "t1", LogIndex: 0, TokenAddress: "0x70c1", From: "0xa", To: "0xb",
				Value: "1", BlockNumber: 1, Timestamp: &ts, TokenType: types.TokenTypeERC20}},
			SkipAddressStats: true,
		})
	})
}
