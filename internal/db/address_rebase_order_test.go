package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A rebase batch locks every address_stats row of its batch at once, in address order (revertedSinceRead), and only
// then creates the rows missing at its read. A missing address between two locked ones is taken after a higher one, out
// of the order ingest takes them: a block write that creates that address and then moves the higher one waits for the
// rebase while the rebase waits for it, and Postgres aborts one of them as a deadlock.
func TestARebaseCreatingAMissingRowKeepsIngestsAddressOrder(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	low := "0x0000000000000000000000000000000000000010"
	mid := "0x0000000000000000000000000000000000000020"
	high := "0x0000000000000000000000000000000000000030"
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{
		Block: &types.Block{Number: 5, Hash: "0xb5", ParentHash: "0xb4", Timestamp: 1_700_000_005},
		Transactions: []*types.Transaction{
			{Hash: "0xt1", BlockNumber: 5, TxIndex: 0, From: low, To: &high, Value: "0", Status: 1},
			{Hash: "0xt2", BlockNumber: 5, TxIndex: 1, From: mid, To: &low, Value: "0", Status: 1},
		},
	}))
	_, err := d.pool.Exec(ctx, `INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count,
		sent_count, first_seen, last_seen, is_contract) VALUES ($1, 2, 0, 0, 1, 5, 5, false), ($2, 1, 0, 0, 0, 5, 5, false)`, low, high)
	require.NoError(t, err, "the low and high rows are right; the middle one is missing")

	var ingest pgx.Tx
	var once sync.Once
	rebaseReadHook = func([]string) {
		once.Do(func() {
			tx, err := d.pool.Begin(ctx)
			require.NoError(t, err)
			ingest = tx
			_, err = tx.Exec(ctx, `INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count,
				sent_count, first_seen, last_seen, is_contract) VALUES ($1, 1, 0, 0, 1, 6, 6, false)
				ON CONFLICT (address) DO UPDATE SET tx_count = address_stats.tx_count + 1`, mid)
			require.NoError(t, err, "a block write creates the middle row, first in its address order")
		})
	}
	t.Cleanup(func() { rebaseReadHook = nil })

	rebased := make(chan error, 1)
	go func() {
		_, err := d.RebaseAddresses(ctx, []string{low, mid, high}, []string{mid})
		rebased <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		_ = d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%INSERT INTO address_stats AS s%')`).Scan(&waiting)
		return waiting
	}, 20*time.Second, 20*time.Millisecond, "the rebase waits to create the middle row")

	_, ingestErr := ingest.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 1 WHERE address = $1`, high)
	if ingestErr == nil {
		ingestErr = ingest.Commit(ctx)
	} else {
		_ = ingest.Rollback(ctx)
	}
	rebaseErr := <-rebased
	require.NoError(t, ingestErr, "the block write, moving the high row next")
	require.NoError(t, rebaseErr, "the rebase")
}
