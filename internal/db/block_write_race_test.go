package db

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

func tipBlock(number uint64) *BlockData {
	tx, to := fmt.Sprintf("0xtip%d", number), "0xb1"
	return &BlockData{
		Block:        &types.Block{Number: number, Hash: fmt.Sprintf("0xblk%d", number), Timestamp: 1_700_000_000 + number},
		Transactions: []*types.Transaction{{Hash: tx, BlockNumber: number, From: "0xa1", To: &to, Value: "0", Status: 1}},
		AddressStats: map[string]*AddressStatsDelta{
			"0xa1": {Address: "0xa1", TxCountDelta: 1, BlockNumber: number},
			"0xb1": {Address: "0xb1", TxCountDelta: 1, BlockNumber: number},
		},
	}
}

func txCounts(t *testing.T, d *DB) (a, b, blocks, txs int64) {
	t.Helper()
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT (SELECT tx_count FROM address_stats WHERE address = '0xa1'),
		       (SELECT tx_count FROM address_stats WHERE address = '0xb1'),
		       (SELECT count FROM chain_counters WHERE name = 'blocks_total'),
		       (SELECT count FROM chain_counters WHERE name = 'transactions_total')`).Scan(&a, &b, &blocks, &txs))
	return a, b, blocks, txs
}

// Realtime stores the tip block; catch-up, which checked HasBlock before fetching it, writes it again after
// realtime committed. The second write must change nothing: its rows are there, and so are its deltas.
func TestABlockWrittenTwiceCountsOnce(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, tipBlock(100)))
	require.NoError(t, d.InsertBlockDataBatch(ctx, tipBlock(100)))
	a, b, blocks, txs := txCounts(t, d)
	require.Equal(t, [4]int64{1, 1, 1, 1}, [4]int64{a, b, blocks, txs}, "address tx_count, blocks_total, transactions_total")
}

// Realtime and up to three catch-up workers write the same block at once (seen on a production deployment). The
// block's hash is unique per partition but was not the insert's arbiter, so a writer that lost the race on the
// hash index failed with 23505 instead of finding the block stored.
func TestConcurrentWritersOfOneBlockNeitherFailNorCountTwice(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	const writers, blocks = 4, 150
	var failures []error
	var mu sync.Mutex
	for n := uint64(1_000); n < 1_000+blocks; n++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if err := d.InsertBlockDataBatch(ctx, tipBlock(n)); err != nil {
					mu.Lock()
					failures = append(failures, err)
					mu.Unlock()
				}
			}()
		}
		close(start)
		wg.Wait()
	}
	require.Empty(t, failures)
	var stored, counted int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM blocks), (SELECT sum(tx_count) FROM address_stats)`).Scan(&stored, &counted))
	require.Equal(t, int64(blocks), stored)
	require.Equal(t, int64(2*blocks), counted, "each block's two addresses counted once")
}
