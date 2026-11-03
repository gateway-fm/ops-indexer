package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A live block and a backfill batch create one contract address (a CREATE2 again after the child destroyed itself).
// Ingest's Finish takes the contract first and the counters after it; a backfill batch that took the counters before
// the contract deadlocked with it, and Postgres aborted one of them.
func TestABackfillBatchTakesAContractBeforeTheCounters(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const child = "0x00000000000000000000000000000000000c41d0"
	require.NoError(t, d.InitStatsBackfill(ctx, 99, 1))

	live, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer live.Rollback(context.Background())
	_, err = live.Exec(ctx, `INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number, is_verified)
		VALUES ($1, '6080', '0xb', '0xc8', 200, false) ON CONFLICT (address) DO NOTHING`, child)
	require.NoError(t, err)

	span := NewSpanTotals()
	span.Blocks = 10
	committed := make(chan error, 1)
	go func() {
		committed <- d.CommitBackfillBatch(ctx, &BackfillBatch{From: 0, To: 10,
			Shared:    &BlockData{AddressStats: map[string]*AddressStatsDelta{}},
			Contracts: []*types.Contract{{Address: child, Bytecode: "6080", Creator: "0xb", CreationTx: "0x05", BlockNumber: 5}},
			Spans:     map[uint64]*SpanTotals{0: span}, SpanHi: map[uint64]uint64{0: 100}})
	}()
	require.Eventually(t, func() bool {
		var waiting int
		_ = d.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND datname = current_database()`).Scan(&waiting)
		return waiting == 1
	}, 10*time.Second, 20*time.Millisecond, "the backfill batch waits on the live block's contract")

	_, err = live.Exec(ctx, `
		INSERT INTO chain_counters (name, count, updated_at) VALUES ('blocks_total', 1, NOW())
		ON CONFLICT (name) DO UPDATE SET count = chain_counters.count + 1, updated_at = NOW()`)
	require.NoError(t, err, "the live block takes the counters after its contract")
	require.NoError(t, live.Commit(ctx))
	require.NoError(t, <-committed)

	var rows int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM contracts WHERE address = $1`, child).Scan(&rows))
	require.Equal(t, 1, rows)
}
