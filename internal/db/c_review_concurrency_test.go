package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// waitForLockWait waits until some backend waits for a lock on relation.
func waitForLockWait(t *testing.T, d *DB, relation string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		require.NoError(t, d.pool.QueryRow(context.Background(), `
			SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted AND relation = to_regclass($1))`, relation).Scan(&waiting))
		if waiting {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("nothing waited for a lock on %s", relation)
}

// The retained recount picks the oldest unchecked retained span in its REPEATABLE READ snapshot, then counts the span's
// history rows statement by statement. Its first statement (the span's token_transfers) takes minutes on a large span.
// Retention prunes the span's range meanwhile: the watermark moves, and DETACH CONCURRENTLY of blocks and transactions
// waits only for transactions that already use those tables, which the recount does not yet. The recount's later
// statements then read no blocks and no transactions for the span, and the settle "repairs" the span's ledger rows to
// that count.
func TestRetainedRecountOfASpanRetentionPrunesMeanwhile(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000, DetachWait: time.Second, DropGateWait: 5 * time.Second}

	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 82_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	require.Equal(t, uint64(60_000), prunedThrough(t, d))

	// One more range becomes prunable: [60001, 62001), whose first span the recount picks next.
	seedHistory(t, d, 80_002, 82_001, 1)
	closeLedger(t, d)
	var lo, blocks, txs int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT s.lo, (SELECT value FROM ledger_chain WHERE lo = s.lo AND name = 'blocks_total'),
		       (SELECT value FROM ledger_chain WHERE lo = s.lo AND name = 'transactions_total')
		FROM ledger_spans s WHERE s.lo >= history_from() ORDER BY s.checked_at NULLS FIRST, s.lo LIMIT 1`).Scan(&lo, &blocks, &txs))
	require.Equal(t, int64(60_001), lo)
	require.Equal(t, int64(999), blocks)

	// The span's token_transfers read is slow (here: held up by a lock on its partition).
	admin, err := pgx.Connect(ctx, d.pool.Config().ConnConfig.ConnString())
	require.NoError(t, err)
	defer admin.Close(ctx)
	hold, err := admin.Begin(ctx)
	require.NoError(t, err)
	_, err = hold.Exec(ctx, `LOCK TABLE token_transfers_p60001 IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)

	type result struct {
		c   SpanCheck
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		c, ok, err := d.RecountRetainedSpan(ctx, true, "")
		done <- result{c, ok, err}
	}()
	waitForLockWait(t, d, "token_transfers_p60001")

	require.NoError(t, d.retentionPass(ctx, cfg))
	require.Equal(t, uint64(62_000), prunedThrough(t, d), "retention pruned the recount's range")
	require.NoError(t, hold.Rollback(ctx))

	r := <-done
	require.NoError(t, r.err)
	require.True(t, r.ok)
	require.Equal(t, int64(60_001), r.c.Lo)

	var gotBlocks, gotTxs int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT value FROM ledger_chain WHERE lo = $1 AND name = 'blocks_total'),
		       (SELECT value FROM ledger_chain WHERE lo = $1 AND name = 'transactions_total')`, lo).Scan(&gotBlocks, &gotTxs))
	t.Logf("recount: drifted=%d state=%s; ledger blocks_total %d -> %d, transactions_total %d -> %d",
		r.c.Drifted, r.c.State, blocks, gotBlocks, txs, gotTxs)
	counter := func() (n int64) {
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT count FROM chain_counters WHERE name = 'blocks_total'`).Scan(&n))
		return n
	}
	before := counter()
	_, err = d.CheckRunning(ctx, 0, true)
	require.NoError(t, err)
	t.Logf("the running check then moves blocks_total %d -> %d (blocks stored or pruned: 82002)", before, counter())
	require.Equal(t, blocks, gotBlocks, "the span's blocks_total in the ledger")
	require.Equal(t, txs, gotTxs, "the span's transactions_total in the ledger")
	require.Zero(t, r.c.Drifted, "the span was counted right when it closed")
}
