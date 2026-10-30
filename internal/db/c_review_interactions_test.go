package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// lockWaiters counts the backends of this database waiting on a row or transaction lock.
func lockWaiters(t *testing.T, d *DB) int {
	t.Helper()
	var n int
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND wait_event_type = 'Lock' AND wait_event IN ('transactionid', 'tuple')`).Scan(&n))
	return n
}

// The running check's repair of a chain counter and a token counter, found in the same check, must take the rows in
// the writer's order: tokens before chain_counters, as Finish (writeSharedRows) and DeleteBlock do. A block write that
// holds the token's row and then adds to chain_counters must not deadlock with it.
func TestRunningCheckRepairTakesTokensBeforeChainCounters(t *testing.T) {
	d, cleanup := verifiedDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.Empty(t, checkAllBuckets(t, d, false))
	_, err := d.pool.Exec(ctx, `
		UPDATE chain_counters SET count = count + 1 WHERE name = 'transactions_total';
		UPDATE tokens SET transfer_count = transfer_count + 1 WHERE address = '`+retentionToken+`'`)
	require.NoError(t, err)

	// A block write at Finish, in writeSharedRows' order: the token's advisory key and row first.
	w, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer w.Rollback(context.Background())
	_, err = w.Exec(ctx, `SELECT pg_advisory_xact_lock(k) FROM unnest($1::bigint[]) AS k`, tokenLockKeys([]string{retentionToken}))
	require.NoError(t, err)
	_, err = w.Exec(ctx, `SELECT 1 FROM tokens WHERE address = ANY($1::text[]) ORDER BY address FOR UPDATE`, []string{retentionToken})
	require.NoError(t, err)

	checked := make(chan error, 1)
	go func() {
		_, err := d.CheckRunning(ctx, 0, true)
		checked <- err
	}()
	require.Eventually(t, func() bool { return lockWaiters(t, d) == 1 }, 20*time.Second, 20*time.Millisecond,
		"the repair waits for the token row the block write holds")

	// ... and chain_counters last.
	_, werr := w.Exec(ctx, `
		INSERT INTO chain_counters (name, count, updated_at) VALUES ('transactions_total', 1, NOW())
		ON CONFLICT (name) DO UPDATE SET count = chain_counters.count + EXCLUDED.count, updated_at = NOW()`)
	if werr == nil {
		werr = w.Commit(ctx)
	}
	cerr := <-checked
	require.NoError(t, werr, "the block write")
	require.NoError(t, cerr, "the running check's repair")
}

// repair-address-stats runs next to the live indexer, where every span close rebases the span's addresses under the
// rebase lock for minutes (7 min 31 s for a span at 600 tx/s, PR-C.md). A repair batch that only waits for that lock
// has not run long: it must wait the span's rebase out and repair, not stop the repair.
func TestAddressStatsRepairWaitsOutASpanCloseRebase(t *testing.T) {
	d, ctx := repairFixture(t)
	repairBatchTimeout = 500 * time.Millisecond
	t.Cleanup(func() { repairBatchTimeout = AddressRepairBatchTimeout })

	// What rebasePendingSpans holds on its connection while a span's addresses are rebased.
	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1, 1)`, rebaseLock)
	require.NoError(t, err)
	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(4 * repairBatchTimeout)
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1, 1)`, rebaseLock)
		conn.Release()
	}()
	defer func() { <-released }()

	for bucket := 0; bucket < AddressBuckets; bucket++ {
		_, err := d.RepairAddressBucket(ctx, bucket, AddressRepairBatch)
		require.NoError(t, err, "bucket %d while a span close's rebase holds the rebase lock", bucket)
	}
	requireAddressStatsIsExactlyTheTruth(t, d)
}
