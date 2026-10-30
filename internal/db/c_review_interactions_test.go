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

// A reorg reverts the head block, the only retained block of an address that the stats backfill is counting below
// START_BLOCK in the same moment. The revert reads that the address has no other block and deletes its row; the
// delete waits for the backfill batch, which holds the row it has just added the address's earlier counts to, and
// then deletes the row with those counts in it. The address keeps its backfilled history in the ledger stage and has
// no address_stats row.
func TestARevertKeepsTheCountsABackfillBatchAddsMeanwhile(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, d.InitStatsBackfill(ctx, 99, 1_700_000_099))
	for n := 100; n <= 105; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	dormant := rebaseNewAddress(105)

	first := NewSpanTotals()
	first.Blocks = 10
	require.NoError(t, d.CommitBackfillBatch(ctx, &BackfillBatch{From: 0, To: 10,
		Shared: &BlockData{AddressStats: map[string]*AddressStatsDelta{}},
		Spans:  map[uint64]*SpanTotals{0: first}, SpanHi: map[uint64]uint64{0: 100}}))

	// Holds the batch below after it wrote address_stats, as a long batch takes time over its stage and days.
	blocker, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer blocker.Rollback(context.Background())
	_, err = blocker.Exec(ctx, `SELECT 1 FROM ledger_stage_chain WHERE lo = 0 FOR UPDATE`)
	require.NoError(t, err)

	second := NewSpanTotals()
	second.Blocks, second.Transactions = 10, 1
	second.Addresses[dormant] = &AddressTotals{Txs: 1, Sent: 1, FirstSeen: 15, LastSeen: 15}
	backfilled := make(chan error, 1)
	go func() {
		backfilled <- d.CommitBackfillBatch(ctx, &BackfillBatch{From: 10, To: 20,
			Shared: &BlockData{AddressStats: map[string]*AddressStatsDelta{
				dormant: {Address: dormant, TxCountDelta: 1, SentDelta: 1, BlockNumber: 15}}},
			Spans: map[uint64]*SpanTotals{0: second}, SpanHi: map[uint64]uint64{0: 100}})
	}()
	require.Eventually(t, func() bool { return lockWaiters(t, d) == 1 }, 20*time.Second, 20*time.Millisecond,
		"the backfill batch holds the dormant address's row")

	reverted := make(chan error, 1)
	go func() { reverted <- d.DeleteBlock(ctx, 105) }()
	require.Eventually(t, func() bool { return lockWaiters(t, d) == 2 }, 20*time.Second, 20*time.Millisecond,
		"the revert waits for the batch's row")
	require.NoError(t, blocker.Rollback(ctx))
	require.NoError(t, <-backfilled)
	require.NoError(t, <-reverted)

	var txs, sent, firstSeen, lastSeen int64
	err = d.pool.QueryRow(ctx, `SELECT tx_count, sent_count, first_seen, last_seen FROM address_stats WHERE address = $1`, dormant).
		Scan(&txs, &sent, &firstSeen, &lastSeen)
	require.NoError(t, err, "the dormant address keeps a row for what the backfill counted below START_BLOCK")
	require.Equal(t, []int64{1, 1, 15, 15}, []int64{txs, sent, firstSeen, lastSeen})
	requireAddressStatsIsExactlyTheTruth(t, d)
}
