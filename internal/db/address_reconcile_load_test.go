package db

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// The reconciliation and the running check repaired under REPEATABLE READ, where any row ingest commits
// after the snapshot fails the repair with 40001; with a writer on a hot address none ever committed. They now repair
// like the span rebase, in READ COMMITTED batches that take their rows before counting, and finish under ingest.
func TestAddressRepairsFinishUnderIngest(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 20; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	var stop atomic.Bool
	wrote := make(chan error, 1)
	go func() {
		for n := 21; !stop.Load(); n++ {
			if err := d.InsertBlockDataBatch(ctx, rebaseBlock(n)); err != nil {
				wrote <- err
				return
			}
		}
		wrote <- nil
	}()
	for attempt := 0; attempt < 10; attempt++ {
		_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 1 WHERE address = $1`, rebaseHot)
		require.NoError(t, err, "a drift on the hot address")
		_, err = d.pool.Exec(ctx, `UPDATE ledger_verify_state SET value = 0 WHERE name = 'address_reconcile_next'`)
		require.NoError(t, err)
		bucket, drifted, _, err := d.ReconcileAddressBucket(ctx)
		require.NoError(t, err, "attempt %d: the reconciliation commits while ingest writes the hot address", attempt)
		require.Zero(t, bucket)
		require.Positive(t, drifted)
		_, err = d.CheckRunning(ctx, 0, true)
		require.NoError(t, err, "attempt %d: the running check's repair commits too", attempt)
	}
	stop.Store(true)
	require.NoError(t, <-wrote)
	requireAddressStatsIsTheTruth(t, d)
}

// The running check recorded an address's repair before rebasing it, so a rebase that failed left a repair
// on record, and the next check quarantined the address instead of repairing it. The record now commits with the
// rebase.
func TestARunningRepairWhoseRebaseFailsIsDoneByTheNextCheck(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 5; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 3 WHERE address = $1`, rebaseHot)
	require.NoError(t, err)

	held, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer held.Rollback(ctx)
	_, err = held.Exec(ctx, `SELECT 1 FROM address_stats WHERE address = $1 FOR UPDATE`, rebaseHot)
	require.NoError(t, err)
	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = d.CheckRunning(short, 0, true)
	cancel()
	require.Error(t, err, "the rebase waits for the held row past the deadline")
	require.NoError(t, held.Rollback(ctx))

	_, err = d.CheckRunning(ctx, 0, true)
	require.NoError(t, err)
	n, err := d.QuarantinedCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "repaired, not quarantined")
	requireAddressStatsIsTheTruth(t, d)
}

// The running diff of one address bucket read every row above the closed spans and kept the bucket's 1/256
// after the union (10 minutes a bucket on a 6.4M-transaction tail), and read ledger_address from the bucket
// to the end. The bucket's range now bounds each history table's address index scan, and ledger_address's.
func TestRunningDiffOfABucketReadsOnlyItsAddresses(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	tx, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off`)
	require.NoError(t, err)
	rows, err := tx.Query(ctx, `EXPLAIN SELECT * FROM ledger_running_diff_address(3)`)
	require.NoError(t, err)
	plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	text := strings.Join(plan, "\n")
	require.Regexp(t, `Index Cond: \(\(from_address >= '0x03'::text\) AND \(from_address < '0x04'::text\)`, text,
		"the bucket bounds the transactions' address index")
	require.Regexp(t, `Index Cond: \(\(address >= '0x03'::text\) AND \(address < '0x04'::text\)\)`, text,
		"and ledger_address's, at both ends")
}
