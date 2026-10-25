package db

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

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
