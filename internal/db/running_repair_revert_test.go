package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A chain counter at zero, below its truth: its repair is read before a reorg revert, which cannot lower it further,
// and written after it. The repair sets the counter to its truth under the counter's row lock, so the revert does not
// make it overshoot, and the next check finds nothing to quarantine.
func TestARunningCheckRepairOfAChainCounterAcrossARevertEndsAtTheTruth(t *testing.T) {
	d, cleanup := verifiedDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.Empty(t, checkAllBuckets(t, d, false))
	_, err := d.pool.Exec(ctx, `UPDATE chain_counters SET count = 0 WHERE name = 'transactions_total'`)
	require.NoError(t, err)

	blocker, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer blocker.Rollback(context.Background())
	_, err = blocker.Exec(ctx, `LOCK TABLE ledger_repairs IN EXCLUSIVE MODE`)
	require.NoError(t, err)
	checked := make(chan error, 1)
	go func() {
		_, err := d.CheckRunning(ctx, 0, true)
		checked <- err
	}()
	require.Eventually(t, func() bool {
		var n int
		_ = d.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n)
		return n == 1
	}, 20*time.Second, 20*time.Millisecond, "the repair has read the drift and waits before its row lock")

	require.NoError(t, d.DeleteBlock(ctx, 14_000), "a reorg reverts the head block while the repair waits")
	require.NoError(t, blocker.Rollback(ctx))
	require.NoError(t, <-checked)

	require.Empty(t, checkAllBuckets(t, d, false), "the counter is its truth after the revert")
	_, err = d.CheckRunning(ctx, 0, true)
	require.NoError(t, err)
	n, err := d.QuarantinedCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "nothing is quarantined")
}
