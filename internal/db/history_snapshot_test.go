package db

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHistorySnapshotAcquiresGateBeforeRRSnapshot(t *testing.T) {
	d, done := setupTestDB(t)
	defer done()
	ctx := context.Background()
	blocker, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer blocker.Release()
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock($1,1)`, historyDDLGate)
	require.NoError(t, err)
	var old string
	require.NoError(t, blocker.QueryRow(ctx, `SELECT dataset_epoch::text FROM indexer_dataset_metadata WHERE id=1`).Scan(&old))
	type result struct {
		source string
		err    error
	}
	finished := make(chan result, 1)
	go func() {
		tx, cleanup, e := d.BeginHistorySnapshot(ctx)
		if e != nil {
			finished <- result{err: e}
			return
		}
		defer cleanup()
		var source string
		e = tx.QueryRow(ctx, `SELECT dataset_epoch::text FROM indexer_dataset_metadata WHERE id=1`).Scan(&source)
		finished <- result{source, e}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		e := blocker.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=$1 AND objid=1)`, historyDDLGate).Scan(&waiting)
		return e == nil && waiting
	}, time.Second, 5*time.Millisecond)
	var replaced string
	require.NoError(t, blocker.QueryRow(ctx, `UPDATE indexer_dataset_metadata SET dataset_epoch=gen_random_uuid() WHERE id=1 RETURNING dataset_epoch::text`).Scan(&replaced))
	require.NotEqual(t, old, replaced)
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_unlock($1,1)`, historyDDLGate)
	require.NoError(t, err)
	select {
	case got := <-finished:
		require.NoError(t, got.err)
		require.Equal(t, replaced, got.source, "RR must start after a queued destructive change, not before its lock wait")
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot remained blocked")
	}
}
func TestHistorySnapshotMarkerFailsClosedAndReleasesGate(t *testing.T) {
	d, done := setupTestDB(t)
	defer done()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `CREATE TABLE wipe_in_progress(started_at timestamptz)`)
	require.NoError(t, err)
	tx, cleanup, err := d.BeginHistorySnapshot(ctx)
	require.True(t, errors.Is(err, ErrHistoryWipeInProgress))
	require.Nil(t, tx)
	require.Nil(t, cleanup)
	assertHistoryGateFree(t, d)
}
func TestHistorySnapshotCancelledCleanupReleasesGate(t *testing.T) {
	d, done := setupTestDB(t)
	defer done()
	ctx, cancel := context.WithCancel(context.Background())
	tx, cleanup, err := d.BeginHistorySnapshot(ctx)
	require.NoError(t, err)
	var epoch string
	require.NoError(t, tx.QueryRow(ctx, `SELECT dataset_epoch::text FROM indexer_dataset_metadata WHERE id=1`).Scan(&epoch))
	cancel()
	cleanup()
	assertHistoryGateFree(t, d)
}
func TestHistorySnapshotCancelledGateWaitDoesNotLeak(t *testing.T) {
	d, done := setupTestDB(t)
	defer done()
	ctx := context.Background()
	blocker, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock($1,1)`, historyDDLGate)
	require.NoError(t, err)
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	tx, cleanup, err := d.BeginHistorySnapshot(short)
	require.Error(t, err)
	require.Nil(t, tx)
	require.Nil(t, cleanup)
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_unlock($1,1)`, historyDDLGate)
	require.NoError(t, err)
	blocker.Release()
	assertHistoryGateFree(t, d)
}
func assertHistoryGateFree(t *testing.T, d *DB) {
	t.Helper()
	ctx := context.Background()
	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	require.Eventually(t, func() bool {
		var free bool
		e := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1,1)`, historyDDLGate).Scan(&free)
		if e != nil || !free {
			return false
		}
		_, e = conn.Exec(ctx, `SELECT pg_advisory_unlock($1,1)`, historyDDLGate)
		return e == nil
	}, 2*time.Second, 5*time.Millisecond, "snapshot session gate leaked")
}

func TestHistorySnapshotLimitsPreserveOriginalCallerDeadline(t *testing.T) {
	d, done := setupTestDB(t)
	defer done()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, cleanup, err := d.BeginHistorySnapshotWithLimits(ctx, time.Second, 3*time.Second)
	require.NoError(t, err)
	defer cleanup()
	var lockLimit, statementLimit string
	require.NoError(t, tx.QueryRow(ctx, `SELECT current_setting('lock_timeout'),current_setting('statement_timeout')`).Scan(&lockLimit, &statementLimit))
	require.Equal(t, "1s", lockLimit)
	require.Equal(t, "3s", statementLimit)
	start := time.Now()
	_, err = tx.Exec(ctx, `SELECT pg_sleep(2)`)
	require.Error(t, err)
	require.Less(t, time.Since(start), 1500*time.Millisecond, "inner helper extended the caller's remaining budget")
}

func TestHistorySnapshotRestoresConfiguredSessionLockTimeout(t *testing.T) {
	d, done := setupTestDB(t)
	defer done()
	ctx := context.Background()
	cfg := d.pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "1700ms"
	cfg.MaxConns = 1
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer pool.Close()
	limited := &DB{pool: pool}
	for _, cancelled := range []bool{false, true} {
		var beforePID uint32
		var before string
		require.NoError(t, pool.QueryRow(ctx, `SELECT pg_backend_pid(),current_setting('lock_timeout')`).Scan(&beforePID, &before))
		require.Equal(t, "1700ms", before)
		callCtx, cancel := context.WithCancel(ctx)
		tx, cleanup, err := limited.BeginHistorySnapshotWithLimits(callCtx, time.Second, 3*time.Second)
		require.NoError(t, err)
		if cancelled {
			cancel()
		} else {
			require.NoError(t, tx.Commit(callCtx))
		}
		cleanup()
		cancel()
		var afterPID uint32
		var after string
		require.NoError(t, pool.QueryRow(ctx, `SELECT pg_backend_pid(),current_setting('lock_timeout')`).Scan(&afterPID, &after))
		require.Equal(t, beforePID, afterPID, "idle cleanup should preserve the same pooled connection")
		require.Equal(t, before, after, "snapshot must not weaken its connection's startup/role timeout")
	}
	blocker, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer blocker.Release()
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock(4673,1)`)
	require.NoError(t, err)
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, cleanup, err := limited.BeginHistorySnapshotWithLimits(short, time.Second, 3*time.Second)
	cancel()
	require.Error(t, err)
	require.Nil(t, cleanup)
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_unlock(4673,1)`)
	require.NoError(t, err)
	var after string
	require.NoError(t, pool.QueryRow(ctx, `SHOW lock_timeout`).Scan(&after))
	require.Equal(t, "1700ms", after)
	assertHistoryGateFree(t, d)
}

func TestHistorySnapshotPreservesStricterOperatorCaps(t *testing.T) {
	d, done := setupTestDB(t)
	defer done()
	ctx := context.Background()
	cfg := d.pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "50ms"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "80ms"
	cfg.MaxConns = 1
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer pool.Close()
	limited := &DB{pool: pool}
	blocker, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer blocker.Release()
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock(4673,1)`)
	require.NoError(t, err)
	callCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	start := time.Now()
	_, cleanup, err := limited.BeginHistorySnapshotWithLimits(callCtx, time.Second, 3*time.Second)
	require.Error(t, err)
	require.Nil(t, cleanup)
	var pg *pgconn.PgError
	require.ErrorAs(t, err, &pg)
	require.Equal(t, "55P03", pg.Code)
	require.NoError(t, callCtx.Err())
	require.Less(t, time.Since(start), 500*time.Millisecond, "configured50ms gate limit must win over requested1s")
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_unlock(4673,1)`)
	require.NoError(t, err)
	tx, cleanup, err := limited.BeginHistorySnapshotWithLimits(ctx, time.Second, 3*time.Second)
	require.NoError(t, err)
	var lock, statement string
	require.NoError(t, tx.QueryRow(ctx, `SELECT current_setting('lock_timeout'),current_setting('statement_timeout')`).Scan(&lock, &statement))
	require.Equal(t, "50ms", lock)
	require.Equal(t, "80ms", statement)
	require.NoError(t, tx.Commit(ctx))
	cleanup()
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_setting('lock_timeout'),current_setting('statement_timeout')`).Scan(&lock, &statement))
	require.Equal(t, "50ms", lock)
	require.Equal(t, "80ms", statement)
}
