package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrHistoryWipeInProgress = errors.New("indexed dataset is being replaced")

// BeginHistorySnapshot holds the shared history DDL gate BEFORE beginning RR.
// Taking a transaction advisory lock from a SELECT inside RR would pin its
// snapshot before a blocked lock is acquired, and PostgreSQL TRUNCATE is not
// MVCC-safe. A session lock on the acquired connection avoids that old snapshot.
// Call cleanup after committing or on every error; it rolls back before unlock.
func (d *DB) BeginHistorySnapshot(ctx context.Context) (pgx.Tx, func(), error) {
	return d.BeginHistorySnapshotWithLimits(ctx, 5*time.Second, 5*time.Second)
}

// BeginHistorySnapshotWithLimits applies smaller per-operation server limits.
// The original caller context remains the enclosing budget: none is extended.
func (d *DB) BeginHistorySnapshotWithLimits(ctx context.Context, lockTimeout, statementTimeout time.Duration) (pgx.Tx, func(), error) {
	if lockTimeout < time.Millisecond || lockTimeout > 5*time.Second || statementTimeout < time.Millisecond || statementTimeout > 5*time.Second {
		return nil, nil, errors.New("history snapshot limits must be between1ms and5s")
	}
	waitCtx, cancel := context.WithTimeout(ctx, lockTimeout)
	defer cancel()
	conn, err := d.pool.Acquire(waitCtx)
	if err != nil {
		return nil, nil, err
	}
	var previousLockTimeout string
	var previousLockMS, previousStatementMS int64
	if err = conn.QueryRow(waitCtx, `SELECT current_setting('lock_timeout'),(SELECT setting::bigint FROM pg_settings WHERE name='lock_timeout'),(SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout')`).Scan(&previousLockTimeout, &previousLockMS, &previousStatementMS); err != nil {
		conn.Release()
		return nil, nil, err
	}
	// A configured nonzero operator cap can tighten these bounds, never loosen.
	if previousLockMS > 0 && previousLockMS < lockTimeout.Milliseconds() {
		lockTimeout = time.Duration(previousLockMS) * time.Millisecond
	}
	if previousStatementMS > 0 && previousStatementMS < statementTimeout.Milliseconds() {
		statementTimeout = time.Duration(previousStatementMS) * time.Millisecond
	}
	var tx pgx.Tx
	cleanup := func() {
		cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if tx != nil {
			_ = tx.Rollback(cleanCtx)
		}
		_, unlockErr := conn.Exec(cleanCtx, `SELECT pg_advisory_unlock_shared($1,1)`, historyDDLGate)
		_, resetErr := conn.Exec(cleanCtx, `SELECT set_config('lock_timeout',$1,false)`, previousLockTimeout)
		if unlockErr != nil || resetErr != nil {
			raw := conn.Hijack()
			closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer closeCancel()
			_ = raw.Close(closeCtx)
			return
		}
		conn.Release()
	}
	// A client-side cancellation may close its socket before the server stops
	// waiting. Bound that server-side waiter as well, then restore session state.
	if _, err = conn.Exec(waitCtx, fmt.Sprintf(`SET lock_timeout='%dms'`, lockTimeout.Milliseconds())); err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err = conn.Exec(waitCtx, `SELECT pg_advisory_lock_shared($1,1)`, historyDDLGate); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("acquire history snapshot gate: %w", err)
	}
	if _, err = conn.Exec(ctx, `SELECT set_config('lock_timeout',$1,false)`, previousLockTimeout); err != nil {
		cleanup()
		return nil, nil, err
	}
	tx, err = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout='%dms'; SET LOCAL lock_timeout='%dms'`, statementTimeout.Milliseconds(), lockTimeout.Milliseconds())); err != nil {
		cleanup()
		return nil, nil, err
	}
	var wiping bool
	if err = tx.QueryRow(ctx, `SELECT to_regclass('public.wipe_in_progress') IS NOT NULL`).Scan(&wiping); err != nil {
		cleanup()
		return nil, nil, err
	}
	if wiping {
		cleanup()
		return nil, nil, ErrHistoryWipeInProgress
	}
	return tx, cleanup, nil
}
