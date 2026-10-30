package db

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/jackc/tern/v2/migrate"
	"github.com/stretchr/testify/require"
)

// A direct-SQL reader of address_stats is not queued behind migration 025 while another reader holds the table:
// the migration gives up the lock instead of waiting with every later reader behind it.
func TestMigration025DoesNotQueueAddressStatsReadersBehindItsLock(t *testing.T) {
	pool, _ := emptyTestDB(t)
	ctx := context.Background()
	migrateTo(t, pool, 24)

	holder, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback(ctx) }()
	var n int64
	require.NoError(t, holder.QueryRow(ctx, `SELECT count(*) FROM address_stats`).Scan(&n))

	done := make(chan error, 1)
	go func() {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			done <- err
			return
		}
		defer conn.Release()
		m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
		if err != nil {
			done <- err
			return
		}
		sub, _ := fs.Sub(migrations, "migrations")
		if err := m.LoadMigrations(sub); err != nil {
			done <- err
			return
		}
		done <- m.MigrateTo(ctx, 25)
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		_ = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%sent_count%')`).Scan(&waiting)
		return waiting
	}, 10*time.Second, 50*time.Millisecond, "025 waits for the reader")

	reader, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = reader.Rollback(ctx) }()
	_, err = reader.Exec(ctx, `SET LOCAL statement_timeout = '3s'`)
	require.NoError(t, err)
	_, err = reader.Exec(ctx, `SELECT count(*) FROM address_stats`)
	require.NoError(t, err, "a reader arriving while 025 waits for the table is served")

	_ = reader.Rollback(ctx)
	_ = holder.Rollback(ctx)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	}
}

// A deployment that ran with ledger compaction can still go back to 024: the merged span is a closed span like any
// other, and its rows are already the sums.
func TestMigrationsRevertTo024AfterLedgerCompaction(t *testing.T) {
	pool, _ := emptyTestDB(t)
	ctx := context.Background()
	migrateTo(t, pool, -1)
	d := &DB{pool: pool}
	seedHistory(t, d, 0, 16_000, 1)
	closeLedger(t, d)
	_, err := pool.Exec(ctx, `UPDATE prune_state SET pruned_through = 2999`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE ledger_spans SET check_state = 'ok', checked_at = now() WHERE hi <= history_from()`)
	require.NoError(t, err)
	_, ok, err := d.CompactLedger(ctx, 2)
	require.NoError(t, err)
	require.True(t, ok, "two verified pruned spans merge")

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	require.NoError(t, err)
	sub, err := fs.Sub(migrations, "migrations")
	require.NoError(t, err)
	require.NoError(t, m.LoadMigrations(sub))
	require.NoError(t, m.MigrateTo(ctx, 24), "the down path reaches 024 with a merged span")
}
