package db

import (
	"context"
	"io/fs"
	"testing"

	"github.com/jackc/tern/v2/migrate"
	"github.com/stretchr/testify/require"
)

// A rollback past 028 drops the stats backfill's state, its span in progress and its balance keys, and the downs
// above it drop more of what the backfill needs. Each down commits on its own, so with a backfill unfinished the
// rollback refuses at the newest down, before any down has run, naming the ways out: the schema stays at its version
// with every table, and once the backfill is complete it rolls back.
func TestMigration028DownRefusesAnUnfinishedStatsBackfill(t *testing.T) {
	pool, _ := emptyTestDB(t)
	ctx := context.Background()
	migrateTo(t, pool, -1)
	_, err := pool.Exec(ctx, `INSERT INTO stats_backfill (id, target, through) VALUES (1, 199, 99)`)
	require.NoError(t, err)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	require.NoError(t, err)
	sub, err := fs.Sub(migrations, "migrations")
	require.NoError(t, err)
	require.NoError(t, m.LoadMigrations(sub))
	var latest int32
	require.NoError(t, pool.QueryRow(ctx, `SELECT version FROM schema_version`).Scan(&latest))
	err = m.MigrateTo(ctx, 24)
	require.ErrorContains(t, err, "the stats backfill has not finished")
	require.ErrorContains(t, err, "pre-upgrade backup")
	var version int32
	require.NoError(t, pool.QueryRow(ctx, `SELECT version FROM schema_version`).Scan(&version))
	require.Equal(t, latest, version, "no down ran")
	var kept int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT (to_regclass('daily_stats_base') IS NOT NULL)::INT + (to_regclass('daily_active_base') IS NOT NULL)::INT
		     + (to_regclass('ledger_reverted_address') IS NOT NULL)::INT + (to_regclass('balance_rereads') IS NOT NULL)::INT
		     + (SELECT count(*) FROM information_schema.columns
		        WHERE table_name = 'stats_backfill' AND column_name IN ('last_ts', 'daily_done'))::INT`).Scan(&kept))
	require.Equal(t, 6, kept, "every table and column the backfill needs is still there")

	_, err = pool.Exec(ctx, `UPDATE stats_backfill SET through = target, completed_at = now()`)
	require.NoError(t, err)
	require.NoError(t, m.MigrateTo(ctx, 27), "a finished backfill rolls back")
}
