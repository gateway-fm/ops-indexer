package db

import (
	"context"
	"io/fs"
	"testing"

	"github.com/jackc/tern/v2/migrate"
	"github.com/stretchr/testify/require"
)

// 028 down drops the stats backfill's state, its span in progress and its balance keys: with a backfill unfinished it
// refuses, naming the ways out, and once the backfill is complete it rolls back.
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
	err = m.MigrateTo(ctx, 27)
	require.ErrorContains(t, err, "the stats backfill has not finished")
	require.ErrorContains(t, err, "pre-upgrade backup")

	_, err = pool.Exec(ctx, `UPDATE stats_backfill SET through = target, completed_at = now()`)
	require.NoError(t, err)
	require.NoError(t, m.MigrateTo(ctx, 27), "a finished backfill rolls back")
}
