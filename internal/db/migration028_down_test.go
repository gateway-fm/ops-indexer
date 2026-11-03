package db

import (
	"context"
	"fmt"
	"io/fs"
	"testing"

	"github.com/jackc/tern/v2/migrate"
	"github.com/stretchr/testify/require"
)

// A rollback past 028 drops the stats backfill's state, its span in progress and its balance keys, and the downs
// above it drop more of what the backfill needs. Each down commits on its own, so while the backfill is unfinished
// (its counting, or the balances it still has to read) the rollback refuses at the newest down, before any down has
// run, naming the ways out: the schema stays at its version with every table and every pending key. Once both are
// done it rolls back. 028's down refuses the same way for a database rolled back to it.
func TestMigration028DownRefusesAnUnfinishedStatsBackfill(t *testing.T) {
	unfinished := []struct {
		name  string
		state string
	}{
		{"counting unfinished", `INSERT INTO stats_backfill (id, target, through) VALUES (1, 199, 99)`},
		{"counting complete, balances pending", `
			INSERT INTO stats_backfill (id, target, through, completed_at) VALUES (1, 199, 199, now());
			INSERT INTO stats_backfill_balance_keys (token_address, address, block_number)
			VALUES ('0x00000000000000000000000000000000000b0020', '0x000000000000000000000000000000000000000a', 7)`},
	}
	for _, from := range []int32{-1, 28} {
		for _, u := range unfinished {
			t.Run(fmt.Sprintf("from %d, %s", from, u.name), func(t *testing.T) {
				pool, _ := emptyTestDB(t)
				ctx := context.Background()
				migrateTo(t, pool, from)
				_, err := pool.Exec(ctx, u.state)
				require.NoError(t, err)

				conn, err := pool.Acquire(ctx)
				require.NoError(t, err)
				defer conn.Release()
				m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
				require.NoError(t, err)
				sub, err := fs.Sub(migrations, "migrations")
				require.NoError(t, err)
				require.NoError(t, m.LoadMigrations(sub))
				var before int32
				require.NoError(t, pool.QueryRow(ctx, `SELECT version FROM schema_version`).Scan(&before))
				pending := func() (n int) {
					require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM stats_backfill_balance_keys`).Scan(&n))
					return n
				}
				keys := pending()

				err = m.MigrateTo(ctx, 24)
				require.ErrorContains(t, err, "the stats backfill has not finished")
				require.ErrorContains(t, err, "pre-upgrade backup")
				var version int32
				require.NoError(t, pool.QueryRow(ctx, `SELECT version FROM schema_version`).Scan(&version))
				require.Equal(t, before, version, "no down ran")
				require.Equal(t, keys, pending(), "the pending balance keys are kept")
				if from < 0 {
					var kept int
					require.NoError(t, pool.QueryRow(ctx, `
						SELECT (to_regclass('daily_stats_base') IS NOT NULL)::INT + (to_regclass('daily_active_base') IS NOT NULL)::INT
						     + (to_regclass('ledger_reverted_address') IS NOT NULL)::INT + (to_regclass('balance_rereads') IS NOT NULL)::INT
						     + (SELECT count(*) FROM information_schema.columns
						        WHERE table_name = 'stats_backfill' AND column_name IN ('last_ts', 'daily_done'))::INT`).Scan(&kept))
					require.Equal(t, 6, kept, "every table and column the backfill needs is still there")
				}

				_, err = pool.Exec(ctx, `
					UPDATE stats_backfill SET through = target, completed_at = now();
					DELETE FROM stats_backfill_balance_keys`)
				require.NoError(t, err)
				require.NoError(t, m.MigrateTo(ctx, 24), "a finished backfill rolls back")
			})
		}
	}
}
