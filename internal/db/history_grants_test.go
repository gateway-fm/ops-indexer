package db

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
	"github.com/stretchr/testify/require"
)

// emptyTestDB returns a pool on a new, unmigrated database of the shared test server, and its URL.
func emptyTestDB(t *testing.T) (*pgxpool.Pool, *url.URL) {
	t.Helper()
	ctx := context.Background()
	sharedPGOnce.Do(func() { sharedPGURL, sharedPGErr = startSharedPG(ctx) })
	if sharedPGErr != nil {
		if sharedPGURL == nil && strings.Contains(sharedPGErr.Error(), "could not start postgres container") {
			t.Skipf("skipping: %v", sharedPGErr)
		}
		t.Fatal(sharedPGErr)
	}
	name := fmt.Sprintf("empty_%d", testDBSeq.Add(1))
	admin := *sharedPGURL
	admin.Path = "/postgres"
	conn, err := pgx.Connect(ctx, admin.String())
	require.NoError(t, err)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE template0")
	require.NoError(t, err)
	own := *sharedPGURL
	own.Path = "/" + name
	pool, err := pgxpool.New(ctx, own.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		pool.Close()
		if conn, err := pgx.Connect(ctx, admin.String()); err == nil {
			_, _ = conn.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
			conn.Close(ctx)
		}
	})
	return pool, &own
}

// migrateTo migrates to version, or to the newest migration with version < 0.
func migrateTo(t *testing.T, pool *pgxpool.Pool, version int32) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	require.NoError(t, err)
	sub, err := fs.Sub(migrations, "migrations")
	require.NoError(t, err)
	require.NoError(t, m.LoadMigrations(sub))
	if version < 0 {
		require.NoError(t, m.Migrate(ctx))
	} else {
		require.NoError(t, m.MigrateTo(ctx, version))
	}
}

// grantsOn lists the privileges granted on a table to roles other than its owner, as "role:PRIVILEGE[*]" (* with
// grant option).
func grantsOn(t *testing.T, pool *pgxpool.Pool, table string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END || ':' || a.privilege_type ||
		       CASE WHEN a.is_grantable THEN '*' ELSE '' END
		FROM pg_class c, aclexplode(c.relacl) a
		WHERE c.oid = $1::regclass AND a.grantee <> c.relowner`, table)
	require.NoError(t, err)
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	sort.Strings(out)
	return out
}

// The privileges granted on the history tables before they were partitioned reach the partitioned tables again,
// exactly: what each table had, on its parent and every partition, including partitions created before and after
// the restoring migration. Nothing else is granted.
func TestHistoryTablesKeepTheirGrantsAcrossPartitioning(t *testing.T) {
	pool, dbURL := emptyTestDB(t)
	ctx := context.Background()
	migrateTo(t, pool, 13)

	suffix := strings.TrimPrefix(dbURL.Path, "/")
	reader, writer := "reader_"+suffix, "writer_"+suffix
	for _, role := range []string{reader, writer} {
		_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'pw'`, role))
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.WithoutCancel(ctx), fmt.Sprintf(`DROP OWNED BY %[1]s; DROP ROLE %[1]s`, role))
		})
	}
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		GRANT SELECT ON ALL TABLES IN SCHEMA public TO %[1]s;
		GRANT INSERT, UPDATE ON logs TO %[2]s WITH GRANT OPTION;
		GRANT SELECT ON blocks TO PUBLIC`, reader, writer))
	require.NoError(t, err)
	before := map[string][]string{}
	for _, tbl := range HistoryTables {
		before[tbl] = grantsOn(t, pool, tbl)
	}

	// A deployment that ran 014..022 and cut its history before the restoring migration.
	migrateTo(t, pool, 22)
	d := &DB{pool: pool}
	seedHistory(t, d, 0, 40_001, 1)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	require.NoError(t, d.retentionPass(ctx, cfg))

	migrateTo(t, pool, -1)
	ensurePartitions(t, d, 50_000, cfg)

	var partitions []string
	rows, err := pool.Query(ctx, `SELECT suffix FROM history_partitions ORDER BY lo NULLS FIRST`)
	require.NoError(t, err)
	partitions, err = pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Greater(t, len(partitions), 2, "the legacy partition, one made by the cut and some made after")
	for _, tbl := range HistoryTables {
		require.Equal(t, before[tbl], grantsOn(t, pool, tbl), "%s: the parent has what the table had", tbl)
		for _, p := range partitions {
			require.Equal(t, before[tbl], grantsOn(t, pool, tbl+"_"+p), "%s partition %s", tbl, p)
		}
	}

	u := *dbURL
	u.User = url.UserPassword(reader, "pw")
	c, err := pgx.Connect(ctx, u.String())
	require.NoError(t, err)
	defer c.Close(ctx)
	for _, tbl := range HistoryTables {
		_, err := c.Exec(ctx, "SELECT count(*) FROM "+tbl)
		require.NoError(t, err, "%s readable", tbl)
	}
	_, err = c.Exec(ctx, `INSERT INTO logs (tx_hash, log_index, address, topic0, data, block_number, timestamp)
		VALUES ('x', 0, '0xa', '0x0', '0x', 1, 1)`)
	require.ErrorContains(t, err, "permission denied", "nothing beyond the old grants")

	migrateTo(t, pool, 22)
	ensurePartitions(t, d, 60_000, cfg)
	require.Equal(t, before["logs"], grantsOn(t, pool, "logs"), "reverting the migration keeps the grants")
}
