//go:build faults

package db

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

// fkDB is a migrated database reached through a fault proxy, and its direct URL.
func fkDB(t *testing.T) (*DB, *faultkit.Proxy, string) {
	t.Helper()
	url := faultkit.NewDB(t)
	p := faultkit.NewProxy(t, url)
	pool, err := pgxpool.New(context.Background(), p.URL())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	d := &DB{pool: pool}
	require.NoError(t, d.Migrate())
	return d, p, url
}

// fkRestart is a new process on the same database: a fresh DB on its own pool (through url).
func fkRestart(t *testing.T, url string) *DB {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &DB{pool: pool}
}

// fkDump returns query's rows as JSON text, sorted, so two databases compare equal.
func fkDump(t *testing.T, d *DB, query string) []string {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `SELECT to_jsonb(q)::text FROM (`+query+`) q ORDER BY 1`)
	require.NoError(t, err)
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return out
}

var fkLedgerQueries = []string{
	`SELECT lo, hi, source FROM ledger_spans`,
	`SELECT * FROM ledger_chain`,
	`SELECT * FROM ledger_token`,
	`SELECT * FROM ledger_address`,
}

var fkDailyQueries = []string{
	`SELECT * FROM daily_stats`,
	`SELECT * FROM daily_stats_acc`,
	`SELECT * FROM daily_stats_watermark`,
	`SELECT name, count FROM chain_counters`,
}

func fkSame(t *testing.T, want, got *DB, queries []string) {
	t.Helper()
	for _, q := range queries {
		w, g := fkDump(t, want, q), fkDump(t, got, q)
		require.Equal(t, strings.Join(w, "\n"), strings.Join(g, "\n"), "differs: %s", q)
	}
}
