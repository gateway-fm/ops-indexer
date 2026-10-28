package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// After 013 nothing serves a lookup of logs or token_transfers by transaction hash alone; EnsureTxHashIndexes
// restores one index per table, once.
func TestEnsureTxHashIndexesServesLookupsByTransactionHash(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	plan := func(table string) string {
		tx, err := d.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			t.Fatal(err)
		}
		var lines []string
		rows, err := tx.Query(ctx, `EXPLAIN SELECT * FROM `+table+` WHERE tx_hash = '0xabc'`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var l string
			if err := rows.Scan(&l); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, l)
		}
		return strings.Join(lines, "\n")
	}
	indexes := func(table string) int {
		var n int
		if err := d.pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexdef LIKE '%(tx_hash)'`, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, table := range txHashLookupTables {
		if p := plan(table); !strings.Contains(p, "Seq Scan") {
			t.Fatalf("%s before: expected no usable index, got\n%s", table, p)
		}
	}
	for range 2 {
		if err := d.EnsureTxHashIndexes(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range txHashLookupTables {
		if p := plan(table); strings.Contains(p, "Seq Scan") {
			t.Errorf("%s after: lookup by tx_hash still scans the table\n%s", table, p)
		}
		if n := indexes(table); n != 1 {
			t.Errorf("%s: %d tx_hash indexes, want 1", table, n)
		}
	}
}

// An INVALID index (a failed CREATE INDEX CONCURRENTLY) serves no lookup, so it does not count as present.
func TestEnsureTxHashIndexesBuildsOverAnInvalidIndex(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `CREATE INDEX logs_tx_hash_broken ON logs (tx_hash)`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'logs_tx_hash_broken'::regclass`)
	require.NoError(t, err)

	require.NoError(t, d.EnsureTxHashIndexes(ctx))
	var valid int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		WHERE i.indrelid = 'logs'::regclass AND i.indnatts = 1 AND a.attname = 'tx_hash' AND i.indisvalid`).Scan(&valid))
	require.Equal(t, 1, valid, "a valid tx_hash index exists beside the invalid one")
}
