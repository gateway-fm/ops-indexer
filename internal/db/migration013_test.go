package db

import (
	"context"
	"testing"
)

// Migration 013 keys logs and token_transfers by (block_number, log_index), which ingest appends in order,
// and drops the randomly keyed tx_hash unique constraints and foreign keys.
func TestMigration013KeysLogsAndTransfersByBlockAndLogIndex(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	exists := func(sql string, args ...any) bool {
		var n int
		if err := d.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	for _, table := range []string{"logs", "token_transfers"} {
		if !exists(`SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $1 || '_block_number_log_index_key' AND indexdef LIKE 'CREATE UNIQUE INDEX%'`, table) {
			t.Errorf("%s: no unique index on (block_number, log_index)", table)
		}
		for _, c := range []string{table + "_tx_hash_fkey", table + "_tx_hash_log_index_key"} {
			if exists(`SELECT count(*) FROM pg_constraint WHERE conname = $1`, c) {
				t.Errorf("%s: constraint %s still present", table, c)
			}
		}
	}
}
