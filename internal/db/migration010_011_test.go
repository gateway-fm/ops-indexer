package db

import (
	"context"
	"testing"
)

// Migrations 010 and 011 drop three indexes whose leading column a unique constraint or primary key already
// covers; the reads that used them are served by those keys.
func TestMigrations010And011DropTheDuplicateIndexes(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for _, name := range []string{"idx_logs_tx_hash", "idx_blocks_hash", "idx_balances_address"} {
		var n int
		if err := d.pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE indexname = $1`, name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("%s still exists after the migrations", name)
		}
	}
}
