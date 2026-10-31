package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// ledgerAddressRowsRead flushes every pooled backend's statistics and returns the rows read from ledger_address, by
// sequential scans and through its indexes.
func ledgerAddressRowsRead(t *testing.T, d *DB) int64 {
	t.Helper()
	ctx := context.Background()
	for _, c := range d.pool.AcquireAllIdle(ctx) {
		_, err := c.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
		require.NoError(t, err)
		_, err = c.Exec(ctx, `SELECT 1`)
		require.NoError(t, err)
		c.Release()
	}
	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, `SELECT pg_stat_clear_snapshot()`)
	require.NoError(t, err)
	var n int64
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT COALESCE((SELECT seq_tup_read FROM pg_stat_user_tables WHERE relname = 'ledger_address'), 0)
		     + COALESCE((SELECT sum(idx_tup_read) FROM pg_stat_user_indexes WHERE relname = 'ledger_address'), 0)`).Scan(&n))
	return n
}

// The ledger's address index is built in the background after the migrations, and a reorg does not wait for it.
// A revert of an address that has no stored block left reads its last block from the closed ledger: that read must
// go through the ledger's primary key, span by span, not read the whole ledger while it holds the address's row.
func TestARevertReadsTheClosedLedgerWithoutScanningItWhileItsAddressIndexIsBuilt(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(5)))
	_, err := d.pool.Exec(ctx, `
		INSERT INTO ledger_spans (lo, hi, source) SELECT -1000 * g, -1000 * g + 1, 'rows' FROM generate_series(1, 200) g;
		INSERT INTO ledger_address (lo, address, txs, sent, internal, transfers, first_seen, last_seen)
		SELECT -1000 * g, '0xpad' || g || '-' || a, 1, 1, 0, 0, -1000 * g, -1000 * g
		FROM generate_series(1, 200) g, generate_series(1, 1000) a;
		INSERT INTO ledger_spans (lo, hi, source) VALUES (0, 4, 'rows');
		INSERT INTO ledger_address (lo, address, txs, sent, internal, transfers, first_seen, last_seen)
		VALUES (0, '`+rebaseHot+`', 3, 3, 0, 0, 1, 3);
		UPDATE address_stats SET tx_count = tx_count + 3, sent_count = sent_count + 3, first_seen = 1
		WHERE address = '`+rebaseHot+`';
		DROP INDEX IF EXISTS ledger_address_address;
		ANALYZE ledger_address`)
	require.NoError(t, err, "a closed ledger of 200,001 rows; blocks 1-3 pruned into its last span; the address index not built yet")

	before := ledgerAddressRowsRead(t, d)
	require.NoError(t, d.DeleteBlock(ctx, 5), "the head block is reverted")
	read := ledgerAddressRowsRead(t, d) - before

	var last int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT last_seen FROM address_stats WHERE address = $1`, rebaseHot).Scan(&last))
	require.Equal(t, int64(3), last, "last_seen moves to the closed ledger's last block")
	require.Less(t, read, int64(10_000), "ledger_address rows the revert read for one address (the ledger holds 200,001)")
}
