package db

import (
	"context"
	"io/fs"
	"testing"

	"github.com/jackc/tern/v2/migrate"
	"github.com/stretchr/testify/require"
)

const (
	selfA = "0x00000000000000000000000000000000000000a1"
	selfB = "0x00000000000000000000000000000000000000b2"
	selfC = "0x00000000000000000000000000000000000000c3"
)

func testMigrator(t *testing.T, d *DB) (*migrate.Migrator, func()) {
	t.Helper()
	ctx := context.Background()
	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	require.NoError(t, err)
	migrationsFS, err := fs.Sub(migrations, "migrations")
	require.NoError(t, err)
	require.NoError(t, m.LoadMigrations(migrationsFS))
	return m, conn.Release
}

// Two transactions: a self-send A->A carrying a self-transfer A->A and a transfer B->C, and A->B carrying a
// transfer C->A. Internal calls: A->A in the first, A->B in the second.
func seedSelfTransfers(t *testing.T, d *DB) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		VALUES (1, 'blk1', 'blk0', 1700000001, 1, 1, 2);
		INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ('tx1', 1, 1700000001, 0, '`+selfA+`', '`+selfA+`', '0', 0, 0, '0x', 1, 0),
		       ('tx2', 1, 1700000001, 1, '`+selfA+`', '`+selfB+`', '0', 0, 0, '0x', 1, 0);
		INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp)
		VALUES ('tx1', 0, '0x70c1', '`+selfA+`', '`+selfA+`', 1, 1, 1700000001),
		       ('tx1', 1, '0x70c1', '`+selfB+`', '`+selfC+`', 1, 1, 1700000001),
		       ('tx2', 2, '0x70c1', '`+selfC+`', '`+selfA+`', 1, 1, 1700000001);
		INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value, call_type)
		VALUES ('tx1', 1, '0', '`+selfA+`', '`+selfA+`', '0', 'call'),
		       ('tx2', 1, '0', '`+selfA+`', '`+selfB+`', '0', 'call')`)
	require.NoError(t, err)
}

func addressCounters(t *testing.T, d *DB, address string) (txs, transfers, internal int64) {
	t.Helper()
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT tx_count, token_transfer_count, internal_tx_count FROM address_stats WHERE address = $1`, address).
		Scan(&txs, &transfers, &internal))
	return txs, transfers, internal
}

func TestRebuildAddressStatsTotalsMatchTheRowsTheListsReturn(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedSelfTransfers(t, d)
	require.NoError(t, d.RebuildAddressStats(ctx))

	for _, addr := range []string{selfA, selfB, selfC} {
		txRows, err := d.GetTransactionsByAddress(ctx, addr, 100, nil)
		require.NoError(t, err)
		transferRows, err := d.GetTransfersByAddress(ctx, addr, 100, nil)
		require.NoError(t, err)
		transferTotal, err := d.CountTransfersByAddress(ctx, addr)
		require.NoError(t, err)
		internalRows, internalTotal, err := d.GetInternalTransactionsByAddress(ctx, addr, 100, 0)
		require.NoError(t, err)
		txCount, _, _ := addressCounters(t, d, addr)

		require.Equal(t, int64(len(txRows)), txCount, "%s tx_count", addr)
		require.Equal(t, int64(len(transferRows)), transferTotal, "%s token transfers total", addr)
		require.Equal(t, int64(len(internalRows)), internalTotal, "%s internal transactions total", addr)
	}
	_, transfers, internal := addressCounters(t, d, selfA)
	require.Equal(t, int64(2), transfers, "A: the self-transfer once plus C->A")
	require.Equal(t, int64(2), internal, "A: the self-call once plus A->B")
}

func TestMigration017CountsSelfTransfersOnceAndReverts(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	m, release := testMigrator(t, d)
	defer release()
	require.NoError(t, m.MigrateTo(ctx, 16))

	seedSelfTransfers(t, d)
	_, err := d.pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count)
		VALUES ('`+selfA+`', 3, 3, 3), ('`+selfB+`', 2, 1, 1), ('`+selfC+`', 2, 0, 2)`)
	require.NoError(t, err)

	require.NoError(t, m.Migrate(ctx))
	_, transfers, internal := addressCounters(t, d, selfA)
	require.Equal(t, [2]int64{2, 2}, [2]int64{transfers, internal}, "A loses one side of its self-transfer and self-call")
	_, transfers, internal = addressCounters(t, d, selfB)
	require.Equal(t, [2]int64{1, 1}, [2]int64{transfers, internal}, "B has no self-transfer")

	require.NoError(t, m.MigrateTo(ctx, 16))
	_, transfers, internal = addressCounters(t, d, selfA)
	require.Equal(t, [2]int64{3, 3}, [2]int64{transfers, internal}, "reverting adds the side back")
}

func TestMigration017RefusesOncePruned(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	m, release := testMigrator(t, d)
	defer release()
	require.NoError(t, m.MigrateTo(ctx, 16))
	_, err := d.pool.Exec(ctx, `UPDATE prune_state SET pruned_through = 1000 WHERE id = 1`)
	require.NoError(t, err)

	err = m.Migrate(ctx)
	require.ErrorContains(t, err, "history at or below block 1000 is pruned")
	v, err := m.GetCurrentVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(16), v, "the refused migration is not recorded as applied")
}
