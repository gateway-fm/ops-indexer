package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type ledgerAddressRow struct{ txs, sent, internal, transfers, firstSeen, lastSeen int64 }

// The scenario of the indexer's TestAddressStatsCountEachTransactionTransferAndCallOncePerAddress, stored in block 1:
// a span's address rows count what the writer's deltas count.
func TestLedgerAddressRowsCountWhatTheWriterCounts(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 14_000, 1)
	_, err := d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ('self1', 1, 1700000001, 1, '`+selfA+`', '`+selfA+`', '0', 0, 0, '0x', 1, 2),
		       ('self2', 1, 1700000001, 2, '`+selfA+`', '`+selfB+`', '0', 0, 0, '0x', 1, 106);
		INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp)
		VALUES ('self1', 10, '0x70c1', '`+selfA+`', '`+selfA+`', 1, 1, 1700000001),
		       ('self1', 11, '0x70c1', '`+selfB+`', '`+selfC+`', 1, 1, 1700000001),
		       ('self2', 12, '0x70c1', '`+selfC+`', '`+selfA+`', 1, 1, 1700000001),
		       ('self2', 13, '0x70c1', '`+selfC+`', '`+selfA+`', 1, 1, 1700000001);
		INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value, call_type)
		VALUES ('self1', 1, '0', '`+selfA+`', '`+selfA+`', '0', 'call'),
		       ('self2', 1, '0', '`+selfA+`', '`+selfB+`', '0', 'call');
		INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number)
		VALUES ('0x00000000000000000000000000000000000000d4', '00', '`+selfA+`', 'self1', 1)`)
	require.NoError(t, err)

	closeLedger(t, d)
	row := func(address string) ledgerAddressRow {
		var r ledgerAddressRow
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT txs, sent, internal, transfers, first_seen, last_seen FROM ledger_address WHERE lo = 0 AND address = $1`, address).
			Scan(&r.txs, &r.sent, &r.internal, &r.transfers, &r.firstSeen, &r.lastSeen))
		return r
	}
	require.Equal(t, ledgerAddressRow{txs: 2, sent: 1, internal: 2, transfers: 3, firstSeen: 1, lastSeen: 1}, row(selfA),
		"A: the self-send and A->B once each, only the type-2 one bumps its nonce, the self-transfer and self-call once")
	require.Equal(t, ledgerAddressRow{txs: 2, internal: 1, transfers: 1, firstSeen: 1, lastSeen: 1}, row(selfB))
	require.Equal(t, ledgerAddressRow{txs: 2, transfers: 3, firstSeen: 1, lastSeen: 1}, row(selfC), "C: one per transaction")
	require.Equal(t, ledgerAddressRow{firstSeen: 1, lastSeen: 1}, row("0x00000000000000000000000000000000000000d4"),
		"a created contract is seen in its creation block")

	var zero int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM ledger_address WHERE address = '0x0000000000000000000000000000000000000000'`).Scan(&zero))
	require.Zero(t, zero, "the zero address keeps no counters")
	var spanRows, seeded ledgerAddressRow
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT sum(txs), sum(transfers) FROM ledger_address WHERE address = '0xb'`).Scan(&spanRows.txs, &spanRows.transfers))
	seeded = ledgerAddressRow{txs: 1000, transfers: 1000}
	require.Equal(t, seeded, spanRows, "0xb across the closed spans [0, 1,000): one transaction and one mint per block")
}
