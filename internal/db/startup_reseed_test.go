package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The startup re-seed is the only repair for tokens rows written before the delta writer
// (every database upgraded from 0.4.0): transfer_count, total_supply and holder_count equal a recount after it.
func TestStartupReseedsStaleTokenCounters(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	const zero = "0x0000000000000000000000000000000000000000"
	_, err := d.pool.Exec(ctx, `
		INSERT INTO tokens (address, symbol, name, decimals, token_type, total_supply, block_number, transfer_count, holder_count)
		VALUES ('0x70c1', 'T', 'Token', 18, 'ERC20', '999', 1, 7, 9)`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		VALUES (1, 'b1', 'b0', 1700000001, 0, 30000000, 2)`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp, transfer_type, token_type)
		VALUES ('t1', 0, '0x70c1', $1, '0xa', 100, 1, 1700000001, 'transfer', 'ERC20'),
		       ('t2', 1, '0x70c1', '0xa', '0xb', 30, 1, 1700000001, 'transfer', 'ERC20')`, zero)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO token_balances_current (token_address, address, balance, block_number)
		VALUES ('0x70c1', '0xa', 70, 1), ('0x70c1', '0xb', 30, 1), ('0x70c1', '0xc', 0, 1)`)
	require.NoError(t, err)

	require.NoError(t, d.Migrate())
	var transfers, holders int
	var supply string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT transfer_count, total_supply::TEXT, holder_count FROM tokens WHERE address = '0x70c1'`).
		Scan(&transfers, &supply, &holders))
	require.Equal(t, 2, transfers, "transfer_count from the stored transfers")
	require.Equal(t, "100", supply, "total_supply: minted minus burned")
	require.Equal(t, 2, holders, "holder_count: positive current balances")
}
