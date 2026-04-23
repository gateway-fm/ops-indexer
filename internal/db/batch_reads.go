package db

import (
	"context"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// BatchGetBlockTransactionCounts returns tx counts keyed by block number.
// Missing blocks are absent from the returned map (not set to 0).
func (d *DB) BatchGetBlockTransactionCounts(ctx context.Context, blockNumbers []uint64) (map[uint64]uint32, error) {
	if len(blockNumbers) == 0 {
		return map[uint64]uint32{}, nil
	}
	rows, err := d.pool.Query(ctx, `
		SELECT block_number, COUNT(*)::int
		FROM transactions
		WHERE block_number = ANY($1)
		GROUP BY block_number`, blockNumbers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[uint64]uint32, len(blockNumbers))
	for rows.Next() {
		var n uint64
		var c int32
		if err := rows.Scan(&n, &c); err != nil {
			return nil, err
		}
		out[n] = uint32(c)
	}
	return out, rows.Err()
}

// BatchGetAddressTransactionCounts returns tx counts keyed by lowercase
// address, sourced from the materialized address_stats table.
func (d *DB) BatchGetAddressTransactionCounts(ctx context.Context, addresses []string) (map[string]uint64, error) {
	if len(addresses) == 0 {
		return map[string]uint64{}, nil
	}
	// Normalize to lowercase to match how address_stats stores them.
	lowered := make([]string, len(addresses))
	for i, a := range addresses {
		lowered[i] = lowerAddress(a)
	}
	rows, err := d.pool.Query(ctx, `
		SELECT address, tx_count
		FROM address_stats
		WHERE address = ANY($1)`, lowered)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]uint64, len(addresses))
	for rows.Next() {
		var addr string
		var count int64
		if err := rows.Scan(&addr, &count); err != nil {
			return nil, err
		}
		if count < 0 {
			count = 0
		}
		out[addr] = uint64(count)
	}
	return out, rows.Err()
}

// BatchGetTokenBalances returns token balances grouped by holder address.
// If tokenAddress is non-empty, only that token's balance is returned per
// holder; otherwise all token balances for each holder.
func (d *DB) BatchGetTokenBalances(ctx context.Context, addresses []string, tokenAddress string) (map[string][]types.Balance, error) {
	if len(addresses) == 0 {
		return map[string][]types.Balance{}, nil
	}
	lowered := make([]string, len(addresses))
	for i, a := range addresses {
		lowered[i] = lowerAddress(a)
	}

	var query string
	var args []any
	if tokenAddress != "" {
		query = `
			SELECT DISTINCT ON (address, token_address)
				address, token_address, block_number, balance::text
			FROM balances
			WHERE address = ANY($1) AND token_address = $2
			ORDER BY address, token_address, block_number DESC`
		args = []any{lowered, lowerAddress(tokenAddress)}
	} else {
		query = `
			SELECT DISTINCT ON (address, token_address)
				address, token_address, block_number, balance::text
			FROM balances
			WHERE address = ANY($1)
			ORDER BY address, token_address, block_number DESC`
		args = []any{lowered}
	}

	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]types.Balance, len(addresses))
	for rows.Next() {
		var b types.Balance
		var balanceStr string
		if err := rows.Scan(&b.Address, &b.TokenAddress, &b.BlockNumber, &balanceStr); err != nil {
			return nil, err
		}
		b.Balance = types.JSONString(balanceStr)
		out[b.Address] = append(out[b.Address], b)
	}
	return out, rows.Err()
}

// lowerAddress is a small wrapper that keeps empty strings empty.
func lowerAddress(a string) string {
	if a == "" {
		return ""
	}
	// Addresses are always hex; ToLower is cheap and safe.
	out := make([]byte, len(a))
	for i := 0; i < len(a); i++ {
		c := a[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}
