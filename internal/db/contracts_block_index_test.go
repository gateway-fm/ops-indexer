package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The ledger closes a span reading contracts by block range; without an index on block_number every
// close reads the whole table.
func TestEnsureContractsBlockIndexServesTheSpanRead(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `
		INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number)
		SELECT '0x' || lpad(to_hex(g), 40, '0'), '6080', '0xc0', 'tx' || g, g FROM generate_series(1, 50000) g;
		ANALYZE contracts`)
	require.NoError(t, err)
	plan := func() string {
		var lines []string
		rows, err := d.pool.Query(ctx, `EXPLAIN SELECT address FROM contracts WHERE block_number >= 1000 AND block_number < 1100`)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var l string
			require.NoError(t, rows.Scan(&l))
			lines = append(lines, l)
		}
		return strings.Join(lines, "\n")
	}
	require.NoError(t, d.EnsureContractsBlockIndex(ctx))
	require.NoError(t, d.EnsureContractsBlockIndex(ctx))
	p := plan()
	require.NotContains(t, p, "Seq Scan on contracts", p)
}
