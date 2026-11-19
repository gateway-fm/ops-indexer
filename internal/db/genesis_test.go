package db

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/stretchr/testify/require"
)

func TestInitialGenesisUsesRealIdentityAndNeverReopensPrunedHistory(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.EnsureDatasetIdentity(ctx, "31337", bytes.Repeat([]byte{0xab}, 32)))
	b := &types.Block{Hash: "0x" + strings.Repeat("ab", 32), ParentHash: "0x" + strings.Repeat("00", 32), Timestamp: 10, GasLimit: 30000000}
	require.NoError(t, d.EnsureInitialGenesis(ctx, b))
	require.NoError(t, d.EnsureInitialGenesis(ctx, b))
	var rows, total int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM blocks WHERE number=0),(SELECT count FROM chain_counters WHERE name='blocks_total')`).Scan(&rows, &total))
	require.EqualValues(t, 1, rows)
	require.EqualValues(t, 1, total, "restart cannot count genesis twice")
	wrong := *b
	wrong.Hash = "0x" + strings.Repeat("cd", 32)
	require.ErrorIs(t, d.EnsureInitialGenesis(ctx, &wrong), ErrDatasetIdentity)
	wrong = *b
	wrong.TransactionCount = 1
	require.ErrorIs(t, d.EnsureInitialGenesis(ctx, &wrong), ErrDatasetIdentity)
	_, err := d.pool.Exec(ctx, `DELETE FROM blocks WHERE number=0;UPDATE prune_state SET pruned_through=1 WHERE id=1`)
	require.NoError(t, err)
	require.NoError(t, d.EnsureInitialGenesis(ctx, b))
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM blocks WHERE number=0`).Scan(&rows))
	require.Zero(t, rows, "startup must not bring pruned genesis back")
}
