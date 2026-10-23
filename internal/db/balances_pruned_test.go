package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// With retention on, a balance batch that holds a row for a pruned block still
// stores the rest. The pruned row's history has no partition to go to; its current balance is still applied.
func TestBalanceBatchWithAPrunedBlockStoresTheRest(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Positive(t, wm)

	require.NoError(t, d.InsertBalancesBatch(ctx, []*types.Balance{
		{Address: "0xc0", TokenAddress: retentionToken, BlockNumber: 79_000, Balance: "5"},
		{Address: "0xc1", TokenAddress: retentionToken, BlockNumber: 100, Balance: "9"},
	}))
	var history, current int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM balances WHERE address IN ('0xc0', '0xc1')),
		       (SELECT count(*) FROM token_balances_current WHERE address IN ('0xc0', '0xc1'))`).Scan(&history, &current))
	require.Equal(t, 1, history, "the retained row's history is stored, the pruned one's is not")
	require.Equal(t, 2, current, "both current balances are applied")
}
