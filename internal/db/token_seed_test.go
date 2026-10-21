package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A token row created after its early transfers were pruned (its metadata fetch kept failing) is seeded from the
// retained transfers plus the pruned spans' ledger rows, not from the retained rows alone.
func TestTokenCreatedAfterAPruneIsSeededFromTheLedgerToo(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 82_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(60_000), wm)
	_, err = d.pool.Exec(ctx, `DELETE FROM tokens WHERE address = $1`, retentionToken)
	require.NoError(t, err)

	ts := uint64(1_700_080_002)
	require.NoError(t, d.InsertBlockDataBatch(ctx, &BlockData{
		Block:     &types.Block{Number: 80_002, Hash: "blk80002", ParentHash: "blk80001", Timestamp: ts},
		Tokens:    []*types.Token{{Address: retentionToken, Symbol: "RET", TokenType: types.TokenTypeERC20, BlockNumber: 80_002}},
		Transfers: []*types.TokenTransfer{{TxHash: "late", LogIndex: 0, TokenAddress: retentionToken, From: zeroAddress, To: "0xb",
			Value: "7", BlockNumber: 80_002, Timestamp: &ts, TokenType: types.TokenTypeERC20}},
	}))
	var transfers int64
	var supply string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT transfer_count, total_supply::TEXT FROM tokens WHERE address = $1`, retentionToken).
		Scan(&transfers, &supply))
	require.Equal(t, int64(80_003), transfers, "blocks 0..80,002: 60,001 pruned + 20,002 retained, the new one once")
	require.Equal(t, "560021", supply, "7 per mint")
}
