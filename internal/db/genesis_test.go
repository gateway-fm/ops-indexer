package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Ingest starts after the stored head, 0 on an empty database, so block 0 is never stored: the ledger closes its
// first span and retention prunes from block 1.
func TestLedgerAndRetentionWithoutGenesis(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedHistory(t, d, 1, 40_001, 1)
	before := totalsAfterReseed(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	require.Equal(t, [2]int64{0, 1000}, ledgerSpans(t, d)[0], "the first span closes without block 0")

	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(60_000), wm, "the legacy range and the ranges below the window are pruned")
	after := totalsAfterReseed(t, d)
	require.Equal(t, before.blocks+40_000, after.blocks, "blocks 1..80,001")
	require.Equal(t, int64(80_001), after.blocks)
}
