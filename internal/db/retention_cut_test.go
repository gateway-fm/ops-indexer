package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

func historyCutChecks(t *testing.T, d *DB) int {
	t.Helper()
	var n int
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pg_constraint WHERE conname = 'history_cut' AND conrelid::regclass::text LIKE '%\_legacy'`).Scan(&n))
	return n
}

func insertCutTestBlock(d *DB, number uint64) error {
	return d.InsertBlockDataBatch(context.Background(), &BlockData{
		Block: &types.Block{Number: number, Hash: fmt.Sprintf("cut%d", number), Timestamp: 1_700_000_000 + number},
	})
}

// A cut abandoned after its checks were validated (the head came near it, a failure, a restart) leaves "below the
// cut" on every legacy partition while the history is still uncut. With retention then switched off nothing would
// ever drop it, and ingest stops at the cut: the next start drops it before ingest.
func TestAbandonedCutIsReleasedBeforeIngest(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	_, err := d.pool.Exec(ctx, `SELECT history_cut_constrain(44001)`)
	require.NoError(t, err)
	for _, tbl := range HistoryTables {
		_, err := d.pool.Exec(ctx, `ALTER TABLE `+tbl+`_legacy VALIDATE CONSTRAINT history_cut`)
		require.NoError(t, err)
	}
	require.Error(t, insertCutTestBlock(d, 44_001), "the hazard: ingest is refused at the abandoned cut")

	d.RunRetention(ctx, RetentionConfig{RetainBlocks: 0})
	require.Equal(t, len(HistoryTables), historyCutChecks(t, d), "retention off returns at once and keeps the check")

	require.NoError(t, d.Migrate(), "a restart: the reconcile runs before ingest")
	require.Zero(t, historyCutChecks(t, d))
	require.NoError(t, insertCutTestBlock(d, 44_001), "ingest continues past the old cut")
	cut, err := d.HistoryCut(ctx)
	require.NoError(t, err)
	require.False(t, cut)
}

// A cut abandoned mid-validation drops its checks on the way out: here the logs partition holds a row above the cut,
// so validating it fails after the earlier tables validated.
func TestAbandonedCutDropsItsChecks(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	_, err := d.pool.Exec(ctx, `
		INSERT INTO logs (tx_hash, log_index, address, topic0, data, block_number, timestamp)
		VALUES ('above', 0, '0xa', '0x0', '0x', 50000, 1700050000)`)
	require.NoError(t, err)

	err = d.cutHistory(ctx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000})
	require.ErrorContains(t, err, "validate logs_legacy", "the cut at 44,001 is abandoned at the logs partition")
	require.Zero(t, historyCutChecks(t, d), "the abandoned cut's checks are dropped")
	require.NoError(t, insertCutTestBlock(d, 44_001))
}
