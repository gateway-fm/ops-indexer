package db

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatasetIdentityRestartRestoreAndChainMismatch(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	genesis := bytes.Repeat([]byte{0xab}, 32)
	var first, second, restored string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT dataset_epoch::text FROM indexer_dataset_metadata WHERE id=1`).Scan(&first))
	require.NoError(t, d.EnsureDatasetIdentity(ctx, "31337", genesis))
	require.NoError(t, d.EnsureDatasetIdentity(ctx, "31337", genesis))
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT dataset_epoch::text FROM indexer_dataset_metadata WHERE id=1`).Scan(&second))
	require.Equal(t, first, second, "ordinary initialization/restart retains the epoch")
	require.ErrorIs(t, d.EnsureDatasetIdentity(ctx, "31338", genesis), ErrDatasetIdentity)
	require.ErrorIs(t, d.EnsureDatasetIdentity(ctx, "31337", bytes.Repeat([]byte{0xcd}, 32)), ErrDatasetIdentity)
	require.NoError(t, d.RotateDatasetEpoch(ctx))
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT dataset_epoch::text FROM indexer_dataset_metadata WHERE id=1`).Scan(&restored))
	require.NotEqual(t, second, restored)
	require.NoError(t, d.EnsureDatasetIdentity(ctx, "31337", genesis))
	for _, invalid := range []string{"", "01", "-1", "+1", "0x1"} {
		require.ErrorIs(t, d.EnsureDatasetIdentity(ctx, invalid, genesis), ErrDatasetIdentity)
	}
	require.ErrorIs(t, d.EnsureDatasetIdentity(ctx, "1", genesis[:31]), ErrDatasetIdentity)
}

func TestDatasetIdentityRefusesInterruptedWipe(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `CREATE TABLE wipe_in_progress(started_at timestamptz NOT NULL DEFAULT now())`)
	require.NoError(t, err)
	require.ErrorIs(t, d.EnsureDatasetIdentity(ctx, "31337", make([]byte, 32)), ErrHistoryWipeInProgress)
}
