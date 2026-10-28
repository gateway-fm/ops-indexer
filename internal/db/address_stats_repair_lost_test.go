package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddressStatsRepairNeverReportsDoneOverLostCorrections(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedVariedHistory(t, d, 1, 300)

	stage, err := d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.Greater(t, stage.Changes, int64(10))
	var persistence string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT relpersistence::TEXT FROM pg_class WHERE oid = to_regclass($1)`,
		AddressStatsRepairTable).Scan(&persistence))
	require.Equal(t, "p", persistence, "the staged corrections are WAL-logged: a crash or a failover keeps them")

	applied, _, done, err := d.ApplyAddressStatsRepairBatch(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 10, applied)
	require.False(t, done)

	_, err = d.pool.Exec(ctx, `TRUNCATE `+AddressStatsRepairTable)
	require.NoError(t, err)
	for {
		_, _, done, err = d.ApplyAddressStatsRepairBatch(ctx, 10)
		if err != nil || done {
			break
		}
	}
	require.ErrorIs(t, err, ErrAddressStatsRepairLost, "staged corrections that were never applied are gone")
	require.False(t, done, "a repair that lost staged corrections is not done")

	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err, "the lost repair can be staged again")
	for {
		_, _, done, err = d.ApplyAddressStatsRepairBatch(ctx, 10)
		require.NoError(t, err)
		if done {
			break
		}
	}
	left, err := d.StageAddressStatsRepair(ctx, true)
	require.NoError(t, err)
	require.Zero(t, left.Changes, "after the restaged repair, nothing is left to correct")
}
