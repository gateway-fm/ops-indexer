package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatasetWipeRotatesEpochAndClearsIdentity(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `UPDATE indexer_dataset_metadata SET chain_id='31337',genesis_hash=decode(repeat('ab',32),'hex') WHERE id=1`)
	require.NoError(t, err)
	var before, after string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT dataset_epoch::text FROM indexer_dataset_metadata WHERE id=1`).Scan(&before))
	require.NoError(t, d.WipeAllData(ctx))
	var chainID *string
	var genesis []byte
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT dataset_epoch::text,chain_id,genesis_hash FROM indexer_dataset_metadata WHERE id=1`).Scan(&after, &chainID, &genesis))
	require.NotEqual(t, before, after, "old source continuations cannot survive a wipe")
	require.Nil(t, chainID, "trusted node must reinitialize the selected new chain")
	require.Nil(t, genesis)
}

func TestDatasetWipeRotatesBeforeFirstDestructiveStep(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	var before, after string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT dataset_epoch::text FROM indexer_dataset_metadata WHERE id=1`).Scan(&before))
	_, err := d.pool.Exec(ctx, `CREATE FUNCTION stop_dataset_wipe() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected wipe stop';END$$;
	 CREATE TRIGGER stop_dataset_wipe BEFORE TRUNCATE ON daily_stats FOR EACH STATEMENT EXECUTE FUNCTION stop_dataset_wipe()`)
	require.NoError(t, err)
	require.ErrorContains(t, d.WipeAllData(ctx), "injected wipe stop")
	var marked bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT dataset_epoch::text,to_regclass('wipe_in_progress') IS NOT NULL FROM indexer_dataset_metadata WHERE id=1`).Scan(&after, &marked))
	require.NotEqual(t, before, after, "invalidation precedes every destructive step")
	require.True(t, marked, "interrupted wipe remains unavailable until resumed")
	_, err = d.pool.Exec(ctx, `DROP TRIGGER stop_dataset_wipe ON daily_stats`)
	require.NoError(t, err)
	require.NoError(t, d.ResumeInterruptedWipe(ctx))
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass('wipe_in_progress') IS NOT NULL`).Scan(&marked))
	require.False(t, marked)
}
