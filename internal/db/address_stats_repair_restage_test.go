package db

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// repairUntilDone runs the repair over every address bucket, as repair-address-stats does.
func repairUntilDone(t *testing.T, d *DB) RebaseResult {
	t.Helper()
	ctx := context.Background()
	var total RebaseResult
	for bucket := 0; bucket < AddressBuckets; bucket++ {
		r, err := d.RepairAddressBucket(ctx, bucket, AddressRepairBatch)
		require.NoError(t, err)
		total.add(r)
	}
	return total
}

// onceBetweenReadAndWrite runs fn the first time a rebase batch has read its rows and truth and not yet written.
func onceBetweenReadAndWrite(t *testing.T, fn func()) {
	t.Helper()
	var once sync.Once
	rebaseReadHook = func([]string) { once.Do(fn) }
	t.Cleanup(func() { rebaseReadHook = nil })
}

type seenRow struct {
	txs         int64
	first, last uint64
}

func addressSeen(t *testing.T, d *DB, address string) seenRow {
	t.Helper()
	var r seenRow
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT tx_count, first_seen, last_seen FROM address_stats WHERE address = $1`, address).Scan(&r.txs, &r.first, &r.last))
	return r
}

func TestAddressStatsRepairStagedBeforeAReorgOfAnUndercountedAddress(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}})))
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = 0 WHERE address = '0xa'`)
	require.NoError(t, err)

	onceBetweenReadAndWrite(t, func() { require.NoError(t, d.DeleteBlock(ctx, 2)) })
	repairUntilDone(t, d)

	require.Equal(t, seenRow{txs: 1, first: 1, last: 1}, addressSeen(t, d, "0xa"))
}

func TestAddressStatsRepairStagedBeforeAReorgOfAnOvercountedAddress(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}})))
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 7 WHERE address = '0xa'`)
	require.NoError(t, err)

	onceBetweenReadAndWrite(t, func() { require.NoError(t, d.DeleteBlock(ctx, 2)) })
	repairUntilDone(t, d)

	require.Equal(t, seenRow{txs: 1, first: 1, last: 1}, addressSeen(t, d, "0xa"))
}

func TestAddressStatsRepairStagedBeforeAReorgThatMovesFirstSeen(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xd"}})))
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 3 WHERE address = '0xd'`)
	require.NoError(t, err)

	onceBetweenReadAndWrite(t, func() {
		require.NoError(t, d.DeleteBlock(ctx, 2))
		require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2b", []revertTx{{from: "0xa", to: "0xb"}})))
		require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(3, "h3", []revertTx{{from: "0xa", to: "0xd"}})))
	})
	repairUntilDone(t, d)

	require.Equal(t, seenRow{txs: 1, first: 3, last: 3}, addressSeen(t, d, "0xd"))
	require.Equal(t, seenRow{txs: 3, first: 1, last: 3}, addressSeen(t, d, "0xa"))
}

func TestAddressStatsRepairStagedBeforeAReorgBetweenBatches(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}, {from: "0xb", to: "0xc"}})))
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = 0`)
	require.NoError(t, err)

	batches := 0
	rebaseReadHook = func([]string) {
		if batches++; batches == 2 {
			require.NoError(t, d.DeleteBlock(ctx, 2))
		}
	}
	t.Cleanup(func() { rebaseReadHook = nil })
	for bucket := 0; bucket < AddressBuckets; bucket++ {
		_, err := d.RepairAddressBucket(ctx, bucket, 1)
		require.NoError(t, err)
	}

	require.Equal(t, seenRow{txs: 1, first: 1, last: 1}, addressSeen(t, d, "0xa"), "repaired before the reorg, then reverted")
	require.Equal(t, seenRow{txs: 1, first: 1, last: 1}, addressSeen(t, d, "0xb"), "repaired across the reorg")
	requireAddressStatsIsTheTruth(t, d)
}

// The startup re-seed of chain_counters writes only the totals it derives: the revert count survives it, so a stage
// an earlier release left is still found stale after a rollback and a restart.
func TestTheStartupReseedKeepsTheRevertCount(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}})))
	require.NoError(t, d.DeleteBlock(ctx, 2))
	reverts := func() int64 {
		var n int64
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT count FROM chain_counters WHERE name = $1`, addressStatsRevertsCounter).Scan(&n))
		return n
	}
	require.EqualValues(t, 1, reverts())
	require.NoError(t, d.Reconcile(ctx))
	require.EqualValues(t, 1, reverts(), "the re-seed keeps the revert count")
}

// The revert count is a chain_counters row but not a chain total: no total moves with it.
func TestTheRevertCountIsNotAChainTotal(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}})))
	before, err := d.GetChainStats(ctx)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `INSERT INTO chain_counters (name, count) VALUES ($1, 1000000)`, addressStatsRevertsCounter)
	require.NoError(t, err)
	after, err := d.GetChainStats(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, d.Reconcile(ctx))
	after, err = d.GetChainStats(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

// stageAsTheEarlierRelease creates the stage tables the earlier release's repair used, with one correction applied
// and one not, as a run that died part way leaves them.
func stageAsTheEarlierRelease(t *testing.T, d *DB) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), `
		CREATE TABLE `+AddressStatsRepairTable+` (address TEXT PRIMARY KEY, txs BIGINT NOT NULL, internal BIGINT NOT NULL,
			transfers BIGINT NOT NULL, first_seen BIGINT NOT NULL, last_seen BIGINT NOT NULL, missing BOOLEAN NOT NULL);
		INSERT INTO `+AddressStatsRepairTable+` VALUES ('0xb', -4, 0, 0, 1, 1, false);
		CREATE TABLE `+addressStatsRepairTotalTable+` (staged BIGINT NOT NULL, applied BIGINT NOT NULL, reverts BIGINT NOT NULL);
		INSERT INTO `+addressStatsRepairTotalTable+` VALUES (2, 1, 0)`)
	require.NoError(t, err)
}

func repairStageExists(t *testing.T, d *DB) bool {
	t.Helper()
	exists, err := d.AddressStatsRepairStageExists(context.Background())
	require.NoError(t, err)
	return exists
}

// A wipe removes a stage an earlier release left, both tables, and an unfinished repair's place, before it truncates
// chain_counters: a wipe stopped there leaves neither.
func TestAWipeRemovesAStagedRepairBeforeTheRevertCount(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	stageAsTheEarlierRelease(t, d)
	require.NoError(t, d.SetAddressRepairNext(ctx, 100))

	_, err := d.pool.Exec(ctx, `CREATE FUNCTION stop_wipe() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'stopped'; END $$;
		CREATE TRIGGER stop_wipe BEFORE TRUNCATE ON daily_stats EXECUTE FUNCTION stop_wipe()`)
	require.NoError(t, err)
	require.ErrorContains(t, d.WipeAllData(ctx), "stopped", "stopped at its first truncate")
	_, err = d.pool.Exec(ctx, `DROP TRIGGER stop_wipe ON daily_stats`)
	require.NoError(t, err)
	require.False(t, repairStageExists(t, d), "no stage left beside the counters it was made against")
	next, err := d.AddressRepairNext(ctx)
	require.NoError(t, err)
	require.Zero(t, next, "a repair of the new chain starts at the first bucket")

	require.NoError(t, d.WipeAllData(ctx))
}
