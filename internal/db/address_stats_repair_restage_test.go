package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// repairUntilDone applies the staged repair, staging again whenever a reorg made the stage stale, as
// repair-address-stats does.
func repairUntilDone(t *testing.T, d *DB) (restaged int) {
	t.Helper()
	ctx := context.Background()
	for done := false; !done; {
		var err error
		_, _, done, err = d.ApplyAddressStatsRepairBatch(ctx, 1)
		if errors.Is(err, ErrAddressStatsRepairStale) {
			restaged++
			require.Less(t, restaged, 10)
			_, err = d.StageAddressStatsRepair(ctx, false)
		}
		require.NoError(t, err)
	}
	return restaged
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

	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.NoError(t, d.DeleteBlock(ctx, 2))
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

	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.NoError(t, d.DeleteBlock(ctx, 2))
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

	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.NoError(t, d.DeleteBlock(ctx, 2))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2b", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(3, "h3", []revertTx{{from: "0xa", to: "0xd"}})))
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

	stage, err := d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.Equal(t, int64(3), stage.Changes)
	applied, _, done, err := d.ApplyAddressStatsRepairBatch(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, applied)
	require.False(t, done)
	require.NoError(t, d.DeleteBlock(ctx, 2))
	require.Equal(t, 1, repairUntilDone(t, d))

	require.Equal(t, seenRow{txs: 1, first: 1, last: 1}, addressSeen(t, d, "0xa"), "applied before the reorg, then reverted")
	require.Equal(t, seenRow{txs: 1, first: 1, last: 1}, addressSeen(t, d, "0xb"), "applied after the restage")
	check, err := d.StageAddressStatsRepair(ctx, true)
	require.NoError(t, err)
	require.Zero(t, check.Changes)
}

// A revert that has not committed holds the revert count: a batch waits for it, then finds the stage stale.
func TestAddressStatsRepairBatchWaitsForAnUncommittedRevert(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}})))
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = 0 WHERE address = '0xa'`)
	require.NoError(t, err)
	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)

	tx, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = revertAddressStats(ctx, tx, 2)
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		_, _, _, err := d.ApplyAddressStatsRepairBatch(ctx, 1)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("the batch ran beside an uncommitted revert: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(ctx))
	require.ErrorIs(t, <-result, ErrAddressStatsRepairStale)
}

// A batch holds the revert count until it commits: a revert waits for it before it touches any address_stats row.
func TestARevertWaitsForARepairBatchBeforeAnyAddressRow(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}})))
	_, err := d.pool.Exec(ctx, `INSERT INTO chain_counters (name, count) VALUES ($1, 0)`, addressStatsRevertsCounter)
	require.NoError(t, err)

	batch, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer batch.Rollback(ctx)
	_, err = batch.Exec(ctx, `SELECT count FROM chain_counters WHERE name = $1 FOR SHARE`, addressStatsRevertsCounter)
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() { result <- d.DeleteBlock(ctx, 2) }()
	select {
	case err := <-result:
		t.Fatalf("the revert ran beside a repair batch: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	_, err = batch.Exec(ctx, `SELECT 1 FROM address_stats WHERE address IN ('0xa', '0xc') FOR UPDATE NOWAIT`)
	require.NoError(t, err, "the waiting revert holds no address_stats row")
	require.NoError(t, batch.Commit(ctx))
	require.NoError(t, <-result)
	require.Equal(t, seenRow{txs: 1, first: 1, last: 1}, addressSeen(t, d, "0xa"))
}

// A stage an earlier build made recorded no revert count; whether a reorg ran since is unknown, so it is stale.
func TestAddressStatsRepairStageWithoutARevertCountIsStale(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = 5`)
	require.NoError(t, err)

	for _, earlier := range []string{
		`ALTER TABLE ` + addressStatsRepairTotalTable + ` DROP COLUMN reverts`,
		`DROP TABLE ` + addressStatsRepairTotalTable,
	} {
		_, err = d.StageAddressStatsRepair(ctx, false)
		require.NoError(t, err)
		_, err = d.pool.Exec(ctx, earlier)
		require.NoError(t, err)
		_, _, _, err = d.ApplyAddressStatsRepairBatch(ctx, 1)
		require.ErrorIs(t, err, ErrAddressStatsRepairStale, earlier)
		require.Equal(t, seenRow{txs: 5, first: 1, last: 1}, addressSeen(t, d, "0xa"), "nothing applied")
	}
	require.Zero(t, repairUntilDone(t, d), "nothing staged is left")
	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.Zero(t, repairUntilDone(t, d))
	require.Equal(t, seenRow{txs: 1, first: 1, last: 1}, addressSeen(t, d, "0xa"))
}

// The startup re-seed of chain_counters writes only the totals it derives: the revert count survives it, so a stage
// stays valid across a restart without a reorg and stale across one with a reorg.
func TestTheStartupReseedKeepsTheRevertCount(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(1, "h1", []revertTx{{from: "0xa", to: "0xb"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(2, "h2", []revertTx{{from: "0xa", to: "0xc"}})))
	require.NoError(t, d.InsertBlockDataBatch(ctx, revertTestBlock(3, "h3", []revertTx{{from: "0xa", to: "0xd"}})))
	require.NoError(t, d.DeleteBlock(ctx, 3))
	reverts := func() int64 {
		var n int64
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT count FROM chain_counters WHERE name = $1`, addressStatsRevertsCounter).Scan(&n))
		return n
	}
	require.EqualValues(t, 1, reverts())
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = 0 WHERE address = '0xa'`)
	require.NoError(t, err)

	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.NoError(t, d.Reconcile(ctx))
	require.EqualValues(t, 1, reverts(), "the re-seed keeps the revert count")
	require.Zero(t, repairUntilDone(t, d), "no reorg since the stage: it applies")
	require.Equal(t, seenRow{txs: 2, first: 1, last: 2}, addressSeen(t, d, "0xa"))

	_, err = d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = 0 WHERE address = '0xa'`)
	require.NoError(t, err)
	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	require.NoError(t, d.DeleteBlock(ctx, 2))
	require.NoError(t, d.Reconcile(ctx))
	_, _, _, err = d.ApplyAddressStatsRepairBatch(ctx, 1)
	require.ErrorIs(t, err, ErrAddressStatsRepairStale, "a restart after the reorg does not make the stage valid")
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
