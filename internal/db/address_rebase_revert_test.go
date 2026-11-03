package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A rebase counts a row and its truth in one snapshot and adds the difference afterwards. A reorg's revert that
// commits in between subtracts with a floor at zero and moves last_seen down, so the difference no longer holds:
// an undercounted row ends above its truth with last_seen on the reverted block.
func TestRebaseAcrossARevertOfAnUndercountedRow(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 5; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = 0, sent_count = 0 WHERE address = $1`, rebaseHot)
	require.NoError(t, err, "0 against a truth of 10")
	slowTruth(t, d, 3)

	rebased := make(chan error, 1)
	go func() {
		_, err := d.RebaseAddresses(ctx, []string{rebaseHot}, nil)
		rebased <- err
	}()
	time.Sleep(time.Second)
	require.NoError(t, d.DeleteBlock(ctx, 5), "the head block is reverted while the rebase counts")
	require.NoError(t, <-rebased)
	requireAddressStatsIsTheTruth(t, d)
}

// The overcounted case: the correction is right, but last_seen goes back to the reverted block.
func TestRebaseAcrossARevertOfAnOvercountedRow(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 5; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 7 WHERE address = $1`, rebaseHot)
	require.NoError(t, err, "17 against a truth of 10")
	slowTruth(t, d, 3)

	rebased := make(chan error, 1)
	go func() {
		_, err := d.RebaseAddresses(ctx, []string{rebaseHot}, nil)
		rebased <- err
	}()
	time.Sleep(time.Second)
	require.NoError(t, d.DeleteBlock(ctx, 5), "the head block is reverted while the rebase counts")
	require.NoError(t, <-rebased)
	requireAddressStatsIsTheTruth(t, d)
}
