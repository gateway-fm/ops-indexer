package db

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// rebaseHotBucket is the address bucket of rebaseHot.
func rebaseHotBucket(t *testing.T) int {
	t.Helper()
	b, err := strconv.ParseInt(rebaseHot[2:4], 16, 0)
	require.NoError(t, err)
	return int(b)
}

// undercountHot leaves the hot address, which every block touches, below its truth.
func undercountHot(t *testing.T, d *DB) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), `UPDATE address_stats SET tx_count = 0, sent_count = 0 WHERE address = $1`, rebaseHot)
	require.NoError(t, err)
}

// deleteRowsKeepingTotal deletes the address_stats rows of addresses and takes them off addresses_total.
func deleteRowsKeepingTotal(t *testing.T, d *DB, addresses ...string) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), `
		WITH gone AS (DELETE FROM address_stats WHERE address = ANY ($1) RETURNING 1)
		UPDATE chain_counters SET count = count - (SELECT count(*) FROM gone) WHERE name = 'addresses_total'`, addresses)
	require.NoError(t, err)
}

// In each caller of the rebase, a revert of the head block that commits between the read of the hot address's row and
// truth and the lock of its row: the row ends equal to its truth.

func TestTheRepairRebaseAcrossARevertBetweenItsReadAndItsLock(t *testing.T) {
	d, _ := repairFixture(t)
	undercountHot(t, d)
	var once sync.Once
	rebaseReadHook = func(batch []string) {
		for _, a := range batch {
			if a == rebaseHot {
				once.Do(func() { reorgTail(t, d) })
			}
		}
	}
	t.Cleanup(func() { rebaseReadHook = nil })
	repairUntilDone(t, d)
	requireAddressStatsIsExactlyTheTruth(t, d)
}

func TestTheRunningRepairAcrossARevertBetweenItsReadAndItsLock(t *testing.T) {
	d, ctx := repairFixture(t)
	repairUntilDone(t, d)
	undercountHot(t, d)
	var once sync.Once
	rebaseReadHook = func([]string) { once.Do(func() { reorgTail(t, d) }) }
	t.Cleanup(func() { rebaseReadHook = nil })
	_, err := d.CheckRunning(ctx, rebaseHotBucket(t), true)
	require.NoError(t, err)
	rebaseReadHook = nil
	requireAddressStatsIsExactlyTheTruth(t, d)
}

func TestTheSpanRebaseAcrossARevertBetweenItsStageAndItsLock(t *testing.T) {
	d, ctx := repairFixture(t)
	repairUntilDone(t, d)
	undercountHot(t, d)
	staged := false
	spanRebaseStagedHook = func(int64) {
		if !staged {
			staged = true
			reorgTail(t, d)
		}
	}
	t.Cleanup(func() { spanRebaseStagedHook = nil })
	closed, err := d.CloseLedgerSpans(ctx, repairLedger, 1)
	require.NoError(t, err)
	require.Equal(t, 1, closed, "the close rebases the span it closes")
	require.True(t, staged, "the revert landed between the span's stage and its batches")
	spanRebaseStagedHook = nil
	requireAddressStatsIsExactlyTheTruth(t, d)
}

// A repair creating a missing row, across a revert that commits between its read and its lock and the block stored in
// its place, which creates the row again from that block alone: the row ends equal to its truth.
func TestTheRepairOfAMissingRowAcrossARevertWhoseReplacementCreatesTheRow(t *testing.T) {
	d, _ := repairFixture(t)
	deleteRowsKeepingTotal(t, d, rebaseHot)
	var once sync.Once
	rebaseReadHook = func(batch []string) {
		for _, a := range batch {
			if a == rebaseHot {
				once.Do(func() { reorgTail(t, d) })
			}
		}
	}
	t.Cleanup(func() { rebaseReadHook = nil })
	repairUntilDone(t, d)
	rebaseReadHook = nil
	requireAddressStatsIsExactlyTheTruth(t, d)
	requireAddressStatsIsTheTruth(t, d)
}

func TestTheSpanRebaseOfAMissingRowAcrossARevertWhoseReplacementCreatesTheRow(t *testing.T) {
	d, ctx := repairFixture(t)
	repairUntilDone(t, d)
	deleteRowsKeepingTotal(t, d, rebaseHot)
	staged := false
	spanRebaseStagedHook = func(int64) {
		if !staged {
			staged = true
			reorgTail(t, d)
		}
	}
	t.Cleanup(func() { spanRebaseStagedHook = nil })
	closed, err := d.CloseLedgerSpans(ctx, repairLedger, 1)
	require.NoError(t, err)
	require.Equal(t, 1, closed, "the close rebases the span it closes")
	require.True(t, staged, "the revert landed between the span's stage and its batches")
	spanRebaseStagedHook = nil
	requireAddressStatsIsExactlyTheTruth(t, d)
	requireAddressStatsIsTheTruth(t, d)
}

// A rebase creating two missing rows, across a revert of the head with nothing stored in its place that commits
// between its read and its lock: the row of the address older blocks still see is created at its truth, the row of
// the address only the head saw is not created, and addresses_total counts the one created.
func TestARebaseCreatingRowsAcrossARevertCreatesOnlyThoseStillSeen(t *testing.T) {
	d, ctx := repairFixture(t)
	repairUntilDone(t, d)
	fresh := rebaseNewAddress(13501)
	deleteRowsKeepingTotal(t, d, rebaseHot, fresh)
	var once sync.Once
	rebaseReadHook = func([]string) { once.Do(func() { require.NoError(t, d.DeleteBlock(ctx, 13501)) }) }
	t.Cleanup(func() { rebaseReadHook = nil })
	r, err := d.RebaseAddresses(ctx, []string{rebaseHot, fresh}, []string{rebaseHot, fresh})
	require.NoError(t, err)
	rebaseReadHook = nil
	require.Equal(t, 1, r.Created, "the row of the address older blocks still see is created")
	require.Zero(t, r.Removed, "the row the batch inserted for the address only the head saw was never created")
	var freshRows int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM address_stats WHERE address = $1`, fresh).Scan(&freshRows))
	require.Zero(t, freshRows, "no row for an address no canonical block has seen")
	requireAddressStatsIsExactlyTheTruth(t, d)
	requireAddressStatsIsTheTruth(t, d)
}
