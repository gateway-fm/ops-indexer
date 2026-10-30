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
