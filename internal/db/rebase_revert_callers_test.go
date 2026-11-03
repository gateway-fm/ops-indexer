package db

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

// The running check's repair of a missing row, across a revert that commits between its read and its lock and the
// block stored in its place, which creates the row again from that block alone: the row ends equal to its truth.
func TestTheRunningRepairOfAMissingRowAcrossARevertWhoseReplacementCreatesTheRow(t *testing.T) {
	d, ctx := repairFixture(t)
	repairUntilDone(t, d)
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
	_, err := d.CheckRunning(ctx, rebaseHotBucket(t), true)
	require.NoError(t, err)
	rebaseReadHook = nil
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

// A revert of the head that starts once a rebase creating two missing rows has taken them and read the reverts since
// its read: the revert subtracts its block from the rows the rebase created rather than miss them, so the row of the
// address older blocks still see ends at its truth, the row of the address only the head saw is removed, and
// addresses_total counts the rows.
func TestARevertDuringARebaseCreatingRowsSubtractsFromTheRowsItCreates(t *testing.T) {
	d, ctx := repairFixture(t)
	repairUntilDone(t, d)
	fresh := rebaseNewAddress(13501)
	deleteRowsKeepingTotal(t, d, rebaseHot, fresh)
	reverted := make(chan error, 1)
	var once sync.Once
	rebaseLockedHook = func() {
		once.Do(func() {
			go func() { reverted <- d.DeleteBlock(ctx, 13501) }()
			select {
			case err := <-reverted:
				reverted <- err
			case <-time.After(2 * time.Second):
			}
		})
	}
	t.Cleanup(func() { rebaseLockedHook = nil })
	r, err := d.RebaseAddresses(ctx, []string{rebaseHot, fresh}, []string{rebaseHot, fresh})
	require.NoError(t, err)
	rebaseLockedHook = nil
	require.Equal(t, 2, r.Created, "both rows are created at their truth as read")
	require.NoError(t, <-reverted)
	var freshRows int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM address_stats WHERE address = $1`, fresh).Scan(&freshRows))
	require.Zero(t, freshRows, "the revert removes the row of the address only the head saw")
	requireAddressStatsIsExactlyTheTruth(t, d)
	requireAddressStatsIsTheTruth(t, d)
}

// The same for the running check's repair, which finds both rows missing.
func TestARevertDuringARunningRepairCreatingRowsSubtractsFromTheRowsItCreates(t *testing.T) {
	d, ctx := repairFixture(t)
	repairUntilDone(t, d)
	fresh := rebaseNewAddress(13501)
	deleteRowsKeepingTotal(t, d, rebaseHot, fresh)
	reverted := make(chan error, 1)
	var once sync.Once
	rebaseLockedHook = func() {
		once.Do(func() {
			go func() { reverted <- d.DeleteBlock(ctx, 13501) }()
			select {
			case err := <-reverted:
				reverted <- err
			case <-time.After(2 * time.Second):
			}
		})
	}
	t.Cleanup(func() { rebaseLockedHook = nil })
	_, err := d.CheckRunning(ctx, rebaseHotBucket(t), true)
	require.NoError(t, err)
	rebaseLockedHook = nil
	require.NoError(t, <-reverted)
	var freshRows int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM address_stats WHERE address = $1`, fresh).Scan(&freshRows))
	require.Zero(t, freshRows, "the revert removes the row of the address only the head saw")
	requireAddressStatsIsExactlyTheTruth(t, d)
	requireAddressStatsIsTheTruth(t, d)
}

// revertLockWaiters is how many sessions wait for the revert lock.
func revertLockWaiters(t *testing.T, d *DB) int {
	t.Helper()
	var waiting int
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pg_locks l JOIN pg_database db ON db.oid = l.database
		WHERE l.locktype = 'advisory' AND NOT l.granted AND db.datname = current_database()
		  AND l.classid = $1 AND l.objid = $2 AND l.objsubid = 2`, rebaseLock, revertLock).Scan(&waiting))
	return waiting
}

// openRevert begins a revert of the head and runs it up to and including its address_stats rows: it holds the
// daily-stats lock, the revert lock and the head's rows.
func openRevert(t *testing.T, d *DB) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	revert, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = revert.Rollback(context.Background()) })
	_, err = revert.Exec(ctx, `SELECT daily_stats_invalidate(13501)`)
	require.NoError(t, err)
	_, err = revertAddressStats(ctx, revert, 13501)
	require.NoError(t, err)
	return revert
}

// A rebase batch that creates rows takes the revert lock holding only the rebase lock (and, in a span close, its
// stage); a revert takes it holding the daily-stats lock and its block's balances, tokens and nft_tokens rows, none of
// which a batch takes. In either order the second waits for the first on the revert lock and both finish, for the
// repair, the running check's repair and a span close: a revert holding everything up to and including its rows while the batch reaches the lock
// (in a span close, a revert begun after the close's day recount, between its stage and its batch), and a batch
// holding the lock and its rows while a revert starts.
func TestARevertAndARebaseCreatingRowsDoNotDeadlockInEitherOrder(t *testing.T) {
	missing := rebaseNewAddress(210)
	finishes := func(t *testing.T, done <-chan error, what string) {
		t.Helper()
		select {
		case err := <-done:
			require.NoError(t, err, what)
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not finish: deadlock", what)
		}
	}
	created := func(t *testing.T, d *DB) {
		t.Helper()
		var rows int
		require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT count(*) FROM address_stats WHERE address = $1`,
			missing).Scan(&rows))
		require.Equal(t, 1, rows, "the batch created the missing row")
	}
	repair := func(ctx context.Context, d *DB) error {
		_, err := d.RepairAddressBucket(ctx, 0, AddressRepairBatch)
		return err
	}
	running := func(ctx context.Context, d *DB) error {
		_, err := d.CheckRunning(ctx, 0, true)
		return err
	}
	spanClose := func(ctx context.Context, d *DB) error {
		closed, err := d.CloseLedgerSpans(ctx, repairLedger, 1)
		if err == nil && closed != 1 {
			err = fmt.Errorf("closed %d spans, not 1", closed)
		}
		return err
	}
	revertFirst := func(t *testing.T, rebase func(context.Context, *DB) error, open func(d *DB, opened chan<- pgx.Tx)) {
		d, ctx := repairFixture(t)
		deleteRowsKeepingTotal(t, d, missing)
		opened := make(chan pgx.Tx, 1)
		open(d, opened)
		done := make(chan error, 1)
		go func() { done <- rebase(ctx, d) }()
		revert := <-opened
		require.Eventually(t, func() bool { return revertLockWaiters(t, d) == 1 }, 10*time.Second, 20*time.Millisecond,
			"the batch creating the row waits for the revert")
		require.NoError(t, revert.Rollback(ctx))
		finishes(t, done, "the rebase")
		created(t, d)
	}
	batchFirst := func(t *testing.T, rebase func(context.Context, *DB) error) {
		d, ctx := repairFixture(t)
		deleteRowsKeepingTotal(t, d, missing)
		reverted := make(chan error, 1)
		var once sync.Once
		rebaseLockedHook = func() {
			once.Do(func() {
				go func() { reverted <- d.DeleteBlock(ctx, 13501) }()
				require.Eventually(t, func() bool { return revertLockWaiters(t, d) == 1 }, 10*time.Second,
					20*time.Millisecond, "the revert waits for the batch creating the row")
			})
		}
		t.Cleanup(func() { rebaseLockedHook = nil })
		done := make(chan error, 1)
		go func() { done <- rebase(ctx, d) }()
		finishes(t, done, "the rebase")
		rebaseLockedHook = nil
		finishes(t, reverted, "the revert")
		created(t, d)
	}

	t.Run("repair, revert first", func(t *testing.T) {
		revertFirst(t, repair, func(d *DB, opened chan<- pgx.Tx) { opened <- openRevert(t, d) })
	})
	t.Run("span close, revert first", func(t *testing.T) {
		revertFirst(t, spanClose, func(d *DB, opened chan<- pgx.Tx) {
			var once sync.Once
			spanRebaseStagedHook = func(int64) { once.Do(func() { opened <- openRevert(t, d) }) }
			t.Cleanup(func() { spanRebaseStagedHook = nil })
		})
	})
	t.Run("running check, revert first", func(t *testing.T) {
		revertFirst(t, running, func(d *DB, opened chan<- pgx.Tx) { opened <- openRevert(t, d) })
	})
	t.Run("repair, batch first", func(t *testing.T) { batchFirst(t, repair) })
	t.Run("span close, batch first", func(t *testing.T) { batchFirst(t, spanClose) })
	t.Run("running check, batch first", func(t *testing.T) { batchFirst(t, running) })
}
