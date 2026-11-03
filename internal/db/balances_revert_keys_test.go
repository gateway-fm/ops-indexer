package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A revert reads the keys of its block before it takes their balance locks. A balance write at the reverted block for a
// key outside that read (a reread answered at the head, which is the block being reverted) that commits in between
// has its history row deleted by the revert, which then neither restores the key's current balance nor asks for it to
// be read again: the fork's value stays current, at a block that is gone. A flush under way holds the balance revert
// lock, so the revert waits for it before it reads its keys, and a write that starts meanwhile waits for the revert
// and then asks for its key to be read again rather than write a read decided before it.
func TestARevertRestoresOrRereadsABalanceWrittenAtItsBlockAfterItReadItsKeys(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	other := "0x00000000000000000000000000000000000000c3"
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(9)))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(9, [3]string{other, reorgBalToken, "5"})))
	require.NoError(t, d.InsertBlockDataBatch(ctx,
		reorgBalBlock(10, reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "1", nil))))

	flush, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer flush.Rollback(context.Background())
	_, err = flush.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, balanceRevertLock)
	require.NoError(t, err)
	_, err = flush.Exec(ctx, `SELECT pg_advisory_xact_lock(4669, $1)`, tbcLockKey(reorgBalToken, reorgBalB))
	require.NoError(t, err, "a balance flush holds a key of block 10")

	reverted := make(chan error, 1)
	go func() { reverted <- d.DeleteBlock(ctx, 10) }()
	require.Eventually(t, func() bool {
		var waiting bool
		_ = d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
			AND classid = $1)`, balanceRevertLock).Scan(&waiting)
		return waiting
	}, 20*time.Second, 20*time.Millisecond, "the revert waits for the flush under way")

	epoch, err := d.CurrentBalanceEpoch(ctx)
	require.NoError(t, err)
	b := reorgBalances(10, [3]string{other, reorgBalToken, "99"})
	b[0].Epoch = epoch
	written := make(chan error, 1)
	go func() { written <- d.InsertBalancesBatch(ctx, b) }()
	require.Eventually(t, func() bool {
		var waiting int
		_ = d.pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
			AND classid = $1`, balanceRevertLock).Scan(&waiting)
		return waiting == 2
	}, 20*time.Second, 20*time.Millisecond, "a read at the head, block 10, of a key block 10 did not touch, waits too")
	require.NoError(t, flush.Rollback(ctx))
	require.NoError(t, <-reverted)
	require.NoError(t, <-written)

	var balance string
	var block int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT balance::TEXT, block_number FROM token_balances_current
		WHERE token_address = $1 AND address = $2`, reorgBalToken, other).Scan(&balance, &block))
	var reread bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM balance_rereads
		WHERE token_address = $1 AND address = $2 AND open)`, reorgBalToken, other).Scan(&reread))
	require.True(t, block < 10 || reread,
		"the key's current balance is %s at the reverted block %d, with no request to read it again", balance, block)
}

// A flush of a row at the reverted block, for a key no read of the revert saw, started after the revert's last read
// and before it deletes the block's history rows, waits for the revert, and then asks for the key to be read again
// rather than write a read decided before it. The test starts it from beforeBalanceHistoryDelete and waits a bounded
// time for it.
func TestARevertRereadsAKeyItsHistoryDeleteFoundUnread(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	other := "0x00000000000000000000000000000000000000c4"
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(9)))
	require.NoError(t, d.InsertBlockDataBatch(ctx,
		reorgBalBlock(10, reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "1", nil))))

	written := make(chan error, 1)
	var once sync.Once
	beforeBalanceHistoryDelete = func() {
		once.Do(func() {
			epoch, err := d.CurrentBalanceEpoch(ctx)
			require.NoError(t, err)
			b := reorgBalances(10, [3]string{other, reorgBalToken, "7"})
			b[0].Epoch = epoch
			go func() { written <- d.InsertBalancesBatch(ctx, b) }()
			select {
			case err := <-written:
				written <- err
			case <-time.After(2 * time.Second):
			}
		})
	}
	t.Cleanup(func() { beforeBalanceHistoryDelete = nil })
	require.NoError(t, d.DeleteBlock(ctx, 10))
	require.NoError(t, <-written, "a read at the head, block 10, of a key block 10 did not touch")

	var reread bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM balance_rereads
		WHERE token_address = $1 AND address = $2 AND open)`, reorgBalToken, other).Scan(&reread))
	require.True(t, reread, "the key whose history row the revert deleted is read again")
}

// A balance flush already under way when a revert starts, for a key the reverted block did not touch, that commits
// after the revert's history delete took its snapshot: the delete cannot see its row, so without a fence the key keeps
// the fork's balance at a block that is gone, with no request to read it again. The flush waits for the revert
// instead, and then neither writes a read decided before it nor leaves the key unread: it asks for the key to be read
// again. The test starts the flush from afterBalanceHistoryDelete and waits a bounded time for it.
func TestABalanceFlushLandingAfterARevertsHistoryDeleteIsReadAgainNotWritten(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	late := "0x00000000000000000000000000000000000000c5"
	require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(9)))
	require.NoError(t, d.InsertBlockDataBatch(ctx,
		reorgBalBlock(10, reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "1", nil))))
	require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(10, [3]string{reorgBalA, reorgBalToken, "4"},
		[3]string{reorgBalB, reorgBalToken, "1"})))
	epoch, err := d.CurrentBalanceEpoch(ctx)
	require.NoError(t, err)

	flushed := make(chan error, 1)
	var once sync.Once
	afterBalanceHistoryDelete = func() {
		once.Do(func() {
			b := reorgBalances(10, [3]string{late, reorgBalToken, "99"})
			b[0].Epoch = epoch
			go func() { flushed <- d.InsertBalancesBatch(ctx, b) }()
			select {
			case err := <-flushed:
				flushed <- err
			case <-time.After(2 * time.Second):
			}
		})
	}
	t.Cleanup(func() { afterBalanceHistoryDelete = nil })
	require.NoError(t, d.DeleteBlock(ctx, 10))
	require.NoError(t, <-flushed)

	var current, history int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM token_balances_current WHERE token_address = $1 AND address = $2),
		       (SELECT count(*) FROM balances WHERE token_address = $1 AND address = $2)`, reorgBalToken, late).
		Scan(&current, &history))
	require.Zero(t, current, "no current balance from a read decided before the revert")
	require.Zero(t, history, "no history row from it either")
	var holders, positive int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT COALESCE(holder_count, 0) FROM tokens WHERE address = $1),
		       (SELECT count(*) FROM token_balances_current WHERE token_address = $1 AND balance > 0)`, reorgBalToken).
		Scan(&holders, &positive))
	require.Equal(t, positive, holders, "holder_count counts the positive current balances")
	var reread bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM balance_rereads
		WHERE token_address = $1 AND address = $2 AND open)`, reorgBalToken, late).Scan(&reread))
	require.True(t, reread, "the key is read again at the new head")
}

// The balance revert lock in either order, against a head flush (InsertBalancesBatch, which takes it shared first),
// the reread loop's claim (ClaimBalanceRereads, likewise) and the stats backfill's current balances
// (InsertCurrentBalances, which does not take it): a writer holding
// a key of the reverted block while the revert starts, and a revert holding the lock, the block's keys and its
// history delete while the writer starts. The second waits for the first and both finish.
func TestARevertAndABalanceWriteDoNotDeadlockInEitherOrder(t *testing.T) {
	waiting := func(t *testing.T, d *DB, classid int) int {
		var n int
		require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted AND classid = $1`, classid).Scan(&n))
		return n
	}
	finishes := func(t *testing.T, done <-chan error, what string) {
		t.Helper()
		select {
		case err := <-done:
			require.NoError(t, err, what)
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not finish: deadlock", what)
		}
	}
	setup := func(t *testing.T) (*DB, context.Context) {
		d, cleanup := setupTestDB(t)
		t.Cleanup(cleanup)
		ctx := context.Background()
		require.NoError(t, d.InsertBlockDataBatch(ctx, reorgBalBlock(9)))
		require.NoError(t, d.InsertBlockDataBatch(ctx,
			reorgBalBlock(10, reorgBalTransfer(reorgBalToken, types.TokenTypeERC20, reorgBalA, reorgBalB, "1", nil))))
		require.NoError(t, d.InsertBalancesBatch(ctx, reorgBalances(10, [3]string{reorgBalB, reorgBalToken, "1"})))
		return d, ctx
	}
	writers := []struct {
		name   string
		shared bool
		write  func(ctx context.Context, d *DB) error
	}{
		{"head flush", true, func(ctx context.Context, d *DB) error {
			return d.InsertBalancesBatch(ctx, reorgBalances(10, [3]string{reorgBalB, reorgBalToken, "2"}))
		}},
		{"backfill current balances", false, func(ctx context.Context, d *DB) error {
			return d.InsertCurrentBalances(ctx, reorgBalances(3, [3]string{reorgBalB, reorgBalToken, "2"}))
		}},
		{"reread claim", true, func(ctx context.Context, d *DB) error {
			_, _, err := d.ClaimBalanceRereads(ctx, 100)
			return err
		}},
	}
	for _, w := range writers {
		t.Run(w.name+", writer first", func(t *testing.T) {
			d, ctx := setup(t)
			writer, err := d.pool.Begin(ctx)
			require.NoError(t, err)
			defer writer.Rollback(context.Background())
			if w.shared {
				_, err = writer.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, balanceRevertLock)
				require.NoError(t, err)
			}
			_, err = writer.Exec(ctx, `SELECT pg_advisory_xact_lock(4669, $1)`, tbcLockKey(reorgBalToken, reorgBalB))
			require.NoError(t, err, "the writer holds a key of block 10")
			reverted := make(chan error, 1)
			go func() { reverted <- d.DeleteBlock(ctx, 10) }()
			classid := 4669
			if w.shared {
				classid = balanceRevertLock
			}
			require.Eventually(t, func() bool { return waiting(t, d, classid) == 1 }, 20*time.Second,
				20*time.Millisecond, "the revert waits for the writer")
			require.NoError(t, writer.Commit(ctx))
			finishes(t, reverted, "the revert")
		})
		t.Run(w.name+", revert first", func(t *testing.T) {
			d, ctx := setup(t)
			written := make(chan error, 1)
			var once sync.Once
			afterBalanceHistoryDelete = func() {
				once.Do(func() {
					go func() { written <- w.write(ctx, d) }()
					classid := 4669
					if w.shared {
						classid = balanceRevertLock
					}
					require.Eventually(t, func() bool { return waiting(t, d, classid) == 1 }, 20*time.Second,
						20*time.Millisecond, "the writer waits for the revert")
				})
			}
			t.Cleanup(func() { afterBalanceHistoryDelete = nil })
			reverted := make(chan error, 1)
			go func() { reverted <- d.DeleteBlock(ctx, 10) }()
			finishes(t, reverted, "the revert")
			afterBalanceHistoryDelete = nil
			finishes(t, written, "the writer")
		})
	}
}
