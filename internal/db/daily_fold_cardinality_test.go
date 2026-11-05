package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// storeDistinctAddressDays stores blocks 0 to blocks-1 spread evenly over three UTC days, each with txs transactions
// whose sender and recipient no other transaction uses.
func storeDistinctAddressDays(t *testing.T, d *DB, blocks, txs int64) {
	t.Helper()
	ctx := context.Background()
	step := 3 * 86400 / blocks
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, size)
		SELECT n, '0xwide' || n, '0xwide' || (n - 1), $1 + n * $2, 21000 + n, 30000000, $3, 600 + n % 7
		FROM generate_series(0, $4 - 1) n`, dayStart-2*86400, step, txs, blocks)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, status)
		SELECT '0xwide' || n || '_' || i, n, i, '0x' || lpad(to_hex((n * $2 + i) * 2), 40, '0'),
		       '0x' || lpad(to_hex((n * $2 + i) * 2 + 1), 40, '0'), 0, 21000, 1000000007 + i, 1
		FROM generate_series(0, $1 - 1) n CROSS JOIN generate_series(0, $2 - 1) i`, blocks, txs)
	require.NoError(t, err)
}

// foldStagingBytes is the size of the rows in every session's fold staging tables, without their indexes, which an
// empty table keeps a page of.
func foldStagingBytes(t *testing.T, d *DB) int64 {
	t.Helper()
	var size int64
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT COALESCE(sum(pg_relation_size(c.oid)), 0)::BIGINT
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname LIKE 'pg_temp%' AND c.relname IN ('fold_acc', 'fold_active', 'fold_gone', 'fold_reset', 'fold_stats')`).Scan(&size))
	return size
}

// probeCommitLock takes (4670, 1) on its own connection over and over until the returned stop is called, which
// returns the longest it waited for it.
func probeCommitLock(t *testing.T, d *DB) (stop func() time.Duration) {
	t.Helper()
	ctx := context.Background()
	done := make(chan struct{})
	var longest time.Duration
	var probing sync.WaitGroup
	probing.Add(1)
	go func() {
		defer probing.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			start := time.Now()
			tx, err := d.pool.Begin(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`)
			if waited := time.Since(start); waited > longest {
				longest = waited
			}
			if err == nil {
				err = tx.Commit(ctx)
			}
			if err != nil {
				t.Error(err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	var stopping sync.Once
	stop = func() time.Duration {
		stopping.Do(func() { close(done) })
		probing.Wait()
		return longest
	}
	t.Cleanup(func() { stop() })
	return stop
}

// Three days of 600,000 transactions between 1.2 million addresses, each used once: the first advance counts the
// newest day, the second recounts the two before it and the newest again, and each day's address set is as large
// as it gets. A separate connection takes (4670, 1) over and over and records its longest wait in each advance. The
// apply rewrote every recounted day's set under the lock, which took the waiter's longest wait from 7.3 s to 21 s.
func TestTheFoldUnderManyDistinctAddresses(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	storeDistinctAddressDays(t, d, 1200, 500)

	var folds, waits [2]time.Duration
	var upto uint64
	for i := range folds {
		stop := probeCommitLock(t, d)
		start := time.Now()
		var err error
		upto, err = d.AdvanceDailyStats(ctx)
		folds[i], waits[i] = time.Since(start), stop()
		require.NoError(t, err)
	}
	require.EqualValues(t, 1199, upto)
	requireStoredDaysEqualRecompute(t, d, "three days of distinct addresses")
	staged := foldStagingBytes(t, d)
	t.Logf("folds %s and %s, longest waits for (4670, 1) %s and %s, staging left %d bytes", folds[0], folds[1], waits[0], waits[1], staged)
	require.Less(t, waits[1], folds[1]/4, "the recount writes the difference to the newest day's address set under the lock, not the three sets")
	require.Zero(t, staged, "staging left behind")
}
