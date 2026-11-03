package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// storeBulkDays stores blocks 0 to blocks-1 spread evenly over three UTC days, each with txs transactions between a
// handful of addresses, in two statements.
func storeBulkDays(t *testing.T, d *DB, blocks, txs int64) {
	t.Helper()
	ctx := context.Background()
	step := 3 * 86400 / blocks
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, size)
		SELECT n, '0xbulk' || n, '0xbulk' || (n - 1), $1 + n * $2, 21000 + n, 30000000, $3, 600 + n % 7
		FROM generate_series(0, $4 - 1) n`, dayStart-2*86400, step, txs, blocks)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, status)
		SELECT '0xbulk' || n || '_' || i, n, i, '0x' || lpad(to_hex((n * 7 + i) % 13), 40, '0'),
		       '0x' || lpad(to_hex(100 + (n + i) % 5), 40, '0'), 0, 21000, 1000000007 + i, (n + i) % 5 / 4 # 1
		FROM generate_series(0, $1 - 1) n CROSS JOIN generate_series(0, $2 - 1) i`, blocks, txs)
	require.NoError(t, err)
}

// The first advance after a restart recounts every day whose stored blocks it finds uncounted. It used to count them
// holding (4670, 1), the lock a revert takes first, for as long as the counting took: 30.7 s on a 3,000 tx/s test chain.
// A waiter for the lock now waits only for the fold's writes, a small part of the fold.
func TestTheFoldLetsALockWaiterThroughWhileItCounts(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	storeBulkDays(t, d, 12000, 50)
	_, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err, "the first advance starts at the newest day; the next finds the two before it uncounted")

	stop := make(chan struct{})
	var longest time.Duration
	var probing sync.WaitGroup
	probing.Add(1)
	go func() {
		defer probing.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			start := time.Now()
			tx, err := d.pool.Begin(ctx)
			require.NoError(t, err)
			_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`)
			require.NoError(t, err)
			if waited := time.Since(start); waited > longest {
				longest = waited
			}
			require.NoError(t, tx.Commit(ctx))
			time.Sleep(20 * time.Millisecond)
		}
	}()
	start := time.Now()
	upto, err := d.AdvanceDailyStats(ctx)
	took := time.Since(start)
	close(stop)
	probing.Wait()
	require.NoError(t, err)
	require.EqualValues(t, 11999, upto)
	requireStoredDaysEqualRecompute(t, d, "three days counted after a restart")
	t.Logf("fold %s, longest wait for (4670, 1) %s", took, longest)
	require.Greater(t, took, 500*time.Millisecond, "the fold counts long enough to tell a held lock apart")
	require.Less(t, longest, took/3, "a waiter for (4670, 1) waits for the fold's writes, not its counting")
}
