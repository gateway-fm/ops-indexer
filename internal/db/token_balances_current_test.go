package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/stretchr/testify/require"
)

// token_balances_current caches, per (token, address), the row that a
// DISTINCT ON (address) ... ORDER BY address, block_number DESC over the
// append-only balances table would have returned. holder_count is then a
// filtered count over it rather than a scan of the token's whole history.
//
// The swap is only sound while the cache and that query agree, so the tests
// here pin exactly that:
//
//   - the count over the table equals the count the old query returns,
//     including addresses that cross zero in both directions;
//   - the upsert guard keeps the highest block_number whatever order the
//     writes arrive in, since balance writes are produced concurrently by 15
//     workers and are not ordered;
//   - the startup reconcile repairs the table, which is what makes a rollback
//     to a build that does not maintain it survivable;
//   - the migration's backfill is idempotent.
//
// Writes go through InsertBalancesBatch rather than hand-written SQL, because
// the maintenance statement being tested lives there.
//
// See PRST-4493.

// splitBalanceFixture keys every row off a per-test id so these can share one
// database with each other and with the other suites -- setupBenchDB honours
// BENCH_DATABASE_URL and creates nothing.
type balFixture struct {
	token string
	base  int64
}

// newBalFixture returns the fixture and a function removing its rows. Register
// that with defer AFTER setupBenchDB's own cleanup, never with t.Cleanup:
// t.Cleanup runs after the enclosing function's defers, by which point the pool
// is closed and every delete fails with "closed pool".
func newBalFixture(t *testing.T, d *DB, id int64, baseBlock int64) (balFixture, func()) {
	t.Helper()
	ctx := context.Background()
	f := balFixture{token: fmt.Sprintf("0xbal%038d", id), base: baseBlock}

	drop := func(when string) {
		for _, q := range []string{
			`DELETE FROM balances WHERE token_address = $1`,
			`DELETE FROM token_balances_current WHERE token_address = $1`,
		} {
			if _, err := d.pool.Exec(ctx, q, f.token); err != nil {
				t.Errorf("%s: %v", when, err)
			}
		}
	}
	// Pre-clean: a run killed before its defer leaves rows behind, and the next
	// run would then measure a fixture it did not build.
	drop("pre-clean")

	return f, func() { drop("cleanup") }
}

// writeBal pushes balances through the real writer, in the order given, so the
// upsert guard sees the arrival order the test intends. Each row is
// {holder, blockOffset, balance}.
func (f balFixture) writeBal(t *testing.T, d *DB, rows ...[3]int64) {
	t.Helper()
	batch := make([]*types.Balance, 0, len(rows))
	for _, r := range rows {
		batch = append(batch, &types.Balance{
			Address:      fmt.Sprintf("0xbalholder%031d", r[0]),
			TokenAddress: f.token,
			BlockNumber:  uint64(f.base + r[1]),
			Balance:      types.JSONString(fmt.Sprintf("%d", r[2])),
		})
	}
	require.NoError(t, d.InsertBalancesBatch(context.Background(), batch))
}

// holdersViaHistory is the query token_balances_current replaces. Kept here as
// the oracle: if these two ever disagree, the cache is wrong.
func (f balFixture) holdersViaHistory(t *testing.T, d *DB) int64 {
	t.Helper()
	var n int64
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM (
			SELECT DISTINCT ON (address) balance
			FROM balances
			WHERE token_address = $1
			ORDER BY address, block_number DESC
		) latest
		WHERE balance > 0`, f.token).Scan(&n))
	return n
}

func (f balFixture) currentRow(t *testing.T, d *DB, holder int64) (balance string, block int64) {
	t.Helper()
	err := d.pool.QueryRow(context.Background(), `
		SELECT balance::text, block_number FROM token_balances_current
		WHERE token_address = $1 AND address = $2`,
		f.token, fmt.Sprintf("0xbalholder%031d", holder)).Scan(&balance, &block)
	require.NoError(t, err)
	return
}

// TestCurrentBalances_AgreesWithHistoryQuery is the property the whole swap
// rests on. The fixture covers every membership transition, and the rows are
// handed to the writer out of block order because that is how they arrive.
func TestCurrentBalances_AgreesWithHistoryQuery(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newBalFixture(t, d, 1, 4_493_100_000)
	defer drop()

	// holder 1 holds throughout.
	// holder 2 held, then went to zero.
	// holder 3 went to zero and came back.
	// holder 4 has only ever been zero.
	// Deliberately not in block order: 3's newest row is written first.
	f.writeBal(t, d,
		[3]int64{3, 3, 500},
		[3]int64{1, 1, 1000},
		[3]int64{3, 1, 1000},
		[3]int64{2, 2, 0},
		[3]int64{4, 1, 0},
		[3]int64{3, 2, 0},
		[3]int64{2, 1, 1000},
	)

	got, err := d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.Equal(t, int64(2), got,
		"only holders 1 and 3 currently hold a non-zero balance")
	require.Equal(t, f.holdersViaHistory(t, d), got,
		"the cache must return exactly what the history query returns")

	// A later zero for holder 1 drops it back out, on both sides.
	f.writeBal(t, d, [3]int64{1, 4, 0})
	got, err = d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
	require.Equal(t, f.holdersViaHistory(t, d), got)
}

// TestCurrentBalances_GuardKeepsHighestBlock pins the upsert guard. The 15
// balance workers write concurrently and unordered, so a late-arriving write
// for an earlier block must not overwrite a newer balance.
func TestCurrentBalances_GuardKeepsHighestBlock(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()

	f, drop := newBalFixture(t, d, 2, 4_493_200_000)
	defer drop()

	// Newest first, then an older block in a separate batch: the older write
	// must lose.
	f.writeBal(t, d, [3]int64{1, 10, 999})
	f.writeBal(t, d, [3]int64{1, 5, 111})

	bal, blk := f.currentRow(t, d, 1)
	require.Equal(t, "999", bal, "an older block must not overwrite a newer balance")
	require.Equal(t, f.base+10, blk)

	// A newer block does win, in either arrival order.
	f.writeBal(t, d, [3]int64{1, 20, 222})
	bal, blk = f.currentRow(t, d, 1)
	require.Equal(t, "222", bal)
	require.Equal(t, f.base+20, blk)

	// Same block number is last-write-wins, matching the balances upsert.
	f.writeBal(t, d, [3]int64{1, 20, 333})
	bal, _ = f.currentRow(t, d, 1)
	require.Equal(t, "333", bal, "an identical block_number is last-write-wins, as in balances")
}

// TestCurrentBalances_ReconcileRepairsDrift covers the rollback case: a build
// that does not maintain the table keeps advancing balances, and because
// migration 008 is already recorded as applied nothing else would ever repair
// it. Simulated by tampering with the table directly.
func TestCurrentBalances_ReconcileRepairsDrift(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newBalFixture(t, d, 3, 4_493_900_000)
	defer drop()

	f.writeBal(t, d,
		[3]int64{1, 1, 1000},
		[3]int64{2, 1, 2000},
		[3]int64{3, 1, 0},
	)
	require.Equal(t, int64(2), f.holdersViaHistory(t, d))

	// Now behave like a build that writes balances but not the cache: insert
	// straight into balances, bypassing InsertBalancesBatch. Holder 3 acquires a
	// balance and holder 1's changes, at later blocks, which is what an advancing
	// chain does during a rollback window.
	for _, r := range [][3]int64{{3, 5, 3000}, {1, 6, 4000}} {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO balances (address, token_address, block_number, balance)
			VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("0xbalholder%031d", r[0]), f.token, f.base+r[1], r[2])
		require.NoError(t, err)
	}
	// Plus drift the cache can only have acquired earlier: one row stale, one
	// gone. Both sit below every block written above, which is the shape no
	// watermark-style detector could have seen; the reconcile is total, so it
	// repairs them regardless.
	_, err := d.pool.Exec(ctx, `
		UPDATE token_balances_current SET balance = 1
		WHERE token_address = $1 AND address = $2`,
		f.token, fmt.Sprintf("0xbalholder%031d", 2))
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		DELETE FROM token_balances_current WHERE token_address = $1 AND address = $2`,
		f.token, fmt.Sprintf("0xbalholder%031d", 3))
	require.NoError(t, err)

	// The cache is now wrong in three different ways at once.
	stale, err := d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.NotEqual(t, f.holdersViaHistory(t, d), stale,
		"precondition: the cache must actually be wrong, or this test proves nothing")

	require.NoError(t, d.reconcileTokenBalancesCurrent(ctx))

	after, err := d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.Equal(t, int64(3), after, "all three holders now hold a non-zero balance")
	require.Equal(t, f.holdersViaHistory(t, d), after,
		"reconcile must leave the cache agreeing with the history query")

	bal, blk := f.currentRow(t, d, 1)
	require.Equal(t, "4000", bal, "a balance written behind the cache's back must be picked up")
	require.Equal(t, f.base+6, blk)
	bal, _ = f.currentRow(t, d, 2)
	require.Equal(t, "2000", bal, "the stale row must be repaired even though it is below the watermark")
	bal, _ = f.currentRow(t, d, 3)
	require.Equal(t, "3000", bal, "the deleted row must be restored")
}

// TestCurrentBalances_ReconcileRepairsDriftBelowTheWatermark is the case that
// killed the original watermark-probe design, so it gets its own test.
//
// Balance writes are not ordered: the missing-range collector reprocesses
// historical blocks continuously, so a build without the maintenance path can
// introduce a previously unseen (token, address) at a LOW block number while
// some unrelated row already holds a far higher global maximum. A probe asking
// "is there a balance row above the cache's highest block?" sees nothing and
// skips, and that holder is missing from the count forever.
//
// The reconcile must therefore not depend on any watermark.
func TestCurrentBalances_ReconcileRepairsDriftBelowTheWatermark(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newBalFixture(t, d, 5, 4_493_500_000)
	defer drop()

	// One holder at a very high block, cached properly. This is what pins the
	// global watermark high.
	f.writeBal(t, d, [3]int64{1, 400_000, 1000})

	// Now a build without the maintenance path introduces a NEW holder at a low
	// block: straight into balances, nothing in the cache, and far below the
	// watermark the row above just established.
	_, err := d.pool.Exec(ctx, `
		INSERT INTO balances (address, token_address, block_number, balance)
		VALUES ($1, $2, $3, $4)`,
		fmt.Sprintf("0xbalholder%031d", 2), f.token, f.base+7, 5000)
	require.NoError(t, err)

	// Precondition: the cache is wrong, and wrong in a way no watermark can see.
	var cacheMax, rowsAbove int64
	require.NoError(t, d.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(block_number), -1) FROM token_balances_current`).Scan(&cacheMax))
	require.NoError(t, d.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM balances WHERE block_number > $1`, cacheMax).Scan(&rowsAbove))
	require.Zero(t, rowsAbove,
		"precondition: nothing sits above the cache watermark, so a watermark probe would skip")

	stale, err := d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.Equal(t, int64(1), stale, "precondition: the new holder is missing from the cache")
	require.Equal(t, int64(2), f.holdersViaHistory(t, d), "but present in balances")

	require.NoError(t, d.reconcileTokenBalancesCurrent(ctx))

	got, err := d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.Equal(t, int64(2), got, "reconcile must find drift that sits below the watermark")
	require.Equal(t, f.holdersViaHistory(t, d), got)

	bal, blk := f.currentRow(t, d, 2)
	require.Equal(t, "5000", bal)
	require.Equal(t, f.base+7, blk)
}

// TestCurrentBalances_WipedWithBalancesOnChainReset covers the FORCE_REINDEX
// path. WipeAllData truncates balances; leaving the cache behind is worse than
// stale, because re-indexing from block 0 writes LOWER block numbers and the
// maintenance guard is highest-block-wins -- so the old chain's rows would
// refuse to be replaced and would be served indefinitely.
//
// The only test here that does not confine itself to its fixture's rows:
// WipeAllData truncates every indexed table, which is fine on the throwaway
// container and destructive on a BENCH_DATABASE_URL server, where the seeded
// rows are meant to be reused across runs. Worse than losing them: it does not
// take bench_seed_state with it -- that table is not in the wipe list -- so the
// seeder would go on claiming rows that no longer exist and every later
// benchmark would run against an empty table. Skipped there rather than made
// safe: nothing makes truncating the whole database safe on a shared server.
func TestCurrentBalances_WipedWithBalancesOnChainReset(t *testing.T) {
	if url := strings.TrimSpace(os.Getenv("BENCH_DATABASE_URL")); url != "" {
		t.Skip("skipping: this test truncates every table, and BENCH_DATABASE_URL points at a reusable database")
	}

	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newBalFixture(t, d, 6, 4_493_600_000)
	defer drop()

	// "Old chain": a holder cached at a high block.
	f.writeBal(t, d, [3]int64{1, 500_000, 9999})
	require.Equal(t, int64(1), mustCount(t, d, f.token))

	require.NoError(t, d.WipeAllData(ctx))

	require.Zero(t, mustCount(t, d, f.token),
		"the cache must be truncated along with balances, or the old chain's holders survive")

	// "New chain" from block 0: a LOWER block number than the wiped row held.
	// Without the wipe this write would have been refused by the guard.
	newFixture := balFixture{token: f.token, base: 0}
	newFixture.writeBal(t, d, [3]int64{1, 3, 1234})

	bal, blk := newFixture.currentRow(t, d, 1)
	require.Equal(t, "1234", bal, "the re-indexed chain's balance must win after a wipe")
	require.Equal(t, int64(3), blk)
}

func mustCount(t *testing.T, d *DB, token string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, d.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM token_balances_current WHERE token_address = $1`, token).Scan(&n))
	return n
}

// TestCurrentBalances_BackfillIsIdempotent runs the migration's backfill body
// against a table that already holds the rows. The migration is marked applied
// after the first run, but the same statement is reachable through the
// reconcile path, so re-running it must not duplicate or corrupt anything.
func TestCurrentBalances_BackfillIsIdempotent(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newBalFixture(t, d, 4, 4_493_400_000)
	defer drop()

	f.writeBal(t, d,
		[3]int64{1, 2, 1000},
		[3]int64{1, 1, 500},
		[3]int64{2, 1, 0},
	)

	countRows := func() (n int64) {
		require.NoError(t, d.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM token_balances_current WHERE token_address = $1`,
			f.token).Scan(&n))
		return
	}
	rowsBefore, holdersBefore := countRows(), f.holdersViaHistory(t, d)

	for i := 0; i < 2; i++ {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO token_balances_current (token_address, address, balance, block_number)
			SELECT DISTINCT ON (token_address, address) token_address, address, balance, block_number
			FROM balances
			ORDER BY token_address, address, block_number DESC
			ON CONFLICT (token_address, address) DO NOTHING`)
		require.NoError(t, err)
	}

	require.Equal(t, rowsBefore, countRows(), "backfill must not add rows on a populated table")
	got, err := d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.Equal(t, holdersBefore, got)

	bal, blk := f.currentRow(t, d, 1)
	require.Equal(t, "1000", bal, "the newest balance must survive a re-run")
	require.Equal(t, f.base+2, blk)
}

// TestCurrentBalances_RepeatedKeyInOneBatchKeepsTheWinner pins the collapse of a
// key's rows inside a single batch. One flush routinely carries several blocks
// for the same (token, address) -- the queue is not deduplicated -- and the
// batch now issues one upsert per key instead of one per row, so the row it
// picks has to be the one the per-row guard would have settled on.
func TestCurrentBalances_RepeatedKeyInOneBatchKeepsTheWinner(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newBalFixture(t, d, 7, 4_493_700_000)
	defer drop()

	// One batch, one key, blocks out of order: the highest block wins, not the
	// last one queued.
	//
	// Holder 2 repeats an identical block in the same batch, where the guard's
	// >= makes it last-write-wins; balances resolves that pair the same way, and
	// the two must not disagree.
	f.writeBal(t, d,
		[3]int64{1, 5, 500},
		[3]int64{1, 9, 900},
		[3]int64{1, 2, 200},
		[3]int64{2, 3, 111},
		[3]int64{2, 3, 222},
	)

	bal, blk := f.currentRow(t, d, 1)
	require.Equal(t, "900", bal, "the highest block in the batch must win, not the last queued")
	require.Equal(t, f.base+9, blk)

	bal, _ = f.currentRow(t, d, 2)
	require.Equal(t, "222", bal, "an identical block repeated in one batch is last-write-wins")

	got, err := d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.Equal(t, f.holdersViaHistory(t, d), got)
}

// TestCurrentBalances_ConcurrentOverlappingBatchesDoNotDeadlock covers what the
// cache introduced: a row lock per (token, address), held to commit, where
// balances alone never contended because its key carries block_number. Two of
// the 15 workers flushing overlapping keys in opposite orders could each hold
// the row the other wanted; flushBatch retries a 40P01 three times and then
// drops the batch, so the cost is lost balances and a wrong holder_count.
//
// Opposite orders over a wide key set is the shape that produces the cycle. The
// assertion is that every write succeeds -- InsertBalancesBatch has to be
// deadlock-free by construction, since its caller's retries are finite.
func TestCurrentBalances_ConcurrentOverlappingBatchesDoNotDeadlock(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newBalFixture(t, d, 8, 4_493_800_000)
	defer drop()

	const keys, rounds = 50, 20

	// Each writer owns its own block numbers, so the highest block -- and
	// therefore the row the guard keeps -- is the same whatever order the two
	// arrive in, and the final state is assertable.
	batchFor := func(round, writer int, reverse bool) []*types.Balance {
		out := make([]*types.Balance, 0, keys)
		for i := 0; i < keys; i++ {
			k := i
			if reverse {
				k = keys - 1 - i
			}
			out = append(out, &types.Balance{
				Address:      fmt.Sprintf("0xbalholder%031d", 100+k),
				TokenAddress: f.token,
				BlockNumber:  uint64(f.base + int64(round*2+writer)),
				Balance:      types.JSONString(fmt.Sprintf("%d", 1000+round*2+writer)),
			})
		}
		return out
	}

	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for writer := 0; writer < 2; writer++ {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				errs[writer] = d.InsertBalancesBatch(ctx, batchFor(round, writer, writer == 1))
			}(writer)
		}
		wg.Wait()
		for writer, err := range errs {
			require.NoErrorf(t, err, "round %d writer %d: concurrent overlapping batches must not deadlock", round, writer)
		}
	}

	// The last writer of the last round holds the highest block for every key.
	wantBlock := f.base + int64((rounds-1)*2+1)
	wantBalance := fmt.Sprintf("%d", 1000+(rounds-1)*2+1)
	for k := 0; k < keys; k++ {
		bal, blk := f.currentRow(t, d, int64(100+k))
		require.Equal(t, wantBalance, bal, "key %d", k)
		require.Equal(t, wantBlock, blk, "key %d", k)
	}

	got, err := d.countTokenHolders(ctx, f.token)
	require.NoError(t, err)
	require.Equal(t, int64(keys), got)
	require.Equal(t, f.holdersViaHistory(t, d), got)
}
