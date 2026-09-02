package db

import (
	"context"
	"fmt"
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

	// Highest block range of any fixture here: the staleness probe compares a
	// GLOBAL watermark, so this fixture has to own the maximum for the probe to
	// fire once its rows are removed.
	f, drop := newBalFixture(t, d, 3, 4_493_900_000)
	defer drop()

	f.writeBal(t, d,
		[3]int64{1, 1, 1000},
		[3]int64{2, 1, 2000},
		[3]int64{3, 1, 0},
	)
	require.Equal(t, int64(2), f.holdersViaHistory(t, d))

	// Now behave like a build that writes balances but not the cache: insert
	// straight into balances, bypassing InsertBalancesBatch. Holder 3 acquires
	// a balance and holder 1's changes, at blocks ABOVE the cache's watermark,
	// which is what an advancing chain does during a rollback window.
	for _, r := range [][3]int64{{3, 5, 3000}, {1, 6, 4000}} {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO balances (address, token_address, block_number, balance)
			VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("0xbalholder%031d", r[0]), f.token, f.base+r[1], r[2])
		require.NoError(t, err)
	}
	// Plus drift the cache can only have acquired earlier: one row stale, one
	// gone. Both sit BELOW the watermark, so the probe cannot see them --
	// they are repaired anyway because the repair is total once it fires.
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
