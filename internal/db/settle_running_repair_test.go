package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// firstSpan is repairFixture's first closed span and its totals as the node counts them.
func firstSpan(t *testing.T, d *DB) (lo, hi int64, truth *SpanTotals) {
	t.Helper()
	require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT lo, hi FROM ledger_spans ORDER BY lo LIMIT 1`).
		Scan(&lo, &hi))
	truth = NewSpanTotals()
	for n := lo; n < hi; n++ {
		require.NoError(t, truth.AddBlock(rebaseBlock(int(n))))
	}
	return lo, hi, truth
}

// quarantined is whether the running check quarantined key.
func quarantined(t *testing.T, d *DB, family, key string) bool {
	t.Helper()
	var q bool
	require.NoError(t, d.pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM ledger_quarantine WHERE family = $1 AND key = $2)`, family, key).Scan(&q))
	return q
}

// runningRepairs is how many times the running check repaired key's counter.
func runningRepairs(t *testing.T, d *DB, family, key, counter string) int {
	t.Helper()
	var n int
	require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT count(*) FROM ledger_repairs
		WHERE lo IS NULL AND family = $1 AND key = $2 AND counter = $3`, family, key, counter).Scan(&n))
	return n
}

// settlements are the two ways a closed span's ledger rows are settled to their truth: the node recount, with totals
// counted from the node, and the retained recount, which counts the span's stored rows.
var settlements = []struct {
	name   string
	settle func(ctx context.Context, d *DB, lo, hi int64, truth *SpanTotals) error
}{
	{"node recount", func(ctx context.Context, d *DB, lo, hi int64, truth *SpanTotals) error {
		c, err := d.SettleNodeSpan(ctx, lo, hi, truth, true)
		if err == nil && c.State != "repaired" {
			err = fmt.Errorf("span [%d, %d) settled %q, not repaired", lo, hi, c.State)
		}
		return err
	}},
	{"retained recount", func(ctx context.Context, d *DB, lo, hi int64, _ *SpanTotals) error {
		c, ok, err := d.RecountRetainedSpan(ctx, true, "")
		if err == nil && (!ok || c.Lo != lo || c.State != "repaired") {
			err = fmt.Errorf("recounted span %d (%v) settled %q, not span %d repaired", c.Lo, ok, c.State, lo)
		}
		return err
	}},
}

// The running check repairs a right address against a corrupt closed ledger, then a recount settles the ledger back
// to the truth: the next running check repairs the address again, to the settled truth, instead of quarantining it as
// drifting again after a repair. The truth moved; the address did not drift. The settlement records the address for
// the next span rebase, as a revert does.
func TestARunningRepairAgainstACorruptLedgerConvergesOnceTheLedgerIsSettled(t *testing.T) {
	for _, s := range settlements {
		t.Run(s.name, func(t *testing.T) {
			d, ctx := repairFixture(t)
			repairUntilDone(t, d)
			lo, hi, truth := firstSpan(t, d)
			_, err := d.pool.Exec(ctx, `UPDATE ledger_address SET txs = txs + 8 WHERE lo = $1 AND address = $2`, lo, rebaseHot)
			require.NoError(t, err)
			_, err = d.CheckRunning(ctx, rebaseHotBucket(t), true)
			require.NoError(t, err)
			require.Equal(t, int64(510), addressSeen(t, d, rebaseHot).txs, "repaired to the corrupt ledger's count")

			require.NoError(t, s.settle(ctx, d, lo, hi, truth))
			var recorded bool
			require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ledger_reverted_address WHERE address = $1)`,
				rebaseHot).Scan(&recorded))
			require.True(t, recorded, "the settlement records the address for the next span rebase")
			_, err = d.CheckRunning(ctx, rebaseHotBucket(t), true)
			require.NoError(t, err)

			require.False(t, quarantined(t, d, "address", rebaseHot), "the address is not quarantined")
			requireAddressStatsIsExactlyTheTruth(t, d)
			requireAddressStatsIsTheTruth(t, d)
			require.Equal(t, 2, runningRepairs(t, d, "address", rebaseHot, "txs"), "two repairs, each to the truth of its time")
		})
	}
}

// A recount settles the ledger while the running check's repair of the address is under way: after the batch read
// the corrupt ledger's count, or after it also took its rows and read the reverts since. The repair never writes the
// count it read from the corrupt ledger over the settled one: the settlement waits for it and lands after it. The next
// running check repairs the address to the truth without quarantining it.
func TestARunningRepairOverlappingTheLedgersSettlementConverges(t *testing.T) {
	for _, s := range settlements {
		for _, at := range []string{"read", "locked"} {
			t.Run(s.name+", settled once the batch has "+at, func(t *testing.T) {
				d, ctx := repairFixture(t)
				repairUntilDone(t, d)
				lo, hi, truth := firstSpan(t, d)
				_, err := d.pool.Exec(ctx, `UPDATE ledger_address SET txs = txs + 8 WHERE lo = $1 AND address = $2`, lo, rebaseHot)
				require.NoError(t, err)
				_, err = d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 1 WHERE address = $1`, rebaseHot)
				require.NoError(t, err)

				settled := make(chan error, 1)
				var once sync.Once
				settle := func() {
					once.Do(func() {
						go func() { settled <- s.settle(ctx, d, lo, hi, truth) }()
						select {
						case err := <-settled:
							settled <- err
						case <-time.After(2 * time.Second):
						}
					})
				}
				if at == "read" {
					rebaseReadHook = func([]string) { settle() }
				} else {
					rebaseLockedHook = settle
				}
				t.Cleanup(func() { rebaseReadHook, rebaseLockedHook = nil, nil })
				_, err = d.CheckRunning(ctx, rebaseHotBucket(t), true)
				require.NoError(t, err)
				rebaseReadHook, rebaseLockedHook = nil, nil
				require.NoError(t, <-settled)
				_, err = d.CheckRunning(ctx, rebaseHotBucket(t), true)
				require.NoError(t, err)

				require.False(t, quarantined(t, d, "address", rebaseHot), "the address is not quarantined")
				requireAddressStatsIsExactlyTheTruth(t, d)
				requireAddressStatsIsTheTruth(t, d)
			})
		}
	}
}

// The same for a chain counter, which the running check repairs by itself rather than through the rebase.
func TestAChainCounterRepairedAgainstACorruptLedgerConvergesOnceTheLedgerIsSettled(t *testing.T) {
	d, ctx := repairFixture(t)
	repairUntilDone(t, d)
	lo, hi, truth := firstSpan(t, d)
	counter := func() (n int64) {
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT count FROM chain_counters WHERE name = 'transactions_total'`).Scan(&n))
		return n
	}
	right := counter()
	_, err := d.pool.Exec(ctx, `UPDATE ledger_chain SET value = value + 8 WHERE lo = $1 AND name = 'transactions_total'`, lo)
	require.NoError(t, err)
	_, err = d.CheckRunning(ctx, 0, true)
	require.NoError(t, err)
	require.Equal(t, right+8, counter(), "repaired to the corrupt ledger's count")

	_, err = d.SettleNodeSpan(ctx, lo, hi, truth, true)
	require.NoError(t, err)
	_, err = d.CheckRunning(ctx, 0, true)
	require.NoError(t, err)

	require.False(t, quarantined(t, d, "chain", "transactions_total"), "the counter is not quarantined")
	require.Equal(t, right, counter(), "repaired again, to the settled truth")
	require.Equal(t, 2, runningRepairs(t, d, "chain", "transactions_total", "count"))
}

// rebaseLockWaiters is how many sessions wait for the rebase lock.
func rebaseLockWaiters(t *testing.T, d *DB) int {
	t.Helper()
	var waiting int
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pg_locks l JOIN pg_database db ON db.oid = l.database
		WHERE l.locktype = 'advisory' AND NOT l.granted AND db.datname = current_database()
		  AND l.classid = $1 AND l.objid = 1 AND l.objsubid = 2`, rebaseLock).Scan(&waiting))
	return waiting
}

// A settlement takes the rebase lock after compaction's lock (the node recount) and before any span row; a span
// rebase holds it on its connection while it marks spans rebased, and a repair batch for its transaction. In either
// order the second waits for the first and both finish: each settlement starting while a span rebase holds the lock,
// and a span close and a repair batch starting while a settlement holds its locks and its span's row.
func TestASettlementAndARebaseDoNotDeadlockInEitherOrder(t *testing.T) {
	finishes := func(t *testing.T, done <-chan error, what string) {
		t.Helper()
		select {
		case err := <-done:
			require.NoError(t, err, what)
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not finish: deadlock", what)
		}
	}
	for _, s := range settlements {
		t.Run(s.name+" during a span rebase", func(t *testing.T) {
			d, ctx := repairFixture(t)
			repairUntilDone(t, d)
			lo, hi, truth := firstSpan(t, d)
			_, err := d.pool.Exec(ctx, `UPDATE ledger_address SET txs = txs + 8 WHERE lo = $1 AND address = $2`, lo, rebaseHot)
			require.NoError(t, err)
			settled := make(chan error, 1)
			var once sync.Once
			spanRebaseStagedHook = func(int64) {
				once.Do(func() {
					go func() { settled <- s.settle(ctx, d, lo, hi, truth) }()
					require.Eventually(t, func() bool { return rebaseLockWaiters(t, d) == 1 }, 10*time.Second,
						20*time.Millisecond, "the settlement waits for the span rebase")
				})
			}
			t.Cleanup(func() { spanRebaseStagedHook = nil })
			closed, err := d.CloseLedgerSpans(ctx, repairLedger, 1)
			require.NoError(t, err)
			require.Equal(t, 1, closed)
			spanRebaseStagedHook = nil
			finishes(t, settled, "the settlement")
		})
	}
	for _, held := range []struct {
		name  string
		locks []string
	}{
		{"node recount", []string{`SELECT pg_advisory_xact_lock(4672, 1)`, `SELECT pg_advisory_xact_lock(4674, 1)`}},
		{"retained recount", []string{`SELECT pg_advisory_xact_lock(4674, 1)`}},
	} {
		t.Run(held.name+" holding its locks", func(t *testing.T) {
			d, ctx := repairFixture(t)
			repairUntilDone(t, d)
			lo, _, _ := firstSpan(t, d)
			_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 1 WHERE address = $1`, rebaseHot)
			require.NoError(t, err)
			settlement, err := d.pool.Begin(ctx)
			require.NoError(t, err)
			defer settlement.Rollback(context.Background())
			for _, l := range held.locks {
				_, err = settlement.Exec(ctx, l)
				require.NoError(t, err)
			}
			_, err = settlement.Exec(ctx, `SELECT 1 FROM ledger_spans WHERE lo = $1 FOR UPDATE`, lo)
			require.NoError(t, err)
			closing, repairing := make(chan error, 1), make(chan error, 1)
			go func() {
				_, err := d.CloseLedgerSpans(ctx, repairLedger, 1)
				closing <- err
			}()
			go func() {
				_, err := d.RepairAddressBucket(ctx, rebaseHotBucket(t), AddressRepairBatch)
				repairing <- err
			}()
			require.Eventually(t, func() bool { return rebaseLockWaiters(t, d) >= 1 }, 10*time.Second, 20*time.Millisecond,
				"the rebases wait for the settlement")
			require.NoError(t, settlement.Commit(ctx))
			finishes(t, closing, "the span close")
			finishes(t, repairing, "the repair")
			requireAddressStatsIsExactlyTheTruth(t, d)
		})
	}
}
