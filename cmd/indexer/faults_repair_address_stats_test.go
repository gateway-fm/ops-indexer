//go:build faults

package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/internal/indexer"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

type repairFixture struct {
	url   string
	proxy *faultkit.Proxy
	d     *db.DB
}

// ingest indexes the fixture node's chain to head with the real indexer.
func ingest(t *testing.T, d *db.DB, node *faultkit.Node, head uint64, direct string) {
	t.Helper()
	client, err := rpc.New(node.URL())
	require.NoError(t, err)
	idx := indexer.NewWithConfig(d, client, 20*time.Millisecond, 0, &indexer.Config{RPCWorkers: 4, RPCRateLimit: 1000, DBBatchSize: 50,
		CatchupEnabled: true, CatchupWorkers: 4, CatchupBatchSize: 50, CatchupQueueSize: 100, TokenMetadataWorkers: 2, BalanceWorkers: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = idx.Start(ctx) }()
	admin := faultkit.Conn(t, direct)
	for {
		var n int64
		require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM blocks`).Scan(&n))
		if uint64(n) >= head+1 {
			break
		}
		require.NoError(t, ctx.Err(), "ingest stalled at %d blocks", n)
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
}

func newRepairFixture(t *testing.T) *repairFixture {
	t.Helper()
	url := faultkit.NewDB(t)
	proxy := faultkit.NewProxy(t, url)
	d, err := db.New(proxy.URL())
	require.NoError(t, err)
	t.Cleanup(d.Close)
	require.NoError(t, d.Migrate())
	node := faultkit.NewNode(t, 400)
	ingest(t, d, node, 400, url)
	f := &repairFixture{url: url, proxy: proxy, d: d}
	require.Zero(t, f.wrong(t), "ingest leaves address_stats right")
	f.breakStats(t)
	return f
}

// breakStats undercounts 3 addresses, double counts 2 and deletes 2 rows (addresses_total kept consistent).
func (f *repairFixture) breakStats(t *testing.T) {
	t.Helper()
	admin := faultkit.Conn(t, f.url)
	ctx := context.Background()
	_, err := admin.Exec(ctx, `
		WITH ranked AS (SELECT address, row_number() OVER (ORDER BY address) AS r FROM address_stats)
		UPDATE address_stats a SET tx_count = CASE WHEN r <= 3 THEN a.tx_count - 1 ELSE a.tx_count * 2 END,
		                            token_transfer_count = a.token_transfer_count + 1
		FROM ranked WHERE ranked.address = a.address AND r <= 5`)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, `
		WITH gone AS (DELETE FROM address_stats WHERE address IN (SELECT address FROM address_stats ORDER BY address DESC LIMIT 2) RETURNING 1)
		UPDATE chain_counters SET count = count - (SELECT count(*) FROM gone) WHERE name = 'addresses_total'`)
	require.NoError(t, err)
	require.EqualValues(t, 7, f.wrong(t))
}

// wrong is how many addresses a dry run would change (through a direct, fault-free connection).
func (f *repairFixture) wrong(t *testing.T) int64 {
	t.Helper()
	d, err := db.New(f.url)
	require.NoError(t, err)
	defer d.Close()
	s, err := d.StageAddressStatsRepair(context.Background(), true)
	require.NoError(t, err)
	return s.Changes
}

// consistent checks address_stats against the recount, addresses_total against its rows and that no stage is left.
func (f *repairFixture) consistent(t *testing.T, label string) {
	t.Helper()
	admin := faultkit.Conn(t, f.url)
	var total, rows int64
	var stage bool
	require.NoError(t, admin.QueryRow(context.Background(), `
		SELECT (SELECT count FROM chain_counters WHERE name = 'addresses_total'), (SELECT count(*) FROM address_stats),
		       to_regclass('address_stats_repair') IS NOT NULL OR to_regclass('address_stats_repair_total') IS NOT NULL`).Scan(&total, &rows, &stage))
	require.Zero(t, f.wrong(t), "%s: a dry run after recovery finds nothing wrong", label)
	require.Equal(t, rows, total, "%s: addresses_total counts the rows", label)
	require.False(t, stage, "%s: no stage left", label)
}

// repair runs the command on a fresh DB object through the proxy (a restart of the command).
func (f *repairFixture) repair(t *testing.T, args ...string) int {
	t.Helper()
	d, err := db.New(f.proxy.URL())
	require.NoError(t, err)
	defer d.Close()
	return runRepairAddressStats(args, d)
}

// One fault at one step of the repair, then a rerun: the first run is honest (exit 1 if it did not finish, 0 only
// if every correction is applied), and the rerun leaves address_stats equal to the recount.
func TestFaultRepairFaultAtEachStepThenRerun(t *testing.T) {
	steps := map[string]string{
		"stage":       "SELECT e.address, e.txs - COALESCE",
		"stage-total": "INSERT INTO address_stats_repair_total VALUES",
		"apply-check": "WHERE name = $1 FOR SHARE",
		"apply-batch": "DELETE FROM address_stats_repair r",
		"applied-sum": "UPDATE address_stats_repair_total SET applied",
		"final-lock":  "pg_advisory_xact_lock(4670",
		"final-drop":  "DROP TABLE address_stats_repair_total",
	}
	actions := map[string]faultkit.Action{"lost-ack": faultkit.LostAck, "fail-commit": faultkit.FailCommit, "kill-before": faultkit.KillBefore}
	for sname, match := range steps {
		for aname, action := range actions {
			t.Run(sname+"/"+aname, func(t *testing.T) {
				f := newRepairFixture(t)
				r := f.proxy.Arm(faultkit.Rule{Match: match, Action: action})
				code := f.repair(t, "--batch", "3")
				fired := f.proxy.Fired(r)
				f.proxy.Disarm()
				left := f.wrong(t)
				t.Logf("%s %s: fired=%d first run exit=%d, wrong after it=%d", sname, aname, fired, code, left)
				require.Equal(t, 1, fired, "the fault fired")
				if code == 0 {
					require.Zero(t, left, "exit 0 with corrections unapplied")
				}
				require.Equal(t, 0, f.repair(t, "--batch", "3"), "the rerun finishes")
				f.consistent(t, sname+"/"+aname)
			})
		}
	}
}

// The database goes away in the middle of the apply and comes back: the run stops with exit 1, a rerun finishes.
func TestFaultRepairDatabaseDownMidApply(t *testing.T) {
	f := newRepairFixture(t)
	r := f.proxy.Arm(faultkit.Rule{Match: "DELETE FROM address_stats_repair r", Action: faultkit.KillBefore, Times: 1})
	var once sync.Once
	go func() {
		for f.proxy.Fired(r) == 0 {
			time.Sleep(time.Millisecond)
		}
		once.Do(f.proxy.Down)
	}()
	code := f.repair(t, "--batch", "3")
	require.Equal(t, 1, code)
	f.proxy.Up()
	f.proxy.Disarm()
	require.Equal(t, 0, f.repair(t, "--batch", "3"))
	f.consistent(t, "db down")
}

// A long reader holding the xmin horizon and locks on address_stats and a history table during the whole repair.
func TestFaultRepairUnderALongReader(t *testing.T) {
	f := newRepairFixture(t)
	endStats := faultkit.LongReader(t, f.url, "address_stats")
	endTxs := faultkit.LongReader(t, f.url, "transactions")
	done := make(chan int, 1)
	go func() { done <- f.repair(t, "--batch", "3") }()
	select {
	case code := <-done:
		require.Equal(t, 0, code)
	case <-time.After(60 * time.Second):
		endStats()
		endTxs()
		t.Fatal("the repair waited on the readers")
	}
	endStats()
	endTxs()
	f.consistent(t, "long reader")
}

// Two runs at once.
func TestFaultRepairTwoRunsAtOnce(t *testing.T) {
	f := newRepairFixture(t)
	var wg sync.WaitGroup
	codes := make([]int, 2)
	for i := range codes {
		wg.Add(1)
		go func() { defer wg.Done(); codes[i] = f.repair(t, "--batch", "1") }()
	}
	wg.Wait()
	left := f.wrong(t)
	t.Logf("two runs: exit codes %v, wrong after both=%d", codes, left)
	if left > 0 {
		require.NotContains(t, codes, 0, "a run reported done with corrections unapplied")
	}
}

// --batch 0 and a negative batch.
func TestFaultRepairBatchBounds(t *testing.T) {
	for _, b := range []string{"0", "-1"} {
		t.Run(b, func(t *testing.T) {
			f := newRepairFixture(t)
			code := f.repair(t, "--batch", b)
			left := f.wrong(t)
			t.Logf("--batch %s: exit %d, wrong after=%d", b, code, left)
			if code == 0 {
				require.Zero(t, left, "exit 0 with corrections unapplied")
			}
			require.Equal(t, 0, f.repair(t, "--batch", "3"), "a rerun with a sane batch finishes")
			f.consistent(t, "batch "+b)
		})
	}
}

// The repair next to the live indexer, with a lost ack on one apply batch, then a rerun while ingest goes on: after
// ingest stops, address_stats equals the recount.
func TestFaultRepairNextToLiveIngestWithALostAck(t *testing.T) {
	url := faultkit.NewDB(t)
	proxy := faultkit.NewProxy(t, url)
	d, err := db.New(proxy.URL())
	require.NoError(t, err)
	defer d.Close()
	require.NoError(t, d.Migrate())
	node := faultkit.NewNode(t, 200)
	ingest(t, d, node, 200, url)
	f := &repairFixture{url: url, proxy: proxy, d: d}
	f.breakStats(t)

	node.SetHead(600)
	live := make(chan struct{})
	go func() { defer close(live); ingest(t, d, node, 600, url) }()
	r := proxy.Arm(faultkit.Rule{Match: "DELETE FROM address_stats_repair r", Action: faultkit.LostAck})
	code := f.repair(t, "--batch", "2")
	require.Equal(t, 1, proxy.Fired(r))
	proxy.Disarm()
	t.Logf("first run exit %d", code)
	require.Equal(t, 0, f.repair(t, "--batch", "2"))
	<-live
	f.consistent(t, "next to live ingest")
}

// A repair staged, then a reorg revert with one fault at its revert-count step or at the stale stage's removal, then
// the repair run and rerun: the revert either happened or did not, the stage is never applied across it, and
// address_stats ends equal to the recount.
func TestFaultRepairAcrossARevertThenRerun(t *testing.T) {
	steps := map[string]string{
		"revert-count": "count = chain_counters.count + 1",
		"stale-drop":   "DROP TABLE address_stats_repair; DROP TABLE IF EXISTS",
	}
	actions := map[string]faultkit.Action{"lost-ack": faultkit.LostAck, "fail-commit": faultkit.FailCommit, "kill-before": faultkit.KillBefore}
	for sname, match := range steps {
		for aname, action := range actions {
			t.Run(sname+"/"+aname, func(t *testing.T) {
				f := newRepairFixture(t)
				ctx := context.Background()
				_, err := f.d.StageAddressStatsRepair(ctx, false)
				require.NoError(t, err)
				r := f.proxy.Arm(faultkit.Rule{Match: match, Action: action})
				revertErr := f.d.DeleteBlock(ctx, 400)
				code := f.repair(t, "--batch", "3")
				fired := f.proxy.Fired(r)
				f.proxy.Disarm()
				t.Logf("%s %s: fired=%d revert err=%v first run exit=%d", sname, aname, fired, revertErr, code)
				require.Equal(t, 1, fired, "the fault fired")
				if code == 0 {
					require.Zero(t, f.wrong(t), "exit 0 with corrections unapplied")
				}
				require.Equal(t, 0, f.repair(t, "--batch", "3"), "the rerun finishes")
				f.consistent(t, sname+"/"+aname)
			})
		}
	}
}

// Reorgs that keep landing during a repair: every batch finds the stage stale. The run stages again 10 times, then
// stops with exit 1; once they stop, a rerun finishes.
func TestFaultRepairStopsWhenReorgsKeepMakingTheStageStale(t *testing.T) {
	f := newRepairFixture(t)
	admin := faultkit.Conn(t, f.url)
	ctx := context.Background()
	_, err := admin.Exec(ctx, `UPDATE address_stats SET token_transfer_count = token_transfer_count + 1`)
	require.NoError(t, err)
	require.Greater(t, f.wrong(t), int64(12))
	_, err = admin.Exec(ctx, `
		CREATE FUNCTION reorg_landed() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN UPDATE chain_counters SET count = count + 1 WHERE name = 'address_stats_reverts'; RETURN NULL; END $$;
		CREATE TRIGGER reorg_landed AFTER INSERT OR UPDATE ON address_stats EXECUTE FUNCTION reorg_landed()`)
	require.NoError(t, err)
	require.Equal(t, 1, f.repair(t, "--batch", "1"), "stopped after 10 restages")
	_, err = admin.Exec(ctx, `DROP TRIGGER reorg_landed ON address_stats`)
	require.NoError(t, err)
	require.Equal(t, 0, f.repair(t, "--batch", "1"))
	f.consistent(t, "reorgs stopped")
}

// The repair next to the live indexer while the node reorgs: the run finishes (staging again as it needs) and, after
// ingest stops, address_stats equals the recount.
func TestFaultRepairNextToLiveIngestThroughAReorg(t *testing.T) {
	url := faultkit.NewDB(t)
	proxy := faultkit.NewProxy(t, url)
	d, err := db.New(proxy.URL())
	require.NoError(t, err)
	defer d.Close()
	require.NoError(t, d.Migrate())
	node := faultkit.NewNode(t, 200)
	ingest(t, d, node, 200, url)
	f := &repairFixture{url: url, proxy: proxy, d: d}
	f.breakStats(t)
	admin := faultkit.Conn(t, url)
	_, err = admin.Exec(context.Background(), `UPDATE address_stats SET token_transfer_count = token_transfer_count + 1`)
	require.NoError(t, err)

	node.Fork(190, 1)
	node.SetHead(600)
	live := make(chan struct{})
	go func() { defer close(live); ingest(t, d, node, 600, url) }()
	code := f.repair(t, "--batch", "1")
	<-live
	var reverts int64
	require.NoError(t, admin.QueryRow(context.Background(), `
		SELECT COALESCE((SELECT count FROM chain_counters WHERE name = 'address_stats_reverts'), 0)`).Scan(&reverts))
	t.Logf("repair exit %d, reverts %d", code, reverts)
	require.Positive(t, reverts, "the node's fork was reverted")
	require.Equal(t, 0, code)
	f.consistent(t, "next to live ingest through a reorg")
}
