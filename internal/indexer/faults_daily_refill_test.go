//go:build faults

package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// fkRefillDB is a database whose days 0..1 (blocks 0..287) were lost: blocks 288..1000 stored and aggregated, with
// the watermark at 1000, as the refill finds it after a prune. Days are 144 blocks of the node's chain.
func fkRefillDB(t *testing.T) (*db.DB, *faultkit.Proxy, string) {
	t.Helper()
	url := faultkit.NewDB(t)
	p := faultkit.NewProxy(t, url)
	d, err := db.New(p.URL())
	require.NoError(t, err)
	t.Cleanup(d.Close)
	require.NoError(t, d.Migrate())
	admin := faultkit.Conn(t, url)
	ctx := context.Background()
	_, err = admin.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, size)
		SELECT n, 'h' || n, 'h' || (n - 1), 1767225600 + n * 600, 50000, 30000000, 1, 768 FROM generate_series(288, 1000) n;
		INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, 287)`)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err := d.AdvanceDailyStats(ctx)
		require.NoError(t, err)
	}
	return d, p, url
}

var fkDays = []time.Time{time.Unix(1767225600, 0).UTC(), time.Unix(1767225600+86400, 0).UTC()}

func fkRefillOpts() DailyRefillOptions {
	return DailyRefillOptions{Bodies: true, Receipts: true, BatchesPerSecond: 1e6, Batch: 50}
}

func fkDaily(t *testing.T, url string) string {
	t.Helper()
	c := faultkit.Conn(t, url)
	var out []string
	for _, q := range []string{`SELECT * FROM daily_stats`, `SELECT * FROM daily_stats_acc`, `SELECT * FROM daily_stats_watermark`} {
		rows, err := c.Query(context.Background(), `SELECT to_jsonb(q)::text FROM (`+q+`) q ORDER BY 1`)
		require.NoError(t, err)
		r, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)
		out = append(out, r...)
	}
	return strings.Join(out, "\n")
}

func fkRefillRef(t *testing.T, node *faultkit.Node) string {
	t.Helper()
	d, _, url := fkRefillDB(t)
	_, err := RefillDailyStats(context.Background(), d, RefillNode{C: rpclient.Dial(node.URL())}, fkDays, fkRefillOpts())
	require.NoError(t, err)
	return fkDaily(t, url)
}

func fkRefill(d *db.DB, node *faultkit.Node) error {
	_, err := RefillDailyStats(context.Background(), d, RefillNode{C: rpclient.Dial(node.URL())}, fkDays, fkRefillOpts())
	return err
}

// A fault in the database while the refill writes; the operator reruns the command.
func TestFaultRefillDBFaults(t *testing.T) {
	node := faultkit.NewNode(t, 1200)
	want := fkRefillRef(t, node)
	require.Contains(t, want, `"date": "2026-01-01"`)
	for _, c := range []struct {
		name   string
		action faultkit.Action
		match  string
	}{
		{"lost-ack-final-write", faultkit.LostAck, "create temp table refill_days"},
		{"fail-commit-final-write", faultkit.FailCommit, "create temp table refill_days"},
		{"kill-in-final-write", faultkit.KillBefore, "refill_ends"},
		{"lost-ack-checkpoint", faultkit.LostAck, "insert into daily_stats_refill"},
		{"kill-before-checkpoint", faultkit.KillBefore, "insert into daily_stats_refill"},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, p, url := fkRefillDB(t)
			r := p.Arm(faultkit.Rule{Match: c.match, Action: c.action})
			require.Error(t, fkRefill(d, node), "the run reports the fault")
			require.Equal(t, 1, p.Fired(r))
			again, err := db.New(url)
			require.NoError(t, err)
			defer again.Close()
			require.NoError(t, fkRefill(again, node), "the rerun")
			require.Equal(t, want, fkDaily(t, url))
		})
	}
}

// Faults at the node mid-refill: the run fails, or retries, and never writes a wrong day.
func TestFaultRefillNodeFaults(t *testing.T) {
	ref := faultkit.NewNode(t, 1200)
	want := fkRefillRef(t, ref)
	for _, c := range []struct {
		name    string
		hook    faultkit.Hook
		lag     *uint64
		wantErr bool
	}{
		{"rpc-error-3rd-batch", faultkit.MethodHook("eth_getBlockByNumber", faultkit.Fault{Err: "boom"}, func(n uint64, call int) bool { return n == 60 }), nil, true},
		{"429-twice", fkFirstCalls("eth_getBlockByNumber", 2, faultkit.Fault{HTTP: 429}), nil, false},
		{"503-always", faultkit.MethodHook("eth_getBlockReceipts", faultkit.Fault{HTTP: 503}, nil), nil, true},
		{"null-block-in-batch", faultkit.MethodHook("eth_getBlockByNumber", faultkit.Fault{Null: true}, func(n uint64, _ int) bool { return n == 77 }), nil, true},
		{"null-header-only", fkHeaderNull(200), nil, true},
		{"lagging-node", nil, ptr(uint64(150)), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			node := faultkit.NewNode(t, 1200)
			if c.hook != nil {
				node.AddHook(c.hook)
			}
			node.LagAbove(c.lag)
			d, _, url := fkRefillDB(t)
			r := &dailyRefill{}
			_ = r
			err := fkRefillFast(d, node)
			if c.wantErr {
				require.Error(t, err, "a run against a faulty node fails")
				t.Logf("error: %v", err)
				node.ClearHooks()
				node.LagAbove(nil)
				require.NoError(t, fkRefill(d, node), "the rerun once the node is healthy")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, want, fkDaily(t, url), "the days equal a refill from a healthy node")
		})
	}
}

func ptr[T any](v T) *T { return &v }

func fkFirstCalls(method string, k int, f faultkit.Fault) faultkit.Hook {
	n := 0
	return func(m string, _ []json.RawMessage, _ int) *faultkit.Fault {
		if m != method {
			return nil
		}
		n++
		if n <= k {
			ff := f
			return &ff
		}
		return nil
	}
}

// fkHeaderNull answers null for header-only lookups of block h (a load-balanced node whose header path lags).
func fkHeaderNull(h uint64) faultkit.Hook {
	return func(m string, params []json.RawMessage, _ int) *faultkit.Fault {
		if m != "eth_getBlockByNumber" || len(params) < 2 || string(params[1]) != "false" {
			return nil
		}
		var s string
		_ = json.Unmarshal(params[0], &s)
		var v uint64
		fmt.Sscanf(s, "0x%x", &v)
		if v >= 100 && v < 300 {
			return &faultkit.Fault{Null: true}
		}
		return nil
	}
}

// fkRefillFast is fkRefill with short retry pauses.
func fkRefillFast(d *db.DB, node *faultkit.Node) error {
	return fkRefill(d, node)
}

// A node that accepts a request and does not answer for 3 min: the refill stops with an error at the RPC timeout
// (60 s by default), on its own, before the operator's deadline.
func TestFaultRefillStalledNode(t *testing.T) {
	node := faultkit.NewNode(t, 1200)
	node.AddHook(faultkit.MethodHook("eth_getBlockByNumber", faultkit.Fault{Delay: 3 * time.Minute}, func(n uint64, _ int) bool { return n == 60 }))
	d, _, _ := fkRefillDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	start := time.Now()
	_, err := RefillDailyStats(ctx, d, RefillNode{C: rpclient.Dial(node.URL())}, fkDays, fkRefillOpts())
	t.Logf("returned after %s: %v", time.Since(start).Round(time.Second), err)
	require.Error(t, err)
	require.Less(t, time.Since(start), 90*time.Second, "the stalled request times out on its own, before the operator's deadline")
}

// A long reader on the history tables during the final write.
func TestFaultRefillUnderALongReader(t *testing.T) {
	node := faultkit.NewNode(t, 1200)
	want := fkRefillRef(t, node)
	d, _, url := fkRefillDB(t)
	end := faultkit.LongReader(t, url, "transactions")
	defer end()
	done := make(chan error, 1)
	go func() { done <- fkRefill(d, node) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("the refill waited on the reader")
	}
	require.Equal(t, want, fkDaily(t, url))
}

// A node that answers null for one block's receipts: whatever the first run does with that day, a rerun against a
// healthy node leaves every day equal to a clean refill; no checkpoint keeps a count made without all receipts.
func TestFaultRefillNullReceiptsStickInTheCheckpoint(t *testing.T) {
	want := fkRefillRef(t, faultkit.NewNode(t, 1200))
	node := faultkit.NewNode(t, 1200)
	node.AddHook(faultkit.MethodHook("eth_getBlockReceipts", faultkit.Fault{Null: true}, func(n uint64, _ int) bool { return n == 77 }))
	d, _, url := fkRefillDB(t)
	t.Logf("the run with a null receipts element: %v", fkRefill(d, node))
	node.ClearHooks()
	require.NoError(t, fkRefill(d, node), "the rerun against a healthy node")
	require.Equal(t, want, fkDaily(t, url), "the rerun repairs the day")
}

// A dry run writes nothing to daily_stats, also when its connection dies mid-write; the real run after it equals
// a plain refill, and --recompute-only after a lost ack gives the same days.
func TestFaultRefillDryRunThenRealAndRecompute(t *testing.T) {
	node := faultkit.NewNode(t, 1200)
	want := fkRefillRef(t, node)
	d, p, url := fkRefillDB(t)
	before := fkDaily(t, url)
	opts := fkRefillOpts()
	opts.Dry = true
	p.Arm(faultkit.Rule{Match: "refill_ends", Action: faultkit.KillBefore})
	_, err := RefillDailyStats(context.Background(), d, RefillNode{C: rpclient.Dial(node.URL())}, fkDays, opts)
	require.Error(t, err)
	_, err = RefillDailyStats(context.Background(), d, RefillNode{C: rpclient.Dial(node.URL())}, fkDays, opts)
	require.NoError(t, err)
	require.Equal(t, before, fkDaily(t, url), "a dry run writes nothing")
	require.NoError(t, fkRefill(d, node))
	require.Equal(t, want, fkDaily(t, url))
	p.Arm(faultkit.Rule{Match: "create temp table refill_days", Action: faultkit.LostAck})
	_, err = RecomputeDailyStats(context.Background(), d, false)
	require.Error(t, err)
	_, err = RecomputeDailyStats(context.Background(), d, false)
	require.NoError(t, err)
	require.Equal(t, want, fkDaily(t, url))
}
