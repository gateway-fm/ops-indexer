//go:build faults

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// ingestDuring inserts blocks above the head every 200 ms while run runs, each bounded by 5 s; it returns the
// errors (a stalled insert is a deadline error).
func (f *fk) ingestDuring(run func()) []error {
	done := make(chan struct{})
	go func() { defer close(done); run() }()
	var errs []error
	next := uint64(f.n(`SELECT max(number) FROM blocks`)) + 1
	for {
		select {
		case <-done:
			return errs
		case <-time.After(200 * time.Millisecond):
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := f.d.InsertBlockDataBatch(ctx, &BlockData{Block: insertBlockAt(next)})
		cancel()
		if err != nil {
			errs = append(errs, err)
		} else {
			next++
		}
	}
}

func TestFaultReaderDuringPrune(t *testing.T) {
	for _, table := range []string{"logs", "logs_legacy", "transactions_p44001"} {
		for _, mode := range []string{"reader", "idle-in-transaction"} {
			table, mode := table, mode
			t.Run(table+"/"+mode, func(t *testing.T) {
				t.Parallel()
				f, cfg, before := pruneReady(t)
				var end func()
				if mode == "reader" {
					end = faultkit.LongReader(t, f.url, table)
				} else {
					c := faultkit.Conn(t, f.url)
					_, err := c.Exec(context.Background(), "BEGIN; SELECT 1 FROM "+table+" LIMIT 1")
					require.NoError(t, err)
					end = func() { _, _ = c.Exec(context.Background(), "ROLLBACK") }
				}
				var perr error
				errs := f.ingestDuring(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
					defer cancel()
					perr = f.d.retentionPass(ctx, cfg)
				})
				t.Logf("pass with the holder: %v; ingest errors: %v; state %v", perr, errs, partitionRanges(t, f.d))
				require.Empty(t, errs, "ingest is not held up")
				end()
				rerrs := f.recoverPasses(cfg, 4)
				t.Logf("recovery: %v", rerrs)
				require.Less(t, len(rerrs), 4)
				require.Empty(t, f.violations())
				require.Equal(t, int64(60_000), f.n(`SELECT pruned_through FROM prune_state`))
				_ = before
			})
		}
	}
}

func TestFaultReaderDuringCut(t *testing.T) {
	for _, mode := range []string{"reader", "idle-in-transaction"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f, cfg, before := cutReady(t)
			var end func()
			if mode == "reader" {
				end = faultkit.LongReader(t, f.url, "logs")
			} else {
				c := faultkit.Conn(t, f.url)
				_, err := c.Exec(context.Background(), "BEGIN; SELECT 1 FROM blocks LIMIT 1")
				require.NoError(t, err)
				end = func() { _, _ = c.Exec(context.Background(), "ROLLBACK") }
			}
			var perr error
			errs := f.ingestDuring(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer cancel()
				perr = f.d.retentionPass(ctx, cfg)
			})
			t.Logf("cut pass with the holder: %v; ingest errors %v; checks %d", perr, errs, historyCutChecks(t, f.d))
			require.Empty(t, errs, "ingest is not held up")
			end()
			rerrs := f.recoverPasses(cfg, 4)
			require.Less(t, len(rerrs), 4, "%v", rerrs)
			require.Empty(t, f.violations())
			cut, _ := f.d.HistoryCut(context.Background())
			require.True(t, cut)
			_ = before
		})
	}
}

// The cut asks the node for its head after taking the history-DDL gate; the RPC client has no response timeout.
func TestFaultNodeHeadHangsDuringTheCut(t *testing.T) {
	f, cfg, _ := cutReady(t)
	node := faultkit.NewNode(t, 40_001)
	node.AddHook(faultkit.MethodHook("eth_blockNumber", faultkit.Fault{Delay: 40 * time.Second}, nil))
	client, err := rpc.New(node.URL())
	require.NoError(t, err)
	cfg.NodeHead = client.BlockNumber
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	passDone := make(chan error, 1)
	go func() { passDone <- f.d.retentionPass(ctx, cfg) }()
	time.Sleep(3 * time.Second)

	var gateFree bool
	require.NoError(t, f.admin.QueryRow(context.Background(), `SELECT pg_try_advisory_lock_shared(4673, 1)`).Scan(&gateFree))
	if gateFree {
		_, _ = f.admin.Exec(context.Background(), `SELECT pg_advisory_unlock_shared(4673, 1)`)
	}
	lctx, lcancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, lerr := f.d.CloseLedgerSpans(lctx, testLedger, 1)
	lcancel()
	t.Logf("while eth_blockNumber hangs: gate free=%v, ledger close: %v, checks %d", gateFree, lerr, historyCutChecks(t, f.d))
	select {
	case err := <-passDone:
		t.Logf("pass returned early: %v", err)
	default:
		t.Log("the cut is still waiting on the node")
	}
	require.True(t, gateFree, "a hung node request holds the history-DDL gate, and the ledger closer waits behind it")
}
