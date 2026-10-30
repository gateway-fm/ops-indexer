//go:build faults

package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

func faultBackfillFixture(t *testing.T) (*db.DB, *faultkit.Proxy, *faultkit.Node, string, []Creation) {
	t.Helper()
	url := faultkit.NewDB(t)
	proxy := faultkit.NewProxy(t, url)
	d, err := db.New(proxy.URL())
	require.NoError(t, err)
	t.Cleanup(d.Close)
	require.NoError(t, d.Migrate())
	node := faultkit.NewNode(t, 50)
	var listed []Creation
	for b := uint64(10); b < 15; b++ {
		listed = append(listed, Creation{Address: faultkit.Addr(500 + b), TxHash: faultkit.TxHash(0, b), Block: b, Creator: faultkit.Addr(1)})
	}
	return d, proxy, node, url, listed
}

func codeHook() faultkit.Hook {
	return faultkit.MethodHook("eth_getCode", faultkit.Fault{Result: "0x6080"}, nil)
}

func contractsStored(t *testing.T, url string) int64 {
	var n int64
	require.NoError(t, faultkit.Conn(t, url).QueryRow(context.Background(), `SELECT count(*) FROM contracts`).Scan(&n))
	return n
}

// An RPC error on one listed transaction fails that one (exit 2 in the command), the rest are written; a rerun
// writes the one left and skips the others.
func TestFaultBackfillRPCErrorThenRerun(t *testing.T) {
	d, _, node, url, listed := faultBackfillFixture(t)
	node.AddHook(codeHook())
	node.AddHook(faultkit.MethodHook("eth_getTransactionReceipt", faultkit.Fault{Err: "upstream timeout"}, func(_ uint64, call int) bool { return call == 2 }))
	client, err := rpc.New(node.URL())
	require.NoError(t, err)
	report, err := BackfillContracts(context.Background(), d, client, listed, ContractsBackfillOptions{PerSecond: 1000, WithoutTraces: true})
	require.NoError(t, err)
	require.Len(t, report.Failed, 1)
	require.Equal(t, 4, report.Written)
	node.ClearHooks()
	node.AddHook(codeHook())
	report, err = BackfillContracts(context.Background(), d, client, listed, ContractsBackfillOptions{PerSecond: 1000, WithoutTraces: true})
	require.NoError(t, err)
	require.Empty(t, report.Failed)
	require.Equal(t, 1, report.Written)
	require.Equal(t, 4, report.TxsSkipped)
	require.EqualValues(t, 5, contractsStored(t, url))
}

// A lagging node answers eth_getCode "0x" for a contract it has not caught up to: the row is not written, the run
// reports it NotCreated and finishes without an error.
func TestFaultBackfillLaggingNodeEmptyCode(t *testing.T) {
	d, _, node, url, listed := faultBackfillFixture(t)
	client, err := rpc.New(node.URL())
	require.NoError(t, err)
	report, err := BackfillContracts(context.Background(), d, client, listed, ContractsBackfillOptions{PerSecond: 1000, WithoutTraces: true})
	require.NoError(t, err)
	require.Zero(t, report.Written)
	require.Len(t, report.NotCreated, len(listed), "every listed contract is reported, none is written without its code")
	require.Zero(t, contractsStored(t, url))
}

// A lost ack on a contract insert: the run stops with an error; the rerun finds the row and writes the rest.
func TestFaultBackfillLostAckOnInsertThenRerun(t *testing.T) {
	d, proxy, node, url, listed := faultBackfillFixture(t)
	node.AddHook(codeHook())
	client, err := rpc.New(node.URL())
	require.NoError(t, err)
	r := proxy.Arm(faultkit.Rule{Match: "INSERT INTO contracts (address, bytecode", Action: faultkit.LostAck})
	_, err = BackfillContracts(context.Background(), d, client, listed, ContractsBackfillOptions{PerSecond: 1000, WithoutTraces: true})
	require.Equal(t, 1, proxy.Fired(r))
	require.Error(t, err, "the lost ack stops the run")
	proxy.Disarm()
	report, err := BackfillContracts(context.Background(), d, client, listed, ContractsBackfillOptions{PerSecond: 1000, WithoutTraces: true})
	require.NoError(t, err)
	require.Empty(t, report.Failed)
	require.EqualValues(t, 5, contractsStored(t, url))
}

// A node that accepts a request and does not answer for 3 min: the backfill gives the call up at the RPC timeout
// (60 s by default) and reports the transaction failed for a rerun, instead of waiting for it.
func TestFaultBackfillNodeNeverAnswers(t *testing.T) {
	d, _, node, _, listed := faultBackfillFixture(t)
	node.AddHook(faultkit.MethodHook("eth_getTransactionReceipt", faultkit.Fault{Delay: 3 * time.Minute}, nil))
	client, err := rpc.New(node.URL())
	require.NoError(t, err)
	type result struct {
		report *ContractsBackfillReport
		err    error
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		report, err := BackfillContracts(context.Background(), d, client, listed[:1], ContractsBackfillOptions{PerSecond: 1000, WithoutTraces: true})
		done <- result{report, err}
	}()
	select {
	case r := <-done:
		t.Logf("returned after %s: %v", time.Since(start).Round(time.Second), r.err)
		require.True(t, r.err != nil || (r.report != nil && len(r.report.Failed) == 1), "the unanswered call is reported")
	case <-time.After(90 * time.Second):
		t.Fatalf("still waiting on the node after %s", time.Since(start).Round(time.Second))
	}
}
