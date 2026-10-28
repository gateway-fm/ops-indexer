package db

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// slowNode answers every JSON-RPC request after delay, or not at all if the caller gives up first.
func slowNode(t *testing.T, delay time.Duration) *rpc.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(delay):
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	t.Cleanup(srv.Close)
	client, err := rpc.New(srv.URL)
	require.NoError(t, err)
	return client
}

// The cut reads the node's head to place itself above both heads. A node that does not answer must not hold the
// history-DDL gate: the head is read before the gate, under a short timeout, and the cut falls back to the stored
// head; meanwhile the ledger closer takes the gate and closes a span.
func TestACutWithAHungNodeHeadLeavesTheGateFree(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000, NodeHead: slowNode(t, 40*time.Second).BlockNumber}

	passDone := make(chan error, 1)
	go func() { passDone <- d.retentionPass(ctx, cfg) }()
	time.Sleep(2 * time.Second)

	var gateFree bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT pg_try_advisory_lock_shared($1, 1)`, historyDDLGate).Scan(&gateFree))
	if gateFree {
		_, _ = d.pool.Exec(ctx, `SELECT pg_advisory_unlock_shared($1, 1)`, historyDDLGate)
	}
	require.True(t, gateFree, "the history-DDL gate is free while the node head is read")
	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	closed, err := d.CloseLedgerSpans(closeCtx, testLedger, 1)
	cancel()
	require.NoError(t, err)
	require.Equal(t, 1, closed, "the ledger closes a span meanwhile")

	select {
	case err := <-passDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the retention pass still waits on the node")
	}
	cut, err := d.HistoryCut(ctx)
	require.NoError(t, err)
	require.True(t, cut, "the cut proceeds with the stored head")
	var cutAt int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT hi FROM history_partitions WHERE suffix = 'legacy'`).Scan(&cutAt))
	require.Equal(t, int64(40_001+2*2_000), cutAt, "placed from the stored head")
}

// A span close waits a bounded time for the history-DDL gate and gives up with an error the closer logs, so a gate
// held for good is visible and the close is retried next interval.
func TestASpanCloseGivesUpOnAHeldGate(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	release, err := d.holdHistoryDDLGate(ctx, time.Second)
	require.NoError(t, err)
	defer release()

	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	start := time.Now()
	ledger := testLedger
	ledger.GateWait = time.Second
	closed, err := d.CloseLedgerSpans(closeCtx, ledger, 1)
	require.Zero(t, closed)
	require.True(t, errors.Is(err, errLedgerGateBusy), "the close gives up on the gate: %v", err)
	require.Less(t, time.Since(start), 5*time.Second)
}
