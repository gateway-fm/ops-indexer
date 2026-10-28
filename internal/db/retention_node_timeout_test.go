package db

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// The cut asks the node for its head while it holds the history-DDL gate. A node that accepts the request and never
// answers costs the cut one timeout: it places the cut above the stored head instead, finishes, and gives the gate
// back to the ledger closer, instead of holding it for as long as the connection stays open.
func TestCutGoesOnWhenTheNodeHeadRequestIsNeverAnswered(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	node, err := rpc.NewWithTimeout(srv.URL, 300*time.Millisecond)
	require.NoError(t, err)

	cut := make(chan error, 1)
	go func() {
		cut <- d.cutHistory(ctx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000, NodeHead: node.BlockNumber})
	}()
	select {
	case err := <-cut:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the cut is still waiting on the node")
	}
	done, err := d.HistoryCut(ctx)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, []string{"legacy:44001:attached", "p44001:46001:attached"}, partitionRanges(t, d),
		"cut at the stored head + 2 partitions")

	gctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = d.CloseLedgerSpans(gctx, testLedger, 1)
	require.NoError(t, err, "the ledger closer gets the gate")
}
