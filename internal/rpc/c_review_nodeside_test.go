package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// A node's block trace reports a transaction whose own trace failed (geth's tracer timeout, for one) as an entry with
// an error and no result. The strict fetch must fail the block on it, as it does when the per-transaction trace of
// the same transaction fails, instead of counting that transaction with no internal calls.
func TestAStrictTraceFetchFailsOnAFailedTransactionInTheBlockTrace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "debug_traceBlockByNumber" {
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":[{"txHash":%q,"result":%s},{"txHash":%q,"error":"execution timeout"}]}`,
				req.ID, traceTxA.Hex(), traceFrameWithACall, traceTxB.Hex())
			return
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":"trace failed"}}`, req.ID)
	}))
	defer srv.Close()
	c, err := New(srv.URL)
	require.NoError(t, err)

	got, err := c.FetchTracesBatchStrict(context.Background(), []common.Hash{traceTxA, traceTxB}, 7, 1700000000, 2, 100)
	require.Error(t, err, "B's trace failed; the strict fetch returned %d internal calls as if B made none", len(got))
}
