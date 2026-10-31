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

var (
	traceTxA = common.HexToHash("0xa1")
	traceTxB = common.HexToHash("0xb2")
)

const traceFrameWithACall = `{"type":"CALL","from":"0x00000000000000000000000000000000000000aa","to":"0x00000000000000000000000000000000000000bb","calls":[{"type":"CALL","from":"0x00000000000000000000000000000000000000bb","to":"0x00000000000000000000000000000000000000cc"}]}`

// partialTraceServer traces only transaction A: per block (blockTraced) or per transaction, B's trace failing.
func partialTraceServer(t *testing.T, blockTraced bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []any           `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		switch {
		case req.Method == "debug_traceBlockByNumber" && blockTraced:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":[{"txHash":%q,"result":%s}]}`, req.ID, traceTxA.Hex(), traceFrameWithACall)
		case req.Method == "debug_traceBlockByNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"not available"}}`, req.ID)
		case req.Method == "debug_traceTransaction" && req.Params[0] == traceTxA.Hex():
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, traceFrameWithACall)
		default:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":"trace failed"}}`, req.ID)
		}
	}))
}

// A block whose traces come back for some of its transactions only: the lenient fetch leaves the others' internal
// calls out, as ingest does; the strict fetch, for counts that replace or extend the ledger, fails.
func TestAStrictTraceFetchFailsOnAMissingOrFailedTransactionTrace(t *testing.T) {
	for _, blockTraced := range []bool{true, false} {
		name := "per transaction"
		if blockTraced {
			name = "per block"
		}
		t.Run(name, func(t *testing.T) {
			srv := partialTraceServer(t, blockTraced)
			defer srv.Close()
			c, err := New(srv.URL)
			require.NoError(t, err)
			ctx := context.Background()
			txs := []common.Hash{traceTxA, traceTxB}

			lenient, err := c.FetchTracesBatch(ctx, txs, 7, 1700000000, 2, 100)
			require.NoError(t, err)
			require.Len(t, lenient, 1, "A's internal call, B's left out")

			_, err = c.FetchTracesBatchStrict(ctx, txs, 7, 1700000000, 2, 100)
			require.Error(t, err)
		})
	}
}

// emptyTraceServer answers transaction B's trace with result (null or {}), per block or per transaction; A's is whole.
func emptyTraceServer(t *testing.T, blockTraced bool, result string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []any           `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		switch {
		case req.Method == "debug_traceBlockByNumber" && blockTraced:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":[{"txHash":%q,"result":%s},{"txHash":%q,"result":%s}]}`,
				req.ID, traceTxA.Hex(), traceFrameWithACall, traceTxB.Hex(), result)
		case req.Method == "debug_traceBlockByNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"not available"}}`, req.ID)
		case req.Params[0] == traceTxA.Hex():
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, traceFrameWithACall)
		default:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, result)
		}
	}))
}

// A trace that comes back null or as an empty frame is no trace: the lenient fetch leaves the transaction's internal
// calls out, the strict fetch fails, and a single transaction's trace is an error.
func TestANullOrEmptyTraceIsNoTrace(t *testing.T) {
	for _, result := range []string{"null", "{}"} {
		for _, blockTraced := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/block=%v", result, blockTraced), func(t *testing.T) {
				srv := emptyTraceServer(t, blockTraced, result)
				defer srv.Close()
				c, err := New(srv.URL)
				require.NoError(t, err)
				ctx := context.Background()
				txs := []common.Hash{traceTxA, traceTxB}

				lenient, err := c.FetchTracesBatch(ctx, txs, 7, 1700000000, 2, 100)
				require.NoError(t, err)
				require.Len(t, lenient, 1, "A's internal call, B's left out")

				_, err = c.FetchTracesBatchStrict(ctx, txs, 7, 1700000000, 2, 100)
				require.Error(t, err, "B has no trace")

				_, err = c.TraceTransaction(ctx, traceTxB)
				require.Error(t, err, "B's trace is no trace")
			})
		}
	}
}
