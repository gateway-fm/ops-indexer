package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	explorerTypes "github.com/gateway-fm/chain-indexer/internal/types"
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

// A node that traces but lacks an old block's state is not a node without tracing.
func TestMissingStateIsNotMissingTracing(t *testing.T) {
	require.False(t, TracingAbsent(fmt.Errorf("-32000: historical state is not available")))
	require.False(t, TracingAbsent(fmt.Errorf("missing trie node 1a2b (path ) state 0x1a2b is not available")))
	require.True(t, TracingAbsent(fmt.Errorf("the method debug_traceBlockByNumber does not exist/is not available")))
	require.True(t, TracingAbsent(fmt.Errorf("Method not found")))
}

// answerServer traces transaction A whole, and answers B's trace as b says: "refused" (the node lacks the state),
// "timeout" (the tracer timed out) or "down" (an HTTP 503), per block or per transaction.
func answerServer(t *testing.T, blockTraced bool, b string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []any           `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		msg := map[string]string{"refused": "missing trie node 1a2b (path )", "timeout": "execution timeout"}[b]
		if b == "down" && (req.Method == "debug_traceBlockByNumber" || req.Params[0] == traceTxB.Hex()) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case req.Method == "debug_traceBlockByNumber" && blockTraced:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":[{"txHash":%q,"result":%s},{"txHash":%q,"error":%q}]}`,
				req.ID, traceTxA.Hex(), traceFrameWithACall, traceTxB.Hex(), msg)
		case req.Method == "debug_traceBlockByNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":%q}}`, req.ID, msg)
		case req.Params[0] == traceTxA.Hex():
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, traceFrameWithACall)
		default:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":%q}}`, req.ID, msg)
		}
	}))
}

// A fetch that skips refusals leaves out a transaction the node answers it cannot trace, and fails on a timeout or a
// node that does not answer, which say nothing of the trace.
func TestATraceFetchSkippingRefusalsFailsOnAnythingElse(t *testing.T) {
	for _, c := range []struct {
		b           string
		blockTraced bool
		fails       bool
	}{
		{"refused", true, false},
		{"refused", false, false},
		{"timeout", true, true},
		{"timeout", false, true},
		{"down", false, true},
	} {
		t.Run(fmt.Sprintf("%s/block=%v", c.b, c.blockTraced), func(t *testing.T) {
			srv := answerServer(t, c.blockTraced, c.b)
			defer srv.Close()
			cl, err := New(srv.URL)
			require.NoError(t, err)
			got, err := cl.FetchTracesBatchSkippingRefusals(context.Background(), []common.Hash{traceTxA, traceTxB}, 7,
				1700000000, 2, 100)
			if c.fails {
				require.Error(t, err)
				require.False(t, NodeRefused(err))
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1, "A's internal call, B's left out")
		})
	}
}

// heavyTraceServer traces transaction A whole and B, a heavy transaction on cold state, only when the request gives
// the tracer a timeout: under the node's default it answers B with "execution timeout". It records the timeouts it
// was sent.
func heavyTraceServer(t *testing.T, blockTraced bool, timeouts *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		var tx string
		require.NoError(t, json.Unmarshal(req.Params[0], &tx))
		var cfg struct {
			Timeout string `json:"timeout"`
		}
		require.NoError(t, json.Unmarshal(req.Params[1], &cfg))
		*timeouts = append(*timeouts, cfg.Timeout)
		b := traceFrameWithACall
		if cfg.Timeout == "" {
			b = ""
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case req.Method == "debug_traceBlockByNumber" && blockTraced && b == "":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":[{"txHash":%q,"result":%s},{"txHash":%q,"error":"execution timeout"}]}`,
				req.ID, traceTxA.Hex(), traceFrameWithACall, traceTxB.Hex())
		case req.Method == "debug_traceBlockByNumber" && blockTraced:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":[{"txHash":%q,"result":%s},{"txHash":%q,"result":%s}]}`,
				req.ID, traceTxA.Hex(), traceFrameWithACall, traceTxB.Hex(), b)
		case req.Method == "debug_traceBlockByNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"not available"}}`, req.ID)
		case tx == traceTxA.Hex() || b != "":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, traceFrameWithACall)
		default:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":"execution timeout"}}`, req.ID)
		}
	}))
}

// The counts of old blocks give the tracer a timeout well above a node's default (5 s on geth), which a heavy
// transaction on cold state exceeds: such a count retries a failed block rather than leave it out, so under the
// default it would fail that block on every try. Ingest's fetch keeps the node's default.
func TestTheCountsOfOldBlocksGiveTheTracerATimeout(t *testing.T) {
	for _, blockTraced := range []bool{true, false} {
		t.Run(fmt.Sprintf("block=%v", blockTraced), func(t *testing.T) {
			var timeouts []string
			srv := heavyTraceServer(t, blockTraced, &timeouts)
			defer srv.Close()
			c, err := New(srv.URL)
			require.NoError(t, err)
			ctx := context.Background()
			txs := []common.Hash{traceTxA, traceTxB}

			for name, fetch := range map[string]func(context.Context, []common.Hash, uint64, uint64, int, int) ([]*explorerTypes.InternalTransaction, error){
				"strict": c.FetchTracesBatchStrict, "skipping refusals": c.FetchTracesBatchSkippingRefusals,
			} {
				timeouts = nil
				got, err := fetch(ctx, txs, 7, 1700000000, 2, 100)
				require.NoError(t, err, name)
				require.Len(t, got, 2, "%s: A's and B's internal calls", name)
				require.NotEmpty(t, timeouts, name)
				for _, timeout := range timeouts {
					require.NotEmpty(t, timeout, "%s: every trace call gives the tracer a timeout", name)
				}
			}

			timeouts = nil
			_, err = c.FetchTracesBatch(ctx, txs, 7, 1700000000, 2, 100)
			require.NoError(t, err)
			for _, timeout := range timeouts {
				require.Empty(t, timeout, "ingest keeps the node's default")
			}
		})
	}
}
