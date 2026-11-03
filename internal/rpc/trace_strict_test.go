package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	explorerTypes "github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
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
// "out of range" (the node refuses in its own words), "timeout" (the tracer timed out), "rate limited" or "down" (an
// HTTP 503), per block or per transaction.
func answerServer(t *testing.T, blockTraced bool, b string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []any           `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		msg := map[string]string{"refused": "missing trie node 1a2b (path )",
			"out of range": "BlockOutOfRangeError: block height is 60000 but requested was 7", "timeout": "execution timeout",
			"rate limited": "rate limit exceeded"}[b]
		code := map[string]int{"out of range": -32602, "rate limited": -32005}[b]
		if code == 0 {
			code = -32000
		}
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
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":%d,"message":%q}}`, req.ID, code, msg)
		case req.Params[0] == traceTxA.Hex():
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, traceFrameWithACall)
		default:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":%d,"message":%q}}`, req.ID, code, msg)
		}
	}))
}

// A fetch that skips refusals leaves out a transaction the node answers it cannot trace, in whatever words, and fails
// on a timeout, a rate limit or a node that does not answer, which say nothing of the trace.
func TestATraceFetchSkippingRefusalsFailsOnAnythingElse(t *testing.T) {
	for _, c := range []struct {
		b           string
		blockTraced bool
		fails       bool
	}{
		{"refused", true, false},
		{"refused", false, false},
		{"out of range", false, false},
		{"timeout", true, true},
		{"timeout", false, true},
		{"rate limited", false, true},
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
func heavyTraceServer(t *testing.T, blockTraced bool, timeouts *sentTimeouts) *httptest.Server {
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
		timeouts.add(cfg.Timeout)
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

// sentTimeouts are the tracer timeouts a server was sent, from its concurrent handlers.
type sentTimeouts struct {
	mu   sync.Mutex
	sent []string
}

func (s *sentTimeouts) add(timeout string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, timeout)
}

// take returns the timeouts sent since the last take.
func (s *sentTimeouts) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	sent := s.sent
	s.sent = nil
	return sent
}

// The counts of old blocks give the tracer a timeout well above a node's default (5 s on geth), which a heavy
// transaction on cold state exceeds: such a count retries a failed block rather than leave it out, so under the
// default it would fail that block on every try. Ingest's fetch keeps the node's default.
func TestTheCountsOfOldBlocksGiveTheTracerATimeout(t *testing.T) {
	for _, blockTraced := range []bool{true, false} {
		t.Run(fmt.Sprintf("block=%v", blockTraced), func(t *testing.T) {
			timeouts := &sentTimeouts{}
			srv := heavyTraceServer(t, blockTraced, timeouts)
			defer srv.Close()
			c, err := New(srv.URL)
			require.NoError(t, err)
			ctx := context.Background()
			txs := []common.Hash{traceTxA, traceTxB}

			for name, fetch := range map[string]func(context.Context, []common.Hash, uint64, uint64, int, int) ([]*explorerTypes.InternalTransaction, error){
				"strict": c.FetchTracesBatchStrict, "skipping refusals": c.FetchTracesBatchSkippingRefusals,
			} {
				timeouts.take()
				got, err := fetch(ctx, txs, 7, 1700000000, 2, 100)
				require.NoError(t, err, name)
				require.Len(t, got, 2, "%s: A's and B's internal calls", name)
				sent := timeouts.take()
				require.NotEmpty(t, sent, name)
				for _, timeout := range sent {
					require.NotEmpty(t, timeout, "%s: every trace call gives the tracer a timeout", name)
				}
			}

			timeouts.take()
			_, err = c.FetchTracesBatch(ctx, txs, 7, 1700000000, 2, 100)
			require.NoError(t, err)
			sent := timeouts.take()
			require.NotEmpty(t, sent, "ingest's fetch traced")
			for _, timeout := range sent {
				require.Empty(t, timeout, "ingest keeps the node's default")
			}
		})
	}
}

// Any error the node answers with is it refusing what was asked, except a timeout or a rate limit: those say nothing
// of whether it can serve it. An error the node did not answer with is never a refusal.
func TestAnyAnswerOtherThanATimeoutOrARateLimitIsTheNodeRefusing(t *testing.T) {
	for _, c := range []struct {
		err     error
		refused bool
	}{
		{ErrNotTraced, true},
		{&rpclient.RPCError{Code: -32000, Message: "historical state is not available"}, true},
		{&rpclient.RPCError{Code: -32602, Message: "BlockOutOfRangeError: block height is 60000 but requested was 1"}, true},
		{fmt.Errorf("code at block 1: %w", &rpclient.RPCError{Code: -32000, Message: "pruned history unavailable"}), true},
		{&rpclient.RPCError{Code: -32005, Message: "limit exceeded"}, false},
		{&rpclient.RPCError{Code: -32000, Message: "Rate limit reached"}, false},
		{&rpclient.RPCError{Code: -32000, Message: "too many requests"}, false},
		{&rpclient.RPCError{Code: -32000, Message: "execution timeout"}, false},
		{&rpclient.RPCError{Code: -32000, Message: "context deadline exceeded"}, false},
		{&rpclient.HTTPStatusError{Code: http.StatusTooManyRequests}, false},
		{errors.New("dial tcp 192.0.2.1:8545: connect: connection refused"), false},
		{context.Canceled, false},
	} {
		require.Equal(t, c.refused, NodeRefused(c.err), "%v", c.err)
	}
}
