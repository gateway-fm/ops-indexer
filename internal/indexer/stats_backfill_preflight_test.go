package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// coldArchiveNode traces an old block whole only when the request gives the tracer the counts' timeout: under the
// node's default the block's heaviest transaction times out. Its block 1 has no transaction, so a check of it traces
// the block whole.
func coldArchiveNode(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		var cfg struct {
			Timeout string `json:"timeout"`
		}
		if req.Method == "debug_traceBlockByNumber" && len(req.Params) > 1 {
			require.NoError(t, json.Unmarshal(req.Params[1], &cfg))
		}
		switch {
		case req.Method == "debug_traceBlockByNumber" && cfg.Timeout == "50s":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":[]}`, req.ID)
		case req.Method == "debug_traceBlockByNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":"execution timeout"}}`, req.ID)
		case req.Method == "eth_getBlockByNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"number":"0x1","hash":"0x%064x","parentHash":"0x%064x","timestamp":"0x1","transactions":[]}}`,
				req.ID, 1, 0)
		default:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"unexpected %s"}}`, req.ID, req.Method)
		}
	}))
}

// The start's check that the node traces the old blocks asks as the counts of those blocks do, with their tracer
// timeout: a node that traces them for the backfill passes the check, rather than fail every start on the node's
// shorter default.
func TestTheStartsCheckOfOldTracesGivesTheTracerTheCountsTimeout(t *testing.T) {
	srv := coldArchiveNode(t)
	defer srv.Close()
	client, err := rpc.New(srv.URL)
	require.NoError(t, err)
	counting := &initStore{state: db.BackfillState{Exists: true, Target: 4_999, Through: 1_000}}
	require.NoError(t, PrepareStatsBackfill(context.Background(), counting, client, 5_000, true))
}

// perTxOnlyNode answers every whole-block trace with the error wholeBlock, under any tracer timeout, and traces each
// transaction of block 1 on its own: the counts of old blocks fall back to that and count the block.
func perTxOnlyNode(t *testing.T, wholeBlock string) *httptest.Server {
	t.Helper()
	const tx = "0x00000000000000000000000000000000000000000000000000000000000000a1"
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "debug_traceBlockByNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":%q}}`, req.ID, wholeBlock)
		case "debug_traceTransaction":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"type":"CALL","from":"0x00000000000000000000000000000000000000aa","to":"0x00000000000000000000000000000000000000bb"}}`, req.ID)
		case "eth_getBlockByNumber":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"number":"0x1","hash":"0x%064x","parentHash":"0x%064x","timestamp":"0x1","transactions":[{"hash":%q,"blockNumber":"0x1","transactionIndex":"0x0","from":"0x00000000000000000000000000000000000000aa","to":"0x00000000000000000000000000000000000000bb","value":"0x0","gas":"0x5208","gasPrice":"0x1","input":"0x","nonce":"0x0"}]}}`,
				req.ID, 1, 0, tx)
		default:
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"unexpected %s"}}`, req.ID, req.Method)
		}
	}))
}

// The start's check that the node traces the old blocks traces block 1 as the counts of those blocks do: a node whose
// whole-block trace of block 1 fails (a timeout, or a response over the node's size limit) but that traces each of
// its transactions passes, as the backfill would count the block.
func TestTheStartsCheckOfOldTracesFallsBackToTracingEachTransaction(t *testing.T) {
	for _, wholeBlock := range []string{"execution timeout", "response size exceeds limit"} {
		t.Run(wholeBlock, func(t *testing.T) {
			srv := perTxOnlyNode(t, wholeBlock)
			defer srv.Close()
			client, err := rpc.New(srv.URL)
			require.NoError(t, err)
			strict, err := client.FetchTracesBatchStrict(context.Background(),
				[]common.Hash{common.HexToHash("0xa1")}, 1, 1, 2, 100)
			require.NoError(t, err, "the counts of old blocks trace block 1 one transaction at a time")
			require.Empty(t, strict, "its one call has no internal call")
			counting := &initStore{state: db.BackfillState{Exists: true, Target: 4_999, Through: 1_000}}
			require.NoError(t, PrepareStatsBackfill(context.Background(), counting, client, 5_000, true))
		})
	}
}
