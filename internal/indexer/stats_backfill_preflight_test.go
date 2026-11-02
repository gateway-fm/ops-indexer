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
)

// coldArchiveNode traces an old block whole only when the request gives the tracer the counts' timeout: under the
// node's default the block's heaviest transaction times out.
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
