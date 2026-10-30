package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// A node that answers eth_blockNumber without a number is an error, not head 0: the refill stops at the head read,
// before it looks up a block or checkpoints a day against it.
func TestRefillRejectsAHeadThatIsNotANumber(t *testing.T) {
	for _, head := range []string{`null`, `""`, `"0x"`} {
		t.Run(head, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				type request struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
				}
				answer := func(req request) string {
					result := "null"
					if req.Method == "eth_blockNumber" {
						result = head
					}
					return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, result)
				}
				var raw json.RawMessage
				if json.NewDecoder(r.Body).Decode(&raw) != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var batch []request
				if json.Unmarshal(raw, &batch) != nil {
					var one request
					_ = json.Unmarshal(raw, &one)
					fmt.Fprint(w, answer(one))
					return
				}
				out := make([]string, len(batch))
				for i, req := range batch {
					out[i] = answer(req)
				}
				fmt.Fprintf(w, "[%s]", strings.Join(out, ","))
			}))
			defer srv.Close()
			node := RefillNode{C: rpclient.Dial(srv.URL)}

			_, err := node.BlockNumber(context.Background())
			require.ErrorContains(t, err, "eth_blockNumber")

			saved := &refillCheckpoints{}
			_, err = RefillDailyStats(context.Background(), saved, node, []time.Time{time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)},
				DailyRefillOptions{BatchesPerSecond: 1e6})
			require.ErrorContains(t, err, "eth_blockNumber", "the error names the head read, not a block the head led to")
			require.Empty(t, saved.saved)
		})
	}
}
