package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// receiptNode serves blocks of txsPerBlock transactions, refuses eth_getBlockReceipts, and answers each
// eth_getTransactionReceipt after latency, recording how many were in flight at once and when each started.
type receiptNode struct {
	txsPerBlock int
	latency     time.Duration
	inFlight    atomic.Int64
	peak        atomic.Int64
	mu          sync.Mutex
	starts      []time.Time
}

func (n *receiptNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
	switch req.Method {
	case "eth_getBlockByNumber":
		var hexNum string
		_ = json.Unmarshal(req.Params[0], &hexNum)
		num, _ := strconv.ParseUint(strings.TrimPrefix(hexNum, "0x"), 16, 64)
		txs := make([]map[string]any, n.txsPerBlock)
		for i := range txs {
			txs[i] = map[string]any{"hash": fmt.Sprintf("0x%064x", num*1000+uint64(i)), "type": "0x0"}
		}
		resp["result"] = map[string]any{"number": hexNum, "hash": fmt.Sprintf("0x%064x", num), "transactions": txs}
	case "eth_getBlockReceipts":
		resp["error"] = map[string]any{"code": -32601, "message": "the method eth_getBlockReceipts does not exist"}
	case "eth_getTransactionReceipt":
		now := n.inFlight.Add(1)
		for {
			peak := n.peak.Load()
			if now <= peak || n.peak.CompareAndSwap(peak, now) {
				break
			}
		}
		n.mu.Lock()
		n.starts = append(n.starts, time.Now())
		n.mu.Unlock()
		time.Sleep(n.latency)
		n.inFlight.Add(-1)
		var hash string
		_ = json.Unmarshal(req.Params[0], &hash)
		resp["result"] = map[string]any{"transactionHash": hash, "status": "0x1", "gasUsed": "0x5208", "logs": []any{}}
	default:
		resp["error"] = map[string]any{"code": -32601, "message": "unexpected " + req.Method}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// The per-transaction receipt fallback shares one RPC_WORKERS and RPC_RATE_LIMIT budget across every block the
// loop prefetches, instead of each block getting its own.
func TestPrefetchedReceiptFallbackSharesOneRPCBudget(t *testing.T) {
	const workers, perSecond, blocks, txsPerBlock = 4, 100, 20, 5
	node := &receiptNode{txsPerBlock: txsPerBlock, latency: 10 * time.Millisecond}
	srv := httptest.NewServer(node)
	defer srv.Close()
	client, err := rpc.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	client.LimitReceipts(workers, perSecond)
	idx := NewWithConfig(new(MockDatabase), client, time.Second, 0, &Config{RPCWorkers: workers, RPCRateLimit: perSecond})

	for slot := range idx.prefetchBlocks(context.Background(), 1, blocks, blockPrefetchDepth) {
		if b := <-slot; b.err != nil {
			t.Fatalf("block %d: %v", b.number, b.err)
		}
	}

	node.mu.Lock()
	starts := append([]time.Time(nil), node.starts...)
	node.mu.Unlock()
	if len(starts) != blocks*txsPerBlock {
		t.Fatalf("%d receipt calls, want %d", len(starts), blocks*txsPerBlock)
	}
	if peak := node.peak.Load(); peak > workers {
		t.Errorf("peak %d receipt calls in flight, want at most RPC_WORKERS=%d", peak, workers)
	}
	burst := perSecond/10 + 1
	first, last := starts[0], starts[0]
	for _, s := range starts {
		if s.Before(first) {
			first = s
		}
		if s.After(last) {
			last = s
		}
	}
	rate := float64(len(starts)-burst) / last.Sub(first).Seconds()
	if rate > perSecond*1.15 {
		t.Errorf("%.0f receipt calls/s after the burst, want about RPC_RATE_LIMIT=%d", rate, perSecond)
	}
	t.Logf("%d calls, peak %d in flight, %.0f calls/s after a burst of %d", len(starts), node.peak.Load(), rate, burst)
}
