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
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// laggingNode is a JSON-RPC node over HTTP serving an unchanged chain. While lagging, it answers the header-only
// lookups the reorg check makes for the listed blocks with null, as a load-balanced node a few blocks behind does,
// and still serves full blocks.
type laggingNode struct {
	chain   map[uint64]*rpc.RawBlock
	head    uint64
	mu      sync.Mutex
	ticks   int
	lagging map[uint64]bool
}

func (n *laggingNode) tick() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ticks
}

func (n *laggingNode) recover() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lagging = nil
}

func (n *laggingNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
	n.mu.Lock()
	switch req.Method {
	case "eth_blockNumber":
		n.ticks++
		resp["result"] = fmt.Sprintf("0x%x", n.head)
	case "eth_getBlockByNumber":
		var hexNum string
		var full bool
		_ = json.Unmarshal(req.Params[0], &hexNum)
		_ = json.Unmarshal(req.Params[1], &full)
		num, _ := strconv.ParseUint(strings.TrimPrefix(hexNum, "0x"), 16, 64)
		b, ok := n.chain[num]
		if !ok || (!full && n.lagging[num]) {
			resp["result"] = nil
			break
		}
		resp["result"] = map[string]any{"number": hexNum, "hash": b.Hash.Hex(), "parentHash": b.ParentHash.Hex(),
			"transactions": []any{}}
	default:
		resp["error"] = map[string]any{"code": -32601, "message": "unexpected " + req.Method}
	}
	n.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func newLaggingNode(t *testing.T, head uint64, lagging ...uint64) (*laggingNode, *rpc.Client) {
	t.Helper()
	node := &laggingNode{chain: newReorgNode(head+1, head).chain, head: head, lagging: map[uint64]bool{}}
	for _, b := range lagging {
		node.lagging[b] = true
	}
	srv := httptest.NewServer(node)
	t.Cleanup(srv.Close)
	client, err := rpc.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return node, client
}

// runUntil runs loop until done reports true after at least ticks node ticks, or the deadline passes.
func runUntil(t *testing.T, loop func(ctx context.Context), tick func() int, ticks int, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		loop(ctx)
	}()
	for ctx.Err() == nil && (tick() < ticks || !done()) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-finished
}

// A node that answers null for the stored head's blocks is behind, not on another fork: no stored block is reverted,
// and once it catches up indexing goes on from the stored head with no gap.
func TestLaggingNodeNullHeaderDoesNotRevertStoredBlocks(t *testing.T) {
	loops := map[string]func(store *reorgStore, client *rpc.Client) func(ctx context.Context){
		"realtime": func(store *reorgStore, client *rpc.Client) func(ctx context.Context) {
			rt := NewRealtimeIndexer(store, client, &RealtimeConfig{ConfirmationBlocks: 1, PollInterval: 10 * time.Millisecond},
				&Config{RPCWorkers: 1, RPCRateLimit: 100}, NewTokenCache(), NewContractCache(), nil, false)
			rt.SetLastProcessedBlock(100)
			return func(ctx context.Context) { _ = rt.runPollingMode(ctx) }
		},
		"standard": func(store *reorgStore, client *rpc.Client) func(ctx context.Context) {
			idx := NewWithConfig(store, client, 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
			return func(ctx context.Context) { _ = idx.startStandardMode(ctx, 100) }
		},
	}
	for name, loop := range loops {
		t.Run(name, func(t *testing.T) {
			node, client := newLaggingNode(t, 103, 98, 99, 100)
			store := newReorgStore(100, node.tick)
			run := loop(store, client)

			runUntil(t, run, node.tick, 3, func() bool { return true })
			if d := store.deletedBlocks(); len(d) != 0 {
				t.Fatalf("deleted %v while the node lagged, want nothing deleted", d)
			}
			if w := writtenNumbers(store.written()); len(w) != 0 {
				t.Fatalf("written %v while the node lagged, want the tick to end", w)
			}

			node.recover()
			want := map[string][]uint64{"realtime": {101, 102}, "standard": {101, 102, 103}}[name]
			runUntil(t, run, node.tick, 1, func() bool { return len(store.written()) >= len(want) })
			if d := store.deletedBlocks(); len(d) != 0 {
				t.Fatalf("deleted %v, want nothing deleted", d)
			}
			got := writtenNumbers(store.written())
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("written %v after the node caught up, want %v", got, want)
			}
			for n, h := range store.hashes(1, 100) {
				if h != oldHash(n).Hex() {
					t.Fatalf("stored block %d is %q, want the unchanged chain's", n, h)
				}
			}
		})
	}
}
