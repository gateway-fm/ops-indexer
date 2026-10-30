package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// stallingNode serves node's chain, except that the first full read of block stall is accepted and never answered
// while the test runs: a node behind a proxy that holds the connection open.
type stallingNode struct {
	node    http.Handler
	stall   uint64
	release chan struct{}
	mu      sync.Mutex
	stalled bool
}

func (s *stallingNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req struct {
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	_ = json.Unmarshal(body, &req)
	if req.Method == "eth_getBlockByNumber" && len(req.Params) == 2 && string(req.Params[0]) == fmt.Sprintf(`"0x%x"`, s.stall) &&
		string(req.Params[1]) == "true" {
		s.mu.Lock()
		first := !s.stalled
		s.stalled = true
		s.mu.Unlock()
		if first {
			select {
			case <-s.release:
			case <-r.Context().Done():
			}
			return
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	s.node.ServeHTTP(w, r)
}

// A block read the node never answers costs realtime one timeout: the tick fails, the next one reads the block
// again and indexing goes on, instead of waiting on the read for as long as the connection stays open.
func TestRealtimeGoesOnAfterABlockReadTheNodeNeverAnswers(t *testing.T) {
	node, _ := newLaggingNode(t, 102)
	stalling := &stallingNode{node: node, stall: 101, release: make(chan struct{})}
	srv := httptest.NewServer(stalling)
	t.Cleanup(func() { close(stalling.release); srv.Close() })
	client, err := rpc.NewWithTimeout(srv.URL, 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	store := newReorgStore(100, node.tick)
	rt := NewRealtimeIndexer(store, client, &RealtimeConfig{ConfirmationBlocks: 1, PollInterval: 10 * time.Millisecond},
		&Config{RPCWorkers: 1, RPCRateLimit: 100}, NewTokenCache(), NewContractCache(), nil, false)
	rt.SetLastProcessedBlock(100)

	runUntil(t, func(ctx context.Context) { _ = rt.runPollingMode(ctx) }, node.tick, 1, func() bool { return len(store.written()) >= 1 })
	stalling.mu.Lock()
	stalled := stalling.stalled
	stalling.mu.Unlock()
	if !stalled {
		t.Fatal("block 101 was never read in full")
	}
	if got := writtenNumbers(store.written()); fmt.Sprint(got) != "[101]" {
		t.Fatalf("written %v, want [101] after the stalled read timed out", got)
	}
}

// silentNode accepts every request and never answers until the test ends.
func silentNode(t *testing.T) string {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	return srv.URL
}

// backfill-contracts against a node that never answers reports each listed transaction failed at the timeout and
// returns, so a rerun can resume, instead of waiting until it is signalled.
func TestContractsBackfillGivesUpOnANodeThatNeverAnswers(t *testing.T) {
	node, err := rpc.NewWithTimeout(silentNode(t), 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	listed := []Creation{{Address: addr(0xc1), TxHash: "0x" + fmt.Sprintf("%064x", 0xa), Block: 10}}
	for _, opts := range []ContractsBackfillOptions{{PerSecond: 100}, {PerSecond: 100, WithoutTraces: true}} {
		done := make(chan *ContractsBackfillReport, 1)
		go func() {
			report, _ := BackfillContracts(context.Background(), &backfillDB{contracts: map[string]*types.Contract{}}, node, listed, opts)
			done <- report
		}()
		select {
		case report := <-done:
			if report == nil || len(report.Failed) != 1 || report.Written != 0 {
				t.Fatalf("without traces %v: report %+v, want the listed transaction failed and nothing written", opts.WithoutTraces, report)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("without traces %v: still waiting on the node", opts.WithoutTraces)
		}
	}
}

// The refill's node calls fail at the timeout on a node that never answers: the command stops with the error and
// resumes from its checkpoint on a rerun, instead of waiting without a log line.
func TestRefillNodeCallsGiveUpOnANodeThatNeverAnswers(t *testing.T) {
	node := RefillNode{C: rpclient.DialTimeout(silentNode(t), 200*time.Millisecond)}
	ctx := context.Background()
	calls := map[string]func() error{
		"BlockNumber": func() error { _, err := node.BlockNumber(ctx); return err },
		"Blocks":      func() error { _, err := node.Blocks(ctx, []uint64{1, 2}, true); return err },
	}
	for name, call := range calls {
		start := time.Now()
		if err := call(); err == nil {
			t.Fatalf("%s: no error from a node that never answered", name)
		}
		if waited := time.Since(start); waited > 5*time.Second {
			t.Fatalf("%s: gave up after %s", name, waited)
		}
	}
}
