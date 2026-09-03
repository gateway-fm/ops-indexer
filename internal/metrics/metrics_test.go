package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// scrape starts the metrics server on an ephemeral port and returns the body of
// one /metrics request.
func scrape(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close probe listener: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- ServeMetrics(ctx, addr) }()

	var body string
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err == nil {
			b, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				t.Fatalf("read body: %v", readErr)
			}
			body = string(b)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics server never came up: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve returned: %v", err)
	}
	return body
}

// TestMetricsEndpointServesProgressSeries guards the failure this ticket exists
// to prevent: a series that is declared but never exported, so a panel built on
// it reads a confident zero forever.
func TestMetricsEndpointServesProgressSeries(t *testing.T) {
	SetChainHead(1000)
	SetLastIndexed(400)
	SetMissingBlocks(600)
	BlockIndexed(7, 21000)

	body := scrape(t)

	for _, want := range []string{
		"indexer_chain_head_block 1000",
		"indexer_last_indexed_block 400",
		"indexer_missing_blocks 600",
		"indexer_blocks_indexed_total 1",
		"indexer_transactions_indexed_total 7",
		"indexer_gas_indexed_total 21000",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing series %q in /metrics output", want)
		}
	}
}

// TestSetReceiptStrategyExportsEveryPath asserts the alternatives are exported
// as 0 rather than being absent. An alert on an absent series cannot fire.
func TestSetReceiptStrategyExportsEveryPath(t *testing.T) {
	SetReceiptStrategy("per_tx")

	body := scrape(t)

	want := map[string]string{
		"batch":  `indexer_rpc_strategy{kind="receipts",path="batch"} 0`,
		"logs":   `indexer_rpc_strategy{kind="receipts",path="logs"} 0`,
		"per_tx": `indexer_rpc_strategy{kind="receipts",path="per_tx"} 1`,
		"none":   `indexer_rpc_strategy{kind="receipts",path="none"} 0`,
	}
	for path, line := range want {
		if !strings.Contains(body, line) {
			t.Errorf("receipt strategy path %q: expected %q in output", path, line)
		}
	}
}

// TestObserveRPCMatchesRPClientOutcomes keeps the outcome label values that
// rpclient reports and the ones documented here from drifting apart.
func TestObserveRPCMatchesRPClientOutcomes(t *testing.T) {
	outcomes := []string{
		rpclient.OutcomeOK,
		rpclient.OutcomeTransportError,
		rpclient.OutcomeRPCError,
		rpclient.OutcomeDecodeError,
	}
	for _, o := range outcomes {
		ObserveRPC("eth_getBlockByNumber", o, 5*time.Millisecond)
	}

	body := scrape(t)

	for _, o := range outcomes {
		want := `indexer_rpc_requests_total{method="eth_getBlockByNumber",outcome="` + o + `"} 1`
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in /metrics output", want)
		}
	}
	if !strings.Contains(body, `indexer_rpc_request_duration_seconds_count{method="eth_getBlockByNumber"} 4`) {
		t.Error("expected 4 observations on the rpc duration histogram")
	}
}

// TestBalanceQueueDroppedIgnoresNonPositive keeps a no-drop block from moving
// the counter, which would make the "silent data loss" signal meaningless.
func TestBalanceQueueDroppedIgnoresNonPositive(t *testing.T) {
	BalanceQueueDropped(0)
	BalanceQueueDropped(-3)

	if !strings.Contains(scrape(t), "indexer_balance_queue_dropped_total 0") {
		t.Error("expected the dropped counter to stay at 0")
	}

	BalanceQueueDropped(2)
	if !strings.Contains(scrape(t), "indexer_balance_queue_dropped_total 2") {
		t.Error("expected the dropped counter to reach 2")
	}
}

// TestSetStrategyStaysCoherentUnderConcurrency pins the invariant that exactly
// one path per kind is 1. One logical update is several Set calls, so two
// interleaved callers can leave every path at 0 — a state that never resolves
// and silently disables the fallback alert.
//
// The reader takes the same mutex the writer does, so it observes a snapshot
// only when the update is genuinely atomic. Checking after the writers finish
// is NOT enough: the last writer usually leaves a coherent state, so that
// version of this test passes even with the locking removed.
func TestSetStrategyStaysCoherentUnderConcurrency(t *testing.T) {
	const writers = 8

	// Seed before sampling: WithLabelValues creates a series lazily at 0, so an
	// unseeded read is incoherent for reasons that have nothing to do with
	// interleaving. Without this the test depends on an earlier test having run.
	SetReceiptStrategy("batch")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			path := receiptPaths[g%len(receiptPaths)]
			for {
				select {
				case <-stop:
					return
				default:
					SetReceiptStrategy(path)
				}
			}
		}(g)
	}

	var incoherent, samples int
	for samples = 0; samples < 20000; samples++ {
		strategyMu.Lock()
		active := 0
		for _, p := range receiptPaths {
			var m dto.Metric
			if err := rpcStrategy.WithLabelValues("receipts", p).Write(&m); err == nil {
				if m.GetGauge().GetValue() == 1 {
					active++
				}
			}
		}
		strategyMu.Unlock()
		if active != 1 {
			incoherent++
		}
	}

	close(stop)
	wg.Wait()

	if incoherent > 0 {
		t.Errorf("observed %d incoherent snapshots of %d: a receipt strategy update is not atomic",
			incoherent, samples)
	}
}

// TestStrategySeriesExistBeforeAnyFetch guards the claim that every alternative
// is exported as 0 rather than absent. GaugeVec series are lazy, so before this
// was pre-initialised the trace series did not exist at all on a deployment with
// tracing disabled — and an alert cannot fire on a series that is not there.
//
// Deliberately never calls SetTraceStrategy: the trace series must be present
// purely from package initialisation.
func TestStrategySeriesExistBeforeAnyFetch(t *testing.T) {
	body := scrape(t)

	for _, want := range []string{
		`indexer_rpc_strategy{kind="traces",path="batch"} 0`,
		`indexer_rpc_strategy{kind="traces",path="per_tx"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing pre-initialised series %q; a disabled-tracing deployment would export nothing", want)
		}
	}
}
