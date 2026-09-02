package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

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
