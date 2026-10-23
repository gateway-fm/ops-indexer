package grpcserver

import (
	"context"
	"testing"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

// PRST-5425: the stats tile showed 0 tokens while chain_counters held 17; ChainStats had no field for it.
func TestChainStatsCarriesTheTokenCount(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	if _, err := h.pool.Exec(ctx, `
		INSERT INTO chain_counters (name, count) VALUES ('tokens_total', 17)
		ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count`); err != nil {
		t.Fatalf("seed tokens_total: %v", err)
	}
	resp, err := h.client.GetChainStats(ctx, &indexerv1.Empty{})
	if err != nil {
		t.Fatalf("GetChainStats: %v", err)
	}
	if resp.GetTotalTokens() != 17 {
		t.Fatalf("total_tokens = %d, want 17", resp.GetTotalTokens())
	}
}
