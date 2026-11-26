package grpcserver

import (
	"context"
	"testing"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func TestListTransactions_FeedHonoursToBlock(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	resp, err := h.client.ListTransactions(context.Background(), &indexerv1.ListTransactionsRequest{
		Page:       &indexerv1.PageRequest{PageSize: 10},
		BlockRange: &indexerv1.BlockRange{ToBlock: 4},
	})
	if err != nil {
		t.Fatalf("ListTransactions: %v", err)
	}
	var got []uint64
	for _, tx := range resp.GetTransactions() {
		got = append(got, tx.GetBlockNumber())
	}
	want := []uint64{3, 2, 1}
	if len(got) != len(want) {
		t.Fatalf("blocks = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("blocks = %v, want %v", got, want)
		}
	}
}
