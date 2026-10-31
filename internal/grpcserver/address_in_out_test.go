package grpcserver

import (
	"context"
	"testing"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

// The seeded chain has 0xaaa…01 send five transactions to 0xbbb…02. Address documents tx_count_in as the transactions
// the address is `to` in and tx_count_out as those it is `from` in, and the gRPC consumers show their sum as the
// address's transaction count. Each address took part in five transactions, so in + out must be five for both, in
// GetAddress and in ListAddresses.
func TestAddressInPlusOutIsTheTransactionsTheAddressTookPartIn(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	if err := h.db.RebuildAddressStats(ctx); err != nil {
		t.Fatalf("RebuildAddressStats: %v", err)
	}

	const sender, recipient = "0xaaa0000000000000000000000000000000000001", "0xbbb0000000000000000000000000000000000002"
	for _, address := range []string{sender, recipient} {
		a, err := h.client.GetAddress(ctx, &indexerv1.GetAddressRequest{Address: address})
		if err != nil {
			t.Fatalf("GetAddress %s: %v", address, err)
		}
		if got := a.GetTxCountIn() + a.GetTxCountOut(); got != 5 {
			t.Errorf("GetAddress %s: tx_count_in %d + tx_count_out %d = %d, want 5 transactions",
				address, a.GetTxCountIn(), a.GetTxCountOut(), got)
		}
	}

	list, err := h.client.ListAddresses(ctx, &indexerv1.ListAddressesRequest{Page: &indexerv1.OffsetPageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("ListAddresses: %v", err)
	}
	for _, a := range list.GetAddresses() {
		if got := a.GetTxCountIn() + a.GetTxCountOut(); got != 5 {
			t.Errorf("ListAddresses %s: tx_count_in %d + tx_count_out %d = %d, want 5 transactions",
				a.GetAddress(), a.GetTxCountIn(), a.GetTxCountOut(), got)
		}
	}
}
