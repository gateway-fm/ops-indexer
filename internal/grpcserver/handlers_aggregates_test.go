package grpcserver

import (
	"context"
	"fmt"
	"testing"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

// txHash returns the deterministic hash seedChain uses for block n's tx.
func txHash(n uint64) string { return fmt.Sprintf("0x%064x", 0x1000+n) }

func strptr(s string) *string { return &s }

// TestGetDailyStats_SurfacesAggregateFields is the RD-1061 charts/counters
// fix: the daily-stats aggregate fields (cumulative_*, success/failed counts,
// new_contracts, token_transfer_count, avg_block_{time,size}) must flow through
// the proto DailyStats message and the mapper, not just live in the DB.
func TestGetDailyStats_SurfacesAggregateFields(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	want := &types.DailyStats{
		Date:                   "2026-06-01",
		TotalBlocks:            10,
		TotalTransactions:      42,
		TotalGasUsed:           1_000_000,
		AvgGasPrice:            7,
		SuccessfulTxs:          40,
		FailedTxs:              2,
		ActiveAddresses:        15,
		NewAddresses:           9,
		AvgBlockTime:           12.5,
		AvgBlockSize:           777,
		NewContracts:           3,
		TokenTransferCount:     21,
		CumulativeTransactions: 1042,
		CumulativeAddresses:    509,
		CumulativeContracts:    33,
	}
	if err := h.db.UpsertDailyStats(context.Background(), want); err != nil {
		t.Fatalf("UpsertDailyStats: %v", err)
	}

	resp, err := h.client.GetDailyStats(context.Background(), &indexerv1.GetDailyStatsRequest{
		FromDate: "2026-06-01",
		ToDate:   "2026-06-01",
	})
	if err != nil {
		t.Fatalf("GetDailyStats: %v", err)
	}
	if len(resp.GetDays()) != 1 {
		t.Fatalf("expected 1 day, got %d", len(resp.GetDays()))
	}
	d := resp.GetDays()[0]

	// Fields that already existed (regression guard).
	if d.GetTransactions() != 42 || d.GetNewAddresses() != 9 ||
		d.GetActiveAddresses() != 15 || d.GetBlocks() != 10 || d.GetGasUsed() != 1_000_000 {
		t.Errorf("pre-existing fields not surfaced: %+v", d)
	}

	// The newly-exposed aggregate fields.
	checks := []struct {
		name string
		got  uint64
		want uint64
	}{
		{"cumulative_transactions", d.GetCumulativeTransactions(), 1042},
		{"cumulative_addresses", d.GetCumulativeAddresses(), 509},
		{"cumulative_contracts", d.GetCumulativeContracts(), 33},
		{"success_count", d.GetSuccessCount(), 40},
		{"failed_count", d.GetFailedCount(), 2},
		{"new_contracts", d.GetNewContracts(), 3},
		{"token_transfer_count", d.GetTokenTransferCount(), 21},
		{"avg_block_size", d.GetAvgBlockSize(), 777},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if d.GetAvgBlockTime() < 12.4 || d.GetAvgBlockTime() > 12.6 {
		t.Errorf("avg_block_time = %v, want ~12.5", d.GetAvgBlockTime())
	}
}

// TestListTransactionsPaginated_TrueTotal is the RD-1061 BUG-8 fix: the
// offset-paginated tx feed reports a real chain-wide total_items, not the page
// length. seedChain inserts exactly 5 transactions.
func TestListTransactionsPaginated_TrueTotal(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	resp, err := h.client.ListTransactionsPaginated(context.Background(), &indexerv1.ListTransactionsPaginatedRequest{
		Page: &indexerv1.OffsetPageRequest{Page: 1, PageSize: 2},
	})
	if err != nil {
		t.Fatalf("ListTransactionsPaginated: %v", err)
	}
	if got := len(resp.GetTransactions()); got != 2 {
		t.Fatalf("page 1: expected 2 transactions, got %d", got)
	}
	if got := resp.GetPage().GetTotalItems(); got != 5 {
		t.Errorf("total_items = %d, want 5 (the true chain total, not page size)", got)
	}
	if resp.GetPage().GetPage() != 1 || resp.GetPage().GetPageSize() != 2 {
		t.Errorf("page echo = (%d,%d), want (1,2)", resp.GetPage().GetPage(), resp.GetPage().GetPageSize())
	}

	// Last (partial) page still reports the same total.
	resp3, err := h.client.ListTransactionsPaginated(context.Background(), &indexerv1.ListTransactionsPaginatedRequest{
		Page: &indexerv1.OffsetPageRequest{Page: 3, PageSize: 2},
	})
	if err != nil {
		t.Fatalf("ListTransactionsPaginated page 3: %v", err)
	}
	if got := len(resp3.GetTransactions()); got != 1 {
		t.Errorf("page 3: expected 1 transaction, got %d", got)
	}
	if got := resp3.GetPage().GetTotalItems(); got != 5 {
		t.Errorf("page 3 total_items = %d, want 5", got)
	}
}

// TestListEndpoints_TotalCount is the RD-1061 "of N" fix: ListLogs,
// ListTokenTransfers and ListInternalTransactions expose a filter-wide
// total_count independent of page size.
func TestListEndpoints_TotalCount(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()

	const acct = "0xc0ffee0000000000000000000000000000000001"
	const token = "0xdeadbeef00000000000000000000000000000001"
	const topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

	// 3 logs emitted by acct (block 1's tx).
	for i := 0; i < 3; i++ {
		if err := h.db.InsertLog(ctx, &types.Log{
			TxHash: txHash(1), LogIndex: i, Address: acct,
			Topic0: strptr(topic), Data: "0x", BlockNumber: 1,
		}); err != nil {
			t.Fatalf("InsertLog %d: %v", i, err)
		}
	}

	// 4 token transfers of `token`, all sent from acct (block 2's tx).
	for i := 0; i < 4; i++ {
		if err := h.db.InsertTokenTransfer(ctx, &types.TokenTransfer{
			TxHash: txHash(2), LogIndex: i, TokenAddress: token,
			From: acct, To: "0xbbb0000000000000000000000000000000000002",
			Value: "1000", BlockNumber: 2, TransferType: "transfer", TokenType: "ERC20",
		}); err != nil {
			t.Fatalf("InsertTokenTransfer %d: %v", i, err)
		}
	}

	// 5 internal txs from acct (block 3's tx).
	for i := 0; i < 5; i++ {
		if err := h.db.InsertInternalTransaction(ctx, &types.InternalTransaction{
			TxHash: txHash(3), BlockNumber: 3, TraceAddress: fmt.Sprintf("%d", i),
			From: acct, To: strptr("0xbbb0000000000000000000000000000000000002"),
			Value: "0", CallType: "call",
		}); err != nil {
			t.Fatalf("InsertInternalTransaction %d: %v", i, err)
		}
	}

	t.Run("logs by_address", func(t *testing.T) {
		resp, err := h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{
			ByAddress: acct,
			Page:      &indexerv1.PageRequest{PageSize: 2},
		})
		if err != nil {
			t.Fatalf("ListLogs: %v", err)
		}
		if got := len(resp.GetLogs()); got != 2 {
			t.Errorf("page returned %d logs, want 2 (page_size)", got)
		}
		if got := resp.GetTotalCount(); got != 3 {
			t.Errorf("total_count = %d, want 3", got)
		}
	})

	t.Run("logs by_tx_hash counts all logs of the tx", func(t *testing.T) {
		resp, err := h.client.ListLogs(ctx, &indexerv1.ListLogsRequest{ByTxHash: txHash(1)})
		if err != nil {
			t.Fatalf("ListLogs: %v", err)
		}
		if got := resp.GetTotalCount(); got != 3 {
			t.Errorf("total_count = %d, want 3", got)
		}
	})

	t.Run("token transfers by_token", func(t *testing.T) {
		resp, err := h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{
			ByToken: token,
			Page:    &indexerv1.PageRequest{PageSize: 2},
		})
		if err != nil {
			t.Fatalf("ListTokenTransfers: %v", err)
		}
		if got := resp.GetTotalCount(); got != 4 {
			t.Errorf("total_count = %d, want 4", got)
		}
	})

	t.Run("token transfers by_address", func(t *testing.T) {
		resp, err := h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{
			ByAddress: acct,
			Page:      &indexerv1.PageRequest{PageSize: 2},
		})
		if err != nil {
			t.Fatalf("ListTokenTransfers: %v", err)
		}
		if got := resp.GetTotalCount(); got != 4 {
			t.Errorf("total_count = %d, want 4", got)
		}
	})

	t.Run("internal txs by_address", func(t *testing.T) {
		resp, err := h.client.ListInternalTransactions(ctx, &indexerv1.ListInternalTransactionsRequest{
			ByAddress: acct,
			Page:      &indexerv1.PageRequest{PageSize: 2},
		})
		if err != nil {
			t.Fatalf("ListInternalTransactions: %v", err)
		}
		if got := len(resp.GetInternalTransactions()); got != 2 {
			t.Errorf("page returned %d internal txs, want 2 (page_size)", got)
		}
		if got := resp.GetTotalCount(); got != 5 {
			t.Errorf("total_count = %d, want 5", got)
		}
	})
}
