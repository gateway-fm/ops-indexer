package grpcserver

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

// prune simulates a retention pass: history at or below through is deleted and the watermark is set.
func (h *testHarness) prune(through uint64) {
	h.t.Helper()
	ctx := context.Background()
	for _, table := range []string{"token_transfers", "transactions"} {
		if _, err := h.pool.Exec(ctx, `DELETE FROM `+table+` WHERE block_number <= $1`, through); err != nil {
			h.t.Fatalf("prune %s: %v", table, err)
		}
	}
	if _, err := h.pool.Exec(ctx, `DELETE FROM blocks WHERE number <= $1`, through); err != nil {
		h.t.Fatalf("prune blocks: %v", err)
	}
	if _, err := h.pool.Exec(ctx, `
		UPDATE prune_state SET pruned_through = $1::bigint, pruned_through_ts = 1700000000 + $1::bigint WHERE id = 1`, through); err != nil {
		h.t.Fatalf("set watermark: %v", err)
	}
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v (%v), want %v", got, err, want)
	}
}

func TestRetention_GetBlockByNumber(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()

	// Warm the cache with W = 0 so the pruned block is only caught by the refresh on a miss.
	if _, err := h.client.GetChainStats(ctx, &indexerv1.Empty{}); err != nil {
		t.Fatalf("GetChainStats: %v", err)
	}
	h.prune(2)

	for _, n := range []uint64{1, 2} {
		_, err := h.client.GetBlock(ctx, &indexerv1.GetBlockRequest{Selector: &indexerv1.GetBlockRequest_Number{Number: n}})
		requireCode(t, err, codes.OutOfRange)
	}
	if _, err := h.client.GetBlock(ctx, &indexerv1.GetBlockRequest{Selector: &indexerv1.GetBlockRequest_Number{Number: 3}}); err != nil {
		t.Fatalf("GetBlock 3: %v", err)
	}
	_, err := h.client.GetBlock(ctx, &indexerv1.GetBlockRequest{Selector: &indexerv1.GetBlockRequest_Number{Number: 999}})
	requireCode(t, err, codes.NotFound)
}

func TestRetention_HashLookupsNotFoundWithNote(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	h.prune(2)

	_, err := h.client.GetBlock(ctx, &indexerv1.GetBlockRequest{
		Selector: &indexerv1.GetBlockRequest_Hash{Hash: "0x0000000000000000000000000000000000000000000000000000000000000001"},
	})
	requireCode(t, err, codes.NotFound)
	if !strings.Contains(status.Convert(err).Message(), "(history at or below block 2 is pruned)") {
		t.Errorf("message %q lacks the retention note", status.Convert(err).Message())
	}

	_, err = h.client.GetTransaction(ctx, &indexerv1.GetTransactionRequest{Hash: txHash(1)})
	requireCode(t, err, codes.NotFound)
	if !strings.Contains(status.Convert(err).Message(), "pruned") {
		t.Errorf("message %q lacks the retention note", status.Convert(err).Message())
	}
}

func TestRetention_ChainStatsWatermark(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()

	resp, err := h.client.GetChainStats(ctx, &indexerv1.Empty{})
	if err != nil {
		t.Fatalf("GetChainStats: %v", err)
	}
	if resp.GetPrunedThroughBlock() != 0 || resp.GetPrunedThroughTimestamp() != nil {
		t.Fatalf("unpruned: watermark = (%d, %v), want (0, unset)", resp.GetPrunedThroughBlock(), resp.GetPrunedThroughTimestamp())
	}

	h.prune(2)
	if _, _, err := h.srv.refreshPrunedThrough(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	resp, err = h.client.GetChainStats(ctx, &indexerv1.Empty{})
	if err != nil {
		t.Fatalf("GetChainStats: %v", err)
	}
	if resp.GetPrunedThroughBlock() != 2 || resp.GetPrunedThroughTimestamp().GetSeconds() != 1_700_000_002 {
		t.Fatalf("watermark = (%d, %v), want (2, 1700000002)", resp.GetPrunedThroughBlock(), resp.GetPrunedThroughTimestamp())
	}
	if resp.GetTotalTransactions() != 5 {
		t.Errorf("total_transactions = %d, want 5 (totals count pruned history)", resp.GetTotalTransactions())
	}
}

func TestRetention_TokenTransfersByTokenTotalCountsPruned(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	const token = "0xdeadbeef00000000000000000000000000000001"

	if _, err := h.pool.Exec(ctx, `
		INSERT INTO tokens (address, symbol, token_type, block_number) VALUES ($1, 'TKN', 'ERC20', 1)`, token); err != nil {
		t.Fatalf("seed token: %v", err)
	}
	for n := uint64(1); n <= 5; n++ {
		if err := h.db.InsertTokenTransfer(ctx, &types.TokenTransfer{
			TxHash: txHash(n), LogIndex: 0, TokenAddress: token,
			From: "0xaaa0000000000000000000000000000000000001", To: "0xbbb0000000000000000000000000000000000002",
			Value: "1", BlockNumber: n, TransferType: "transfer", TokenType: "ERC20",
		}); err != nil {
			t.Fatalf("InsertTokenTransfer %d: %v", n, err)
		}
	}
	h.prune(2)

	resp, err := h.client.ListTokenTransfers(ctx, &indexerv1.ListTokenTransfersRequest{
		ByToken: token,
		Page:    &indexerv1.PageRequest{PageSize: 10},
	})
	if err != nil {
		t.Fatalf("ListTokenTransfers: %v", err)
	}
	if got := len(resp.GetTransfers()); got != 3 {
		t.Errorf("rows = %d, want 3 (retained only)", got)
	}
	if got := resp.GetTotalCount(); got != 5 {
		t.Errorf("total_count = %d, want 5 (tokens.transfer_count)", got)
	}
}

func TestRetention_ExplicitBlockTargets(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	h.prune(2)

	_, err := h.client.BatchGetBlockTransactionCounts(ctx, &indexerv1.BatchGetBlockTransactionCountsRequest{BlockNumbers: []uint64{4, 2}})
	requireCode(t, err, codes.OutOfRange)
	counts, err := h.client.BatchGetBlockTransactionCounts(ctx, &indexerv1.BatchGetBlockTransactionCountsRequest{BlockNumbers: []uint64{3, 4}})
	if err != nil {
		t.Fatalf("BatchGetBlockTransactionCounts retained: %v", err)
	}
	if counts.GetCounts()[3] != 1 || counts.GetCounts()[4] != 1 {
		t.Errorf("counts = %v, want 3:1 4:1", counts.GetCounts())
	}

	_, err = h.client.ListBlocks(ctx, &indexerv1.ListBlocksRequest{BeforeNumber: 3})
	requireCode(t, err, codes.OutOfRange)
	blocks, err := h.client.ListBlocks(ctx, &indexerv1.ListBlocksRequest{BeforeNumber: 4})
	if err != nil {
		t.Fatalf("ListBlocks before 4: %v", err)
	}
	if len(blocks.GetBlocks()) != 1 || blocks.GetBlocks()[0].GetNumber() != 3 {
		t.Errorf("ListBlocks before 4 = %v, want [3]", blocks.GetBlocks())
	}

	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{
		Filter: &indexerv1.ListTransactionsRequest_ByBlock{ByBlock: &indexerv1.ListTransactionsRequest_BlockFilter{
			Selector: &indexerv1.ListTransactionsRequest_BlockFilter_Number{Number: 1},
		}},
	})
	requireCode(t, err, codes.OutOfRange)

	_, err = h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{
		Filter: &indexerv1.ListTransactionsRequest_ByAddress{ByAddress: &indexerv1.ListTransactionsRequest_AddressFilter{
			Address: "0xaaa0000000000000000000000000000000000001",
		}},
		BlockRange: &indexerv1.BlockRange{ToBlock: 3},
	})
	requireCode(t, err, codes.OutOfRange)

	// A feed that reaches the watermark just ends.
	feed, err := h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Page: &indexerv1.PageRequest{PageSize: 10}})
	if err != nil {
		t.Fatalf("ListTransactions feed: %v", err)
	}
	if len(feed.GetTransactions()) != 3 {
		t.Errorf("feed rows = %d, want 3", len(feed.GetTransactions()))
	}
}
