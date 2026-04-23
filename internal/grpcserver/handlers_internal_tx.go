package grpcserver

import (
	"context"
	"log/slog"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

func (s *Server) ListInternalTransactions(ctx context.Context, req *indexerv1.ListInternalTransactionsRequest) (*indexerv1.ListInternalTransactionsResponse, error) {
	limit := int(s.clampPageSize(req.GetPage().GetPageSize()))

	hasTx := req.GetByTxHash() != ""
	hasAddr := req.GetByAddress() != ""
	hasBlock := req.GetByBlock() != nil
	if !hasTx && !hasAddr && !hasBlock {
		return nil, invalidArgument("at least one of by_tx_hash, by_address, by_block must be set")
	}

	var rows []types.InternalTransaction
	var err error
	switch {
	case hasTx:
		rows, err = s.db.GetInternalTransactionsByTx(ctx, req.GetByTxHash())
	case hasAddr:
		// Offset-based DB method; cursor pagination pending DB support.
		rs, _, derr := s.db.GetInternalTransactionsByAddress(ctx, strings.ToLower(req.GetByAddress()), limit, 0)
		rows, err = rs, derr
	case hasBlock:
		switch b := req.GetByBlock().GetSelector().(type) {
		case *indexerv1.ListInternalTransactionsRequest_BlockFilter_Number:
			rows, err = s.db.GetInternalTransactionsByBlock(ctx, b.Number)
		case *indexerv1.ListInternalTransactionsRequest_BlockFilter_Hash:
			blk, berr := s.db.GetBlockByHash(ctx, b.Hash)
			if berr != nil {
				return nil, internalErr(berr, "ListInternalTransactions")
			}
			if blk == nil {
				return nil, notFound("block", b.Hash)
			}
			rows, err = s.db.GetInternalTransactionsByBlock(ctx, blk.Number)
		default:
			return nil, invalidArgument("by_block selector is required")
		}
	}
	if err != nil {
		slog.Error("ListInternalTransactions", "error", err)
		return nil, internalErr(err, "ListInternalTransactions")
	}

	return &indexerv1.ListInternalTransactionsResponse{
		InternalTransactions: mapInternalTxs(rows),
		Page:                 &indexerv1.PageResponse{}, // cursor pagination pending
	}, nil
}
