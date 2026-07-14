package grpcserver

import (
	"context"
	"log/slog"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

func (s *Server) GetTransaction(ctx context.Context, req *indexerv1.GetTransactionRequest) (*indexerv1.Transaction, error) {
	if req.GetHash() == "" {
		return nil, invalidArgument("hash is required")
	}
	// Use the category-aware variant so responses include the category bitfield.
	tx, err := s.db.GetTransactionWithCategories(ctx, req.GetHash())
	if err != nil {
		slog.Error("GetTransaction", "error", err)
		return nil, internalErr(err, "GetTransaction")
	}
	if tx == nil {
		return nil, notFound("transaction", req.GetHash())
	}
	return mapTransaction(tx), nil
}

func (s *Server) ListTransactions(ctx context.Context, req *indexerv1.ListTransactionsRequest) (*indexerv1.ListTransactionsResponse, error) {
	limit := s.clampPageSize(req.GetPage().GetPageSize())

	// Filter dispatch.
	switch f := req.GetFilter().(type) {
	case *indexerv1.ListTransactionsRequest_ByAddress:
		if f.ByAddress.GetAddress() == "" {
			return nil, invalidArgument("address is required")
		}
		addr := strings.ToLower(f.ByAddress.GetAddress())
		// Position priority: opaque cursor (full (block, tx_index) keyset —
		// RD-1148: resuming mid-block must not skip the block's remaining
		// rows), else block_range.to_block as an inclusive whole-block upper
		// bound (lets callers that page by block number, e.g. the privacy
		// proxy's legacy ?before=, position without knowing the cursor
		// encoding).
		var before *db.AddressFeedBound
		if c := req.GetPage().GetCursor(); c != "" {
			var cur txFeedCursor
			if err := decodeCursor(c, &cur); err != nil {
				return nil, err
			}
			before = &db.AddressFeedBound{Block: cur.BlockNumber, Index: &cur.TransactionIndex}
		} else if tb := req.GetBlockRange().GetToBlock(); tb > 0 {
			before = &db.AddressFeedBound{Block: tb, Inclusive: true}
		}
		rows, err := s.db.GetTransactionsByAddress(ctx, addr, int(limit)+1, before)
		if err != nil {
			slog.Error("ListTransactions by address", "error", err)
			return nil, internalErr(err, "ListTransactions")
		}
		return buildTxListResponse(rows, limit), nil

	case *indexerv1.ListTransactionsRequest_ByBlock:
		switch b := f.ByBlock.GetSelector().(type) {
		case *indexerv1.ListTransactionsRequest_BlockFilter_Number:
			rows, err := s.db.GetTransactionsByBlock(ctx, b.Number)
			if err != nil {
				slog.Error("ListTransactions by block", "error", err)
				return nil, internalErr(err, "ListTransactions")
			}
			return &indexerv1.ListTransactionsResponse{
				Transactions: mapTransactions(rows),
				Page:         &indexerv1.PageResponse{},
			}, nil
		case *indexerv1.ListTransactionsRequest_BlockFilter_Hash:
			// Lookup block by hash, then fetch its transactions by number.
			blk, err := s.db.GetBlockByHash(ctx, b.Hash)
			if err != nil {
				slog.Error("ListTransactions by block hash", "error", err)
				return nil, internalErr(err, "ListTransactions")
			}
			if blk == nil {
				return nil, notFound("block", b.Hash)
			}
			rows, err := s.db.GetTransactionsByBlock(ctx, blk.Number)
			if err != nil {
				return nil, internalErr(err, "ListTransactions")
			}
			return &indexerv1.ListTransactionsResponse{
				Transactions: mapTransactions(rows),
				Page:         &indexerv1.PageResponse{},
			}, nil
		}
		return nil, invalidArgument("block filter: selector is required")

	case nil:
		// No filter — main transaction feed.
		var beforeBlock *uint64
		if c := req.GetPage().GetCursor(); c != "" {
			var cur txFeedCursor
			if err := decodeCursor(c, &cur); err != nil {
				return nil, err
			}
			beforeBlock = &cur.BlockNumber
		}
		rows, err := s.db.GetTransactionsWithCategories(ctx, int(limit)+1, beforeBlock)
		if err != nil {
			slog.Error("ListTransactions feed", "error", err)
			return nil, internalErr(err, "ListTransactions")
		}
		return buildTxListResponse(rows, limit), nil
	}
	return nil, invalidArgument("unsupported filter type")
}

// ListTransactionsPaginated serves the global tx feed with offset pagination
// and a true chain-wide total, for browse-style UIs that show "page X of Y".
func (s *Server) ListTransactionsPaginated(ctx context.Context, req *indexerv1.ListTransactionsPaginatedRequest) (*indexerv1.ListTransactionsPaginatedResponse, error) {
	page := int(req.GetPage().GetPage())
	if page < 1 {
		page = 1
	}
	pageSize := int(s.clampPageSize(req.GetPage().GetPageSize()))

	rows, total, err := s.db.GetTransactionsPaginatedWithCategories(ctx, page, pageSize)
	if err != nil {
		slog.Error("ListTransactionsPaginated", "error", err)
		return nil, internalErr(err, "ListTransactionsPaginated")
	}
	return &indexerv1.ListTransactionsPaginatedResponse{
		Transactions: mapTransactions(rows),
		Page: &indexerv1.OffsetPageResponse{
			Page:       int32(page),
			PageSize:   int32(pageSize),
			TotalItems: total,
		},
	}, nil
}

// buildTxListResponse applies the limit+1 cursor encoding pattern.
// If len(rows) > limit, encode a cursor pointing at the last returned row
// and trim rows to the requested page size.
func buildTxListResponse(rows []types.Transaction, limit int32) *indexerv1.ListTransactionsResponse {
	nextCursor := ""
	if int32(len(rows)) > limit {
		last := rows[limit-1]
		enc, err := encodeCursor(txFeedCursor{
			BlockNumber:      last.BlockNumber,
			TransactionIndex: uint32(last.TxIndex),
		})
		if err == nil {
			nextCursor = enc
		}
		rows = rows[:limit]
	}
	return &indexerv1.ListTransactionsResponse{
		Transactions: mapTransactions(rows),
		Page:         &indexerv1.PageResponse{NextCursor: nextCursor},
	}
}

func (s *Server) BatchGetAddressTransactionCounts(
	ctx context.Context,
	req *indexerv1.BatchGetAddressTransactionCountsRequest,
) (*indexerv1.BatchGetAddressTransactionCountsResponse, error) {
	addrs := req.GetAddresses()
	if len(addrs) == 0 {
		return &indexerv1.BatchGetAddressTransactionCountsResponse{Counts: map[string]uint64{}}, nil
	}
	counts, err := s.db.BatchGetAddressTransactionCounts(ctx, addrs)
	if err != nil {
		slog.Error("BatchGetAddressTransactionCounts", "error", err)
		return nil, internalErr(err, "BatchGetAddressTransactionCounts")
	}
	// Return 0 for addresses that don't exist in address_stats so callers
	// get a full map matching their input.
	for _, a := range addrs {
		lk := strings.ToLower(a)
		if _, ok := counts[lk]; !ok {
			counts[lk] = 0
		}
	}
	return &indexerv1.BatchGetAddressTransactionCountsResponse{Counts: counts}, nil
}
