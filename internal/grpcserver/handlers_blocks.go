package grpcserver

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) GetBlock(ctx context.Context, req *indexerv1.GetBlockRequest) (*indexerv1.Block, error) {
	switch sel := req.GetSelector().(type) {
	case *indexerv1.GetBlockRequest_Number:
		b, err := s.db.GetBlock(ctx, sel.Number)
		if err != nil {
			slog.Error("GetBlock by number", "error", err)
			return nil, internalErr(err, "GetBlock")
		}
		if b == nil {
			return nil, notFound("block", "")
		}
		return mapBlock(b), nil
	case *indexerv1.GetBlockRequest_Hash:
		if sel.Hash == "" {
			return nil, invalidArgument("hash is required")
		}
		b, err := s.db.GetBlockByHash(ctx, sel.Hash)
		if err != nil {
			slog.Error("GetBlock by hash", "error", err)
			return nil, internalErr(err, "GetBlock")
		}
		if b == nil {
			return nil, notFound("block", sel.Hash)
		}
		return mapBlock(b), nil
	default:
		return nil, invalidArgument("selector must be number or hash")
	}
}

func (s *Server) ListBlocks(ctx context.Context, req *indexerv1.ListBlocksRequest) (*indexerv1.ListBlocksResponse, error) {
	limit := s.clampPageSize(req.GetPage().GetPageSize())

	var beforeBlock *uint64
	if c := req.GetPage().GetCursor(); c != "" {
		var cur blockFeedCursor
		if err := decodeCursor(c, &cur); err != nil {
			return nil, err
		}
		beforeBlock = &cur.BlockNumber
	} else if req.GetBeforeNumber() > 0 {
		b := req.GetBeforeNumber()
		beforeBlock = &b
	}

	// limit+1 trick to detect more pages.
	rows, err := s.db.GetBlocks(ctx, int(limit)+1, beforeBlock)
	if err != nil {
		slog.Error("ListBlocks", "error", err)
		return nil, internalErr(err, "ListBlocks")
	}

	nextCursor := ""
	if int32(len(rows)) > limit {
		last := rows[limit-1]
		enc, err := encodeCursor(blockFeedCursor{BlockNumber: last.Number})
		if err == nil {
			nextCursor = enc
		}
		rows = rows[:limit]
	}

	return &indexerv1.ListBlocksResponse{
		Blocks: mapBlocks(rows),
		Page:   &indexerv1.PageResponse{NextCursor: nextCursor},
	}, nil
}

func (s *Server) GetLatestBlockNumber(ctx context.Context, _ *indexerv1.Empty) (*indexerv1.LatestBlockNumber, error) {
	n, err := s.db.GetLatestBlockNumber(ctx)
	if err != nil {
		slog.Error("GetLatestBlockNumber", "error", err)
		return nil, internalErr(err, "GetLatestBlockNumber")
	}
	return &indexerv1.LatestBlockNumber{Number: n}, nil
}

func (s *Server) BatchGetBlockTransactionCounts(
	ctx context.Context,
	req *indexerv1.BatchGetBlockTransactionCountsRequest,
) (*indexerv1.BatchGetBlockTransactionCountsResponse, error) {
	_ = ctx
	_ = req
	// TODO: add db.BatchGetBlockTransactionCounts(ctx, numbers[]) using
	// `SELECT block_number, COUNT(*) FROM transactions WHERE block_number = ANY($1) GROUP BY block_number`.
	return nil, status.Error(codes.Unimplemented, "BatchGetBlockTransactionCounts pending db support")
}
