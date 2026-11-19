package grpcserver

import (
	"context"
	"log/slog"
	"slices"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) GetBlock(ctx context.Context, req *indexerv1.GetBlockRequest) (*indexerv1.Block, error) {
	switch sel := req.GetSelector().(type) {
	case *indexerv1.GetBlockRequest_Number:
		if nb, err := s.prunedBlockByNumber(ctx, sel.Number); err != nil || nb != nil {
			if err != nil {
				return nil, err
			}
			return mapBlock(nb.Block), nil
		}
		if err := s.checkRetained(ctx, sel.Number); err != nil {
			return nil, err
		}
		b, err := s.db.GetBlock(ctx, sel.Number)
		if err != nil {
			slog.Error("GetBlock by number", "error", err)
			return nil, internalErr(err, "GetBlock")
		}
		if b == nil {
			if err := s.recheckRetained(ctx, sel.Number); err != nil {
				return nil, err
			}
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
			if nb, err := s.prunedBlockByHash(ctx, sel.Hash); err != nil || nb != nil {
				if err != nil {
					return nil, err
				}
				return mapBlock(nb.Block), nil
			}
			return nil, s.notFoundByHash(ctx, "block", sel.Hash)
		}
		return mapBlock(b), nil
	default:
		return nil, invalidArgument("selector must be number or hash")
	}
}

func (s *Server) ListBlocks(ctx context.Context, req *indexerv1.ListBlocksRequest) (*indexerv1.ListBlocksResponse, error) {
	if req.BeforeNumber > uint64(^uint64(0)>>1) {
		return nil, invalidArgument("block bound exceeds supported integer range")
	}
	in, binding, err := s.v1Input("blocks", req.BeforeNumber, req.Page, nil)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Blocks(ctx, req.BeforeNumber, in)
	if err != nil {
		return nil, v1PagingError(err, "ListBlocks")
	}
	return &indexerv1.ListBlocksResponse{Blocks: mapBlocks(result.Rows), Page: &indexerv1.PageResponse{NextCursor: v1Next(binding, result.Next)}}, nil
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
	nums := req.GetBlockNumbers()
	if len(nums) == 0 {
		return &indexerv1.BatchGetBlockTransactionCountsResponse{Counts: map[uint64]uint32{}}, nil
	}
	oldest := slices.Min(nums)
	if err := s.checkRetained(ctx, oldest); err != nil {
		return nil, err
	}
	counts, err := s.db.BatchGetBlockTransactionCounts(ctx, nums)
	if err != nil {
		slog.Error("BatchGetBlockTransactionCounts", "error", err)
		return nil, internalErr(err, "BatchGetBlockTransactionCounts")
	}
	if _, ok := counts[oldest]; !ok {
		if err := s.recheckRetained(ctx, oldest); err != nil {
			return nil, err
		}
	}
	// Missing blocks: return 0 so clients get a full map.
	for _, n := range nums {
		if _, ok := counts[n]; !ok {
			counts[n] = 0
		}
	}
	return &indexerv1.BatchGetBlockTransactionCountsResponse{Counts: counts}, nil
}
