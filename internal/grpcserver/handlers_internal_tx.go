package grpcserver

import (
	"context"
	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"strings"
)

func (s *Server) ListInternalTransactions(ctx context.Context, req *indexerv1.ListInternalTransactionsRequest) (*indexerv1.ListInternalTransactionsResponse, error) {
	f := db.V1InternalFilter{TxHash: strings.ToLower(req.ByTxHash), Address: strings.ToLower(req.ByAddress)}
	if req.ByBlock != nil {
		switch b := req.ByBlock.GetSelector().(type) {
		case *indexerv1.ListInternalTransactionsRequest_BlockFilter_Number:
			f.Block = &b.Number
		case *indexerv1.ListInternalTransactionsRequest_BlockFilter_Hash:
			f.Hash = strings.ToLower(b.Hash)
			if f.Hash == "" {
				return nil, invalidArgument("block hash is required")
			}

		default:
			return nil, invalidArgument("by_block selector required")
		}
	}
	if f.TxHash == "" && f.Address == "" && f.Block == nil && f.Hash == "" {
		return nil, invalidArgument("at least one filter must be set")
	}
	if f.Block != nil && *f.Block > uint64(^uint64(0)>>1) {
		return nil, invalidArgument("block number exceeds supported integer range")
	}
	in, binding, err := s.v1Input("internal-transactions", f, req.Page, nil)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Internals(ctx, f, in)
	if err != nil {
		return nil, v1PagingError(err, "ListInternalTransactions")
	}
	return &indexerv1.ListInternalTransactionsResponse{InternalTransactions: mapInternalTxs(result.Rows), Page: &indexerv1.PageResponse{NextCursor: v1Next(binding, result.Next)}, TotalCount: result.Total}, nil
}
