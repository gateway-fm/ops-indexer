package grpcserver

import (
	"context"
	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"strings"
)

func (s *Server) ListLogs(ctx context.Context, req *indexerv1.ListLogsRequest) (*indexerv1.ListLogsResponse, error) {
	f := db.V1LogFilter{TxHash: strings.ToLower(req.ByTxHash), Address: strings.ToLower(req.ByAddress), Topic: strings.ToLower(req.Topic0)}
	if f.TxHash == "" && f.Address == "" && f.Topic == "" {
		return nil, invalidArgument("at least one filter must be set")
	}
	var err error
	f.From, f.To, err = v1BlockRange(req.BlockRange)
	if err != nil {
		return nil, err
	}
	in, binding, err := s.v1Input("logs", f, req.Page, nil)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Logs(ctx, f, in)
	if err != nil {
		return nil, v1PagingError(err, "ListLogs")
	}
	return &indexerv1.ListLogsResponse{Logs: mapLogs(result.Rows), Page: &indexerv1.PageResponse{NextCursor: v1Next(binding, result.Next)}, TotalCount: result.Total}, nil
}
