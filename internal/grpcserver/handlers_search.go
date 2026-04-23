package grpcserver

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) Search(ctx context.Context, req *indexerv1.SearchRequest) (*indexerv1.SearchResponse, error) {
	_ = ctx
	if req.GetQuery() == "" {
		return nil, invalidArgument("query is required")
	}
	// TODO: port SearchSuggestions from block-explorer's db.queries.go. The
	// function exists there but wasn't copied as part of Phase 2.1 because the
	// return type (SearchSuggestion/Response) is a CLI-friendly shape that
	// needs reshaping into the proto.
	return nil, status.Error(codes.Unimplemented, "Search pending db.SearchSuggestions port")
}
