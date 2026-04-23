package grpcserver

import (
	"context"
	"log/slog"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) GetContract(ctx context.Context, req *indexerv1.GetContractRequest) (*indexerv1.Contract, error) {
	if req.GetAddress() == "" {
		return nil, invalidArgument("address is required")
	}
	c, err := s.db.GetContract(ctx, strings.ToLower(req.GetAddress()))
	if err != nil {
		slog.Error("GetContract", "error", err)
		return nil, internalErr(err, "GetContract")
	}
	if c == nil {
		return nil, notFound("contract", req.GetAddress())
	}
	// mapContract intentionally drops ABI, source, verified. See docs/API.md.
	return mapContract(c), nil
}
