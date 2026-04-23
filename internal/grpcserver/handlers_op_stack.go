package grpcserver

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) GetOPDeposit(ctx context.Context, req *indexerv1.GetOPDepositRequest) (*indexerv1.OPDeposit, error) {
	if !s.cfg.OPStackEnabled {
		return nil, unavailable("OP-Stack endpoints are not enabled for this chain")
	}
	_ = ctx
	_ = req
	// TODO: db has no GetOPDeposit query yet; port from block-explorer's
	// op_deposits.go read paths.
	return nil, status.Error(codes.Unimplemented, "GetOPDeposit pending db support")
}

func (s *Server) ListOPDeposits(ctx context.Context, req *indexerv1.ListOPDepositsRequest) (*indexerv1.ListOPDepositsResponse, error) {
	if !s.cfg.OPStackEnabled {
		return nil, unavailable("OP-Stack endpoints are not enabled for this chain")
	}
	_ = ctx
	_ = req
	return nil, status.Error(codes.Unimplemented, "ListOPDeposits pending db support")
}
