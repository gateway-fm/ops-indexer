package grpcserver

import (
	"context"
	"log/slog"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) GetOPDeposit(ctx context.Context, req *indexerv1.GetOPDepositRequest) (*indexerv1.OPDeposit, error) {
	if !s.cfg.OPStackEnabled {
		return nil, unavailable("OP-Stack endpoints are not enabled for this chain")
	}
	switch sel := req.GetSelector().(type) {
	case *indexerv1.GetOPDepositRequest_L1TransactionHash:
		if sel.L1TransactionHash == "" {
			return nil, invalidArgument("l1_transaction_hash is required")
		}
		dep, err := s.db.GetOPDepositByL1Hash(ctx, sel.L1TransactionHash)
		if err != nil {
			slog.Error("GetOPDeposit by L1 hash", "error", err)
			return nil, internalErr(err, "GetOPDeposit")
		}
		if dep == nil {
			return nil, notFound("op_deposit", sel.L1TransactionHash)
		}
		return mapOPDeposit(dep), nil
	case *indexerv1.GetOPDepositRequest_L2TransactionHash:
		if sel.L2TransactionHash == "" {
			return nil, invalidArgument("l2_transaction_hash is required")
		}
		dep, err := s.db.GetOPDeposit(ctx, sel.L2TransactionHash)
		if err != nil {
			slog.Error("GetOPDeposit by L2 hash", "error", err)
			return nil, internalErr(err, "GetOPDeposit")
		}
		if dep == nil {
			return nil, notFound("op_deposit", sel.L2TransactionHash)
		}
		return mapOPDeposit(dep), nil
	default:
		return nil, invalidArgument("selector must be l1_transaction_hash or l2_transaction_hash")
	}
}

func (s *Server) ListOPDeposits(ctx context.Context, req *indexerv1.ListOPDepositsRequest) (*indexerv1.ListOPDepositsResponse, error) {
	if !s.cfg.OPStackEnabled {
		return nil, unavailable("OP-Stack endpoints are not enabled for this chain")
	}
	if req.ByTo != "" {
		return nil, invalidArgument("by_to is not supported for OP deposits; use by_from")
	}
	lower, upper, err := v1BlockRange(req.L1BlockRange)
	if err != nil {
		return nil, err
	}
	f := struct {
		From         string
		Lower, Upper uint64
	}{strings.ToLower(req.ByFrom), lower, upper}
	in, binding, err := s.v1Input("op-deposits", f, req.Page, nil)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Deposits(ctx, f.From, lower, upper, in)
	if err != nil {
		return nil, v1PagingError(err, "ListOPDeposits")
	}
	out := make([]*indexerv1.OPDeposit, 0, len(result.Rows))
	for i := range result.Rows {
		out = append(out, mapOPDeposit(&result.Rows[i]))
	}
	return &indexerv1.ListOPDepositsResponse{Deposits: out, Page: &indexerv1.PageResponse{NextCursor: v1Next(binding, result.Next)}}, nil
}
