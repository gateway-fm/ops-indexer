package grpcserver

import (
	"context"
	"log/slog"

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
	limit := int(s.clampPageSize(req.GetPage().GetPageSize()))

	var fromAddr, toAddr *string
	if req.GetByFrom() != "" {
		v := req.GetByFrom()
		fromAddr = &v
	}
	if req.GetByTo() != "" {
		v := req.GetByTo()
		toAddr = &v
	}
	var l1From, l1To *uint64
	if br := req.GetL1BlockRange(); br != nil {
		if br.GetFromBlock() > 0 {
			v := br.GetFromBlock()
			l1From = &v
		}
		if br.GetToBlock() > 0 {
			v := br.GetToBlock()
			l1To = &v
		}
	}

	var afterL1Block uint64
	if c := req.GetPage().GetCursor(); c != "" {
		var cur blockFeedCursor // reuse: block_number-keyed cursor
		if err := decodeCursor(c, &cur); err != nil {
			return nil, err
		}
		afterL1Block = cur.BlockNumber
	}

	rows, err := s.db.ListOPDeposits(ctx, fromAddr, toAddr, l1From, l1To, afterL1Block, limit+1)
	if err != nil {
		slog.Error("ListOPDeposits", "error", err)
		return nil, internalErr(err, "ListOPDeposits")
	}

	nextCursor := ""
	if len(rows) > limit {
		last := rows[limit-1]
		if enc, e := encodeCursor(blockFeedCursor{BlockNumber: last.L1BlockNumber}); e == nil {
			nextCursor = enc
		}
		rows = rows[:limit]
	}

	out := make([]*indexerv1.OPDeposit, 0, len(rows))
	for i := range rows {
		out = append(out, mapOPDeposit(&rows[i]))
	}
	return &indexerv1.ListOPDepositsResponse{
		Deposits: out,
		Page:     &indexerv1.PageResponse{NextCursor: nextCursor},
	}, nil
}
