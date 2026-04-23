package grpcserver

import (
	"context"
	"log/slog"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

func (s *Server) ListLogs(ctx context.Context, req *indexerv1.ListLogsRequest) (*indexerv1.ListLogsResponse, error) {
	limit := int(s.clampPageSize(req.GetPage().GetPageSize()))

	hasTxHash := req.GetByTxHash() != ""
	hasAddress := req.GetByAddress() != ""
	hasTopic := req.GetTopic0() != ""

	if !hasTxHash && !hasAddress && !hasTopic {
		return nil, invalidArgument("at least one of by_tx_hash, by_address, topic0 must be set")
	}

	var rows []types.Log
	var err error
	// Dispatch to the narrowest existing db query.
	switch {
	case hasTxHash:
		rows, err = s.db.GetLogsByTransaction(ctx, req.GetByTxHash())
	case hasAddress && !hasTopic:
		// db.GetLogsByAddress uses offset pagination; we adapt by ignoring offset
		// for the first cursor-less page and returning no next cursor. A
		// follow-up DB change introduces proper cursor semantics for this path.
		var rs []types.Log
		rs, _, err = s.db.GetLogsByAddress(ctx, strings.ToLower(req.GetByAddress()), limit, 0)
		rows = rs
	case hasTopic && !hasAddress:
		var rs []types.Log
		rs, _, err = s.db.GetLogsByTopic(ctx, req.GetTopic0(), limit, 0)
		rows = rs
	default:
		// Multi-filter branch: use the flexible GetLogs.
		var addr, topic *string
		if hasAddress {
			a := strings.ToLower(req.GetByAddress())
			addr = &a
		}
		if hasTopic {
			t := req.GetTopic0()
			topic = &t
		}
		var from, to *uint64
		if br := req.GetBlockRange(); br != nil {
			if br.GetFromBlock() > 0 {
				f := br.GetFromBlock()
				from = &f
			}
			if br.GetToBlock() > 0 {
				t := br.GetToBlock()
				to = &t
			}
		}
		rows, err = s.db.GetLogs(ctx, addr, topic, from, to, limit)
	}
	if err != nil {
		slog.Error("ListLogs", "error", err)
		return nil, internalErr(err, "ListLogs")
	}

	return &indexerv1.ListLogsResponse{
		Logs: mapLogs(rows),
		Page: &indexerv1.PageResponse{}, // cursor pagination for logs pending DB support
	}, nil
}
