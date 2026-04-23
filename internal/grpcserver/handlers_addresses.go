package grpcserver

import (
	"context"
	"log/slog"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) GetAddress(ctx context.Context, req *indexerv1.GetAddressRequest) (*indexerv1.Address, error) {
	if req.GetAddress() == "" {
		return nil, invalidArgument("address is required")
	}
	a, err := s.db.GetAddressStats(ctx, strings.ToLower(req.GetAddress()))
	if err != nil {
		slog.Error("GetAddress", "error", err)
		return nil, internalErr(err, "GetAddress")
	}
	if a == nil {
		return nil, notFound("address", req.GetAddress())
	}
	return mapAddressStats(a), nil
}

func (s *Server) ListAddresses(ctx context.Context, req *indexerv1.ListAddressesRequest) (*indexerv1.ListAddressesResponse, error) {
	page := int(req.GetPage().GetPage())
	if page < 1 {
		page = 1
	}
	pageSize := int(s.clampPageSize(req.GetPage().GetPageSize()))

	rows, total, err := s.db.GetAccountsPaginated(ctx, page, pageSize)
	if err != nil {
		slog.Error("ListAddresses", "error", err)
		return nil, internalErr(err, "ListAddresses")
	}
	return &indexerv1.ListAddressesResponse{
		Addresses: mapAddressStatsList(rows),
		Page: &indexerv1.OffsetPageResponse{
			Page:       int32(page),
			PageSize:   int32(pageSize),
			TotalItems: total,
		},
	}, nil
}
