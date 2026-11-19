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
	switch req.SortBy {
	case indexerv1.ListAddressesRequest_SORT_BY_UNSPECIFIED, indexerv1.ListAddressesRequest_SORT_BY_TX_COUNT:
	default:
		return nil, invalidArgument("native-balance address sorting is not supported")
	}
	in, binding, err := s.v1Input("addresses", "tx-count-desc,address-asc", nil, req.Page)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Addresses(ctx, in)
	if err != nil {
		return nil, v1PagingError(err, "ListAddresses")
	}
	return &indexerv1.ListAddressesResponse{Addresses: mapAddressStatsList(result.Rows), Page: v1Offset(binding, result.Next, result.Page, in.Limit, result.Total)}, nil
}
