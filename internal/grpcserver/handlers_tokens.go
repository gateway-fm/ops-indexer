package grpcserver

import (
	"context"
	"log/slog"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

func (s *Server) GetToken(ctx context.Context, req *indexerv1.GetTokenRequest) (*indexerv1.Token, error) {
	if req.GetAddress() == "" {
		return nil, invalidArgument("address is required")
	}
	tok, err := s.db.GetToken(ctx, strings.ToLower(req.GetAddress()))
	if err != nil {
		slog.Error("GetToken", "error", err)
		return nil, internalErr(err, "GetToken")
	}
	if tok == nil {
		return nil, notFound("token", req.GetAddress())
	}
	return mapToken(tok), nil
}

func (s *Server) ListTokens(ctx context.Context, req *indexerv1.ListTokensRequest) (*indexerv1.ListTokensResponse, error) {
	page := int(req.GetPage().GetPage())
	if page < 1 {
		page = 1
	}
	pageSize := int(s.clampPageSize(req.GetPage().GetPageSize()))
	offset := (page - 1) * pageSize

	tokenType := ""
	switch req.GetTokenType() {
	case indexerv1.TokenType_TOKEN_TYPE_ERC20:
		tokenType = "ERC20"
	case indexerv1.TokenType_TOKEN_TYPE_ERC721:
		tokenType = "ERC721"
	case indexerv1.TokenType_TOKEN_TYPE_ERC1155:
		tokenType = "ERC1155"
	}

	rows, total, err := s.db.GetTokens(ctx, pageSize, offset, tokenType, req.GetSearch())
	if err != nil {
		slog.Error("ListTokens", "error", err)
		return nil, internalErr(err, "ListTokens")
	}
	return &indexerv1.ListTokensResponse{
		Tokens: mapTokens(rows),
		Page: &indexerv1.OffsetPageResponse{
			Page:       int32(page),
			PageSize:   int32(pageSize),
			TotalItems: total,
		},
	}, nil
}

func (s *Server) ListTokenTransfers(ctx context.Context, req *indexerv1.ListTokenTransfersRequest) (*indexerv1.ListTokenTransfersResponse, error) {
	limit := int(s.clampPageSize(req.GetPage().GetPageSize()))

	hasTx := req.GetByTxHash() != ""
	hasAddr := req.GetByAddress() != ""
	hasTok := req.GetByToken() != ""
	if !hasTx && !hasAddr && !hasTok {
		return nil, invalidArgument("at least one of by_tx_hash, by_address, by_token must be set")
	}

	var rows []types.TokenTransfer
	var err error
	switch {
	case hasTx:
		rows, err = s.db.GetTransfersByTransaction(ctx, req.GetByTxHash())
	case hasAddr:
		var beforeBlock *uint64
		if c := req.GetPage().GetCursor(); c != "" {
			var cur transferFeedCursor
			if derr := decodeCursor(c, &cur); derr != nil {
				return nil, derr
			}
			beforeBlock = &cur.BlockNumber
		}
		rows, err = s.db.GetTransfersByAddress(ctx, strings.ToLower(req.GetByAddress()), limit+1, beforeBlock)
	case hasTok:
		var rs []types.TokenTransfer
		rs, _, err = s.db.GetTransfersByToken(ctx, strings.ToLower(req.GetByToken()), limit, 0)
		rows = rs
	}
	if err != nil {
		slog.Error("ListTokenTransfers", "error", err)
		return nil, internalErr(err, "ListTokenTransfers")
	}

	nextCursor := ""
	if hasAddr && len(rows) > limit {
		last := rows[limit-1]
		if enc, e := encodeCursor(transferFeedCursor{BlockNumber: last.BlockNumber, LogIndex: uint32(last.LogIndex)}); e == nil {
			nextCursor = enc
		}
		rows = rows[:limit]
	}

	return &indexerv1.ListTokenTransfersResponse{
		Transfers: mapTokenTransfers(rows),
		Page:      &indexerv1.PageResponse{NextCursor: nextCursor},
	}, nil
}

func (s *Server) ListTokenHolders(ctx context.Context, req *indexerv1.ListTokenHoldersRequest) (*indexerv1.ListTokenHoldersResponse, error) {
	if req.GetTokenAddress() == "" {
		return nil, invalidArgument("token_address is required")
	}
	page := int(req.GetPage().GetPage())
	if page < 1 {
		page = 1
	}
	pageSize := int(s.clampPageSize(req.GetPage().GetPageSize()))
	offset := (page - 1) * pageSize

	rows, total, err := s.db.GetTokenHolders(ctx, strings.ToLower(req.GetTokenAddress()), pageSize, offset)
	if err != nil {
		slog.Error("ListTokenHolders", "error", err)
		return nil, internalErr(err, "ListTokenHolders")
	}
	// Holders from the DB don't carry token_address; set it from the request context.
	holders := mapHolders(rows)
	for _, h := range holders {
		h.TokenAddress = strings.ToLower(req.GetTokenAddress())
	}
	return &indexerv1.ListTokenHoldersResponse{
		Holders: holders,
		Page: &indexerv1.OffsetPageResponse{
			Page:       int32(page),
			PageSize:   int32(pageSize),
			TotalItems: total,
		},
	}, nil
}

func (s *Server) ListTokenBalances(ctx context.Context, req *indexerv1.ListTokenBalancesRequest) (*indexerv1.ListTokenBalancesResponse, error) {
	if req.GetAddress() == "" {
		return nil, invalidArgument("address is required")
	}
	addr := strings.ToLower(req.GetAddress())
	if req.GetTokenAddress() != "" {
		bal, err := s.db.GetLatestBalance(ctx, addr, strings.ToLower(req.GetTokenAddress()))
		if err != nil {
			slog.Error("ListTokenBalances(one)", "error", err)
			return nil, internalErr(err, "ListTokenBalances")
		}
		if bal == nil {
			return &indexerv1.ListTokenBalancesResponse{Page: &indexerv1.PageResponse{}}, nil
		}
		return &indexerv1.ListTokenBalancesResponse{
			Balances: []*indexerv1.TokenBalance{mapBalance(bal)},
			Page:     &indexerv1.PageResponse{},
		}, nil
	}
	rows, err := s.db.GetTokenBalances(ctx, addr)
	if err != nil {
		slog.Error("ListTokenBalances", "error", err)
		return nil, internalErr(err, "ListTokenBalances")
	}
	return &indexerv1.ListTokenBalancesResponse{
		Balances: mapBalances(rows),
		Page:     &indexerv1.PageResponse{}, // balance list is bounded per-address; no cursor needed yet
	}, nil
}

func (s *Server) BatchGetTokenBalances(
	ctx context.Context,
	req *indexerv1.BatchGetTokenBalancesRequest,
) (*indexerv1.BatchGetTokenBalancesResponse, error) {
	addrs := req.GetAddresses()
	if len(addrs) == 0 {
		return &indexerv1.BatchGetTokenBalancesResponse{
			Balances: map[string]*indexerv1.BatchGetTokenBalancesResponse_TokenBalanceList{},
		}, nil
	}
	grouped, err := s.db.BatchGetTokenBalances(ctx, addrs, req.GetTokenAddress())
	if err != nil {
		slog.Error("BatchGetTokenBalances", "error", err)
		return nil, internalErr(err, "BatchGetTokenBalances")
	}
	out := make(map[string]*indexerv1.BatchGetTokenBalancesResponse_TokenBalanceList, len(grouped))
	for addr, balances := range grouped {
		items := make([]*indexerv1.TokenBalance, 0, len(balances))
		for i := range balances {
			items = append(items, mapBalance(&balances[i]))
		}
		out[addr] = &indexerv1.BatchGetTokenBalancesResponse_TokenBalanceList{Items: items}
	}
	// Return an empty list for addresses that have no balances so clients get
	// a full map matching their input.
	for _, a := range addrs {
		lk := strings.ToLower(a)
		if _, ok := out[lk]; !ok {
			out[lk] = &indexerv1.BatchGetTokenBalancesResponse_TokenBalanceList{}
		}
	}
	return &indexerv1.BatchGetTokenBalancesResponse{Balances: out}, nil
}
