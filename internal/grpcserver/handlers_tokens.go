package grpcserver

import (
	"context"
	"log/slog"
	"math/big"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/db"
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

func v1TokenType(t indexerv1.TokenType) (string, error) {
	switch t {
	case indexerv1.TokenType_TOKEN_TYPE_UNSPECIFIED:
		return "", nil
	case indexerv1.TokenType_TOKEN_TYPE_ERC20:
		return "ERC20", nil
	case indexerv1.TokenType_TOKEN_TYPE_ERC721:
		return "ERC721", nil
	case indexerv1.TokenType_TOKEN_TYPE_ERC1155:
		return "ERC1155", nil
	}
	return "", invalidArgument("unsupported token type")
}
func (s *Server) ListTokens(ctx context.Context, req *indexerv1.ListTokensRequest) (*indexerv1.ListTokensResponse, error) {
	tokenType, err := v1TokenType(req.TokenType)
	if err != nil {
		return nil, err
	}
	search := strings.ToLower(strings.TrimSpace(req.Search))
	f := struct{ Type, Search string }{tokenType, search}
	in, binding, err := s.v1Input("tokens", f, nil, req.Page)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Tokens(ctx, tokenType, search, in)
	if err != nil {
		return nil, v1PagingError(err, "ListTokens")
	}
	return &indexerv1.ListTokensResponse{Tokens: mapTokens(result.Rows), Page: v1Offset(binding, result.Next, result.Page, in.Limit, result.Total)}, nil
}
func (s *Server) ListTokenTransfers(ctx context.Context, req *indexerv1.ListTokenTransfersRequest) (*indexerv1.ListTokenTransfersResponse, error) {
	f := db.V1TransferFilter{TxHash: strings.ToLower(req.ByTxHash), Address: strings.ToLower(req.ByAddress), Token: strings.ToLower(req.ByToken), Cumulative: true}
	if f.TxHash == "" && f.Address == "" && f.Token == "" {
		return nil, invalidArgument("at least one filter must be set")
	}
	var err error
	f.From, f.To, err = v1BlockRange(req.BlockRange)
	if err != nil {
		return nil, err
	}
	in, binding, err := s.v1Input("token-transfers", f, req.Page, nil)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Transfers(ctx, f, in)
	if err != nil {
		return nil, v1PagingError(err, "ListTokenTransfers")
	}
	return &indexerv1.ListTokenTransfersResponse{Transfers: mapTokenTransfers(result.Rows), Page: &indexerv1.PageResponse{NextCursor: v1Next(binding, result.Next)}, TotalCount: result.Total}, nil
}
func (s *Server) ListAllTokenTransfers(ctx context.Context, req *indexerv1.ListAllTokenTransfersRequest) (*indexerv1.ListAllTokenTransfersResponse, error) {
	tokenType, err := v1TokenType(req.TokenType)
	if err != nil {
		return nil, err
	}
	f := db.V1TransferFilter{TokenType: tokenType, Cumulative: true}
	in, binding, err := s.v1Input("all-token-transfers", f, nil, req.Page)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Transfers(ctx, f, in)
	if err != nil {
		return nil, v1PagingError(err, "ListAllTokenTransfers")
	}
	return &indexerv1.ListAllTokenTransfersResponse{Transfers: mapTokenTransfers(result.Rows), Page: v1Offset(binding, result.Next, result.Page, in.Limit, result.Total)}, nil
}
func (s *Server) ListTokenHolders(ctx context.Context, req *indexerv1.ListTokenHoldersRequest) (*indexerv1.ListTokenHoldersResponse, error) {
	token := strings.ToLower(req.TokenAddress)
	if token == "" {
		return nil, invalidArgument("token_address is required")
	}
	in, binding, err := s.v1Input("token-holders", token, nil, req.Page)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Holders(ctx, token, in)
	if err != nil {
		return nil, v1PagingError(err, "ListTokenHolders")
	}
	holders := mapHolders(result.Rows)
	for _, h := range holders {
		h.TokenAddress = token
	}
	return &indexerv1.ListTokenHoldersResponse{Holders: holders, Page: v1Offset(binding, result.Next, result.Page, in.Limit, result.Total)}, nil
}
func (s *Server) ListTokenInventory(ctx context.Context, req *indexerv1.ListTokenInventoryRequest) (*indexerv1.ListTokenInventoryResponse, error) {
	token := strings.ToLower(req.TokenAddress)
	if token == "" {
		return nil, invalidArgument("token_address is required")
	}
	id := req.TokenId
	if id != "" {
		n, ok := new(big.Int).SetString(id, 10)
		if !ok || n.Sign() < 0 {
			return nil, invalidArgument("token_id must be an unsigned decimal integer")
		}
		id = n.String()
	}
	f := struct{ Token, ID string }{token, id}
	in, binding, err := s.v1Input("token-inventory", f, nil, req.Page)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Inventory(ctx, token, id, in)
	if err != nil {
		return nil, v1PagingError(err, "ListTokenInventory")
	}
	return &indexerv1.ListTokenInventoryResponse{Items: mapInventoryItems(result.Rows), Page: v1Offset(binding, result.Next, result.Page, in.Limit, result.Total)}, nil
}

func mapInventoryItems(rows []types.TokenInventoryItem) []*indexerv1.TokenInventoryItem {
	out := make([]*indexerv1.TokenInventoryItem, 0, len(rows))
	for _, r := range rows {
		uri := ""
		if r.TokenURI != nil {
			uri = *r.TokenURI
		}
		out = append(out, &indexerv1.TokenInventoryItem{
			TokenId:  bigIntFromString(r.TokenID),
			Owner:    r.Owner,
			TokenUri: uri,
		})
	}
	return out
}

func (s *Server) ListTokenBalances(ctx context.Context, req *indexerv1.ListTokenBalancesRequest) (*indexerv1.ListTokenBalancesResponse, error) {
	address, token := strings.ToLower(req.Address), strings.ToLower(req.TokenAddress)
	if address == "" {
		return nil, invalidArgument("address is required")
	}
	f := struct{ Address, Token string }{address, token}
	in, binding, err := s.v1Input("token-balances", f, req.Page, nil)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Balances(ctx, address, token, in)
	if err != nil {
		return nil, v1PagingError(err, "ListTokenBalances")
	}
	return &indexerv1.ListTokenBalancesResponse{Balances: mapBalances(result.Rows), Page: &indexerv1.PageResponse{NextCursor: v1Next(binding, result.Next)}}, nil
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
