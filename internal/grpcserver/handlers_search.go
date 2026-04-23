package grpcserver

import (
	"context"
	"log/slog"
	"strconv"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) Search(ctx context.Context, req *indexerv1.SearchRequest) (*indexerv1.SearchResponse, error) {
	if req.GetQuery() == "" {
		return nil, invalidArgument("query is required")
	}
	perKind := int(req.GetLimitPerKind())
	if perKind <= 0 {
		perKind = 5
	}
	if perKind > 20 {
		perKind = 20
	}

	rows, err := s.db.SearchSuggestions(ctx, req.GetQuery(), perKind)
	if err != nil {
		slog.Error("Search", "error", err)
		return nil, internalErr(err, "Search")
	}

	results := make([]*indexerv1.SearchResponse_SearchResult, 0, len(rows))
	for _, r := range rows {
		res := &indexerv1.SearchResponse_SearchResult{}
		switch r.Type {
		case "block":
			if num, perr := strconv.ParseUint(r.Value, 10, 64); perr == nil {
				res.Kind = indexerv1.SearchResponse_SEARCH_RESULT_KIND_BLOCK
				res.Item = &indexerv1.SearchResponse_SearchResult_Block{
					Block: &indexerv1.Block{Number: num},
				}
			} else {
				continue
			}
		case "transaction":
			res.Kind = indexerv1.SearchResponse_SEARCH_RESULT_KIND_TRANSACTION
			res.Item = &indexerv1.SearchResponse_SearchResult_Transaction{
				Transaction: &indexerv1.Transaction{Hash: r.Value},
			}
		case "address":
			res.Kind = indexerv1.SearchResponse_SEARCH_RESULT_KIND_ADDRESS
			res.Item = &indexerv1.SearchResponse_SearchResult_Address{
				Address: &indexerv1.Address{Address: r.Value},
			}
		case "token":
			res.Kind = indexerv1.SearchResponse_SEARCH_RESULT_KIND_TOKEN
			res.Item = &indexerv1.SearchResponse_SearchResult_Token{
				Token: &indexerv1.Token{Address: r.Value, Symbol: stripSymbol(r.Label)},
			}
		default:
			continue
		}
		results = append(results, res)
	}
	return &indexerv1.SearchResponse{Results: results}, nil
}

// stripSymbol extracts the leading symbol from a token label of the form
// "SYMBOL" or "SYMBOL (Name)". Falls back to the whole label.
func stripSymbol(label string) string {
	for i, c := range label {
		if c == ' ' {
			return label[:i]
		}
	}
	return label
}
