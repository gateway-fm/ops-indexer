package grpcserver

import (
	"context"
	"log/slog"
	"strings"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/nodelookup"
)

func (s *Server) GetTransaction(ctx context.Context, req *indexerv1.GetTransactionRequest) (*indexerv1.Transaction, error) {
	if req.GetHash() == "" {
		return nil, invalidArgument("hash is required")
	}
	// Use the category-aware variant so responses include the category bitfield.
	tx, err := s.db.GetTransactionWithCategories(ctx, req.GetHash())
	if err != nil {
		slog.Error("GetTransaction", "error", err)
		return nil, internalErr(err, "GetTransaction")
	}
	if tx == nil {
		if through, ok, err := s.prunedThroughForNode(ctx); err != nil {
			return nil, err
		} else if ok {
			ntx, err := s.cfg.NodeLookup.Transaction(ctx, req.GetHash(), through)
			if err != nil {
				return nil, nodeLookupErr(err, "transaction")
			}
			if ntx != nil {
				servedFromNode(ctx)
				return mapTransaction(ntx), nil
			}
		}
		return nil, s.notFoundByHash(ctx, "transaction", req.GetHash())
	}
	return mapTransaction(tx), nil
}

func (s *Server) ListTransactions(ctx context.Context, req *indexerv1.ListTransactionsRequest) (*indexerv1.ListTransactionsResponse, error) {
	f := db.V1TransactionFilter{}
	var archive *nodelookup.Block
	var err error
	f.From, f.To, err = v1BlockRange(req.GetBlockRange())
	if err != nil {
		return nil, err
	}
	switch by := req.GetFilter().(type) {
	case *indexerv1.ListTransactionsRequest_ByAddress:
		if by.ByAddress.GetAddress() == "" {
			return nil, invalidArgument("address is required")
		}
		f.Address = strings.ToLower(by.ByAddress.Address)
		switch by.ByAddress.Direction {
		case indexerv1.AddressDirection_ADDRESS_DIRECTION_UNSPECIFIED, indexerv1.AddressDirection_ADDRESS_DIRECTION_BOTH:
			f.Direction = "both"
		case indexerv1.AddressDirection_ADDRESS_DIRECTION_FROM:
			f.Direction = "from"
		case indexerv1.AddressDirection_ADDRESS_DIRECTION_TO:
			f.Direction = "to"
		default:
			return nil, invalidArgument("unsupported address direction")
		}
	case *indexerv1.ListTransactionsRequest_ByBlock:
		switch b := by.ByBlock.GetSelector().(type) {
		case *indexerv1.ListTransactionsRequest_BlockFilter_Number:
			f.Block = &b.Number
			if s.cfg.NodeLookup != nil {
				if _, _, err = s.refreshPrunedThrough(ctx); err != nil {
					return nil, err
				}
				archive, err = s.prunedBlockByNumber(ctx, b.Number)
				if err != nil {
					return nil, err
				}
			}
		case *indexerv1.ListTransactionsRequest_BlockFilter_Hash:
			f.Hash = strings.ToLower(b.Hash)
			if f.Hash == "" {
				return nil, invalidArgument("block hash is required")
			}
			if s.cfg.NodeLookup != nil {
				if _, _, err = s.refreshPrunedThrough(ctx); err != nil {
					return nil, err
				}
				archive, err = s.prunedBlockByHash(ctx, f.Hash)
				if err != nil {
					return nil, err
				}
			}

		default:
			return nil, invalidArgument("block selector is required")
		}
	}
	for _, cat := range req.CategoryIn {
		switch cat {
		case indexerv1.TransactionCategory_TRANSACTION_CATEGORY_COIN_TRANSFER:
			f.CategoryMask |= 1
		case indexerv1.TransactionCategory_TRANSACTION_CATEGORY_CONTRACT_CREATION:
			f.CategoryMask |= 2
		case indexerv1.TransactionCategory_TRANSACTION_CATEGORY_CONTRACT_CALL:
			f.CategoryMask |= 4
		case indexerv1.TransactionCategory_TRANSACTION_CATEGORY_TOKEN_TRANSFER:
			f.CategoryMask |= 8
		default:
			return nil, invalidArgument("unsupported category")
		}
	}
	switch req.Status {
	case indexerv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED:
	case indexerv1.TransactionStatus_TRANSACTION_STATUS_SUCCESS:
		v := 1
		f.Status = &v
	case indexerv1.TransactionStatus_TRANSACTION_STATUS_FAILED:
		v := 0
		f.Status = &v
	default:
		return nil, invalidArgument("unsupported status")
	}
	if tr := req.TimeRange; tr != nil {
		if tr.Start != nil {
			if e := tr.Start.CheckValid(); e != nil {
				return nil, invalidArgument("invalid time_range.start")
			}
			v := tr.Start.Seconds
			f.TimeFrom = &v
			if tr.Start.Nanos != 0 {
				v++
				f.TimeFrom = &v
			}
		}
		if tr.End != nil {
			if e := tr.End.CheckValid(); e != nil {
				return nil, invalidArgument("invalid time_range.end")
			}
			v := tr.End.Seconds
			if tr.End.Nanos != 0 {
				v++
			}
			f.TimeTo = &v
		}
		if tr.Start != nil && tr.End != nil && !tr.Start.AsTime().Before(tr.End.AsTime()) {
			return nil, invalidArgument("time_range must be nonempty")
		}
	}
	if f.Block != nil && *f.Block > uint64(^uint64(0)>>1) {
		return nil, invalidArgument("block number exceeds supported integer range")
	}
	in, binding, err := s.v1Input("transactions", f, req.Page, nil)
	if err != nil {
		return nil, err
	}
	if archive != nil {
		return s.v1NodeTransactions(ctx, archive, f, in, binding)
	}
	result, err := s.db.V1Transactions(ctx, f, in)
	if err != nil {
		return nil, v1PagingError(err, "ListTransactions")
	}
	return &indexerv1.ListTransactionsResponse{Transactions: mapTransactions(result.Rows), Page: &indexerv1.PageResponse{NextCursor: v1Next(binding, result.Next)}}, nil
}
func (s *Server) ListTransactionsPaginated(ctx context.Context, req *indexerv1.ListTransactionsPaginatedRequest) (*indexerv1.ListTransactionsPaginatedResponse, error) {
	f := db.V1TransactionFilter{GlobalTotal: true}
	in, binding, err := s.v1Input("transactions-paginated", f, nil, req.Page)
	if err != nil {
		return nil, err
	}
	result, err := s.db.V1Transactions(ctx, f, in)
	if err != nil {
		return nil, v1PagingError(err, "ListTransactionsPaginated")
	}
	return &indexerv1.ListTransactionsPaginatedResponse{Transactions: mapTransactions(result.Rows), Page: v1Offset(binding, result.Next, result.Page, in.Limit, result.Total)}, nil
}

func (s *Server) BatchGetAddressTransactionCounts(
	ctx context.Context,
	req *indexerv1.BatchGetAddressTransactionCountsRequest,
) (*indexerv1.BatchGetAddressTransactionCountsResponse, error) {
	addrs := req.GetAddresses()
	if len(addrs) == 0 {
		return &indexerv1.BatchGetAddressTransactionCountsResponse{Counts: map[string]uint64{}}, nil
	}
	counts, err := s.db.BatchGetAddressTransactionCounts(ctx, addrs)
	if err != nil {
		slog.Error("BatchGetAddressTransactionCounts", "error", err)
		return nil, internalErr(err, "BatchGetAddressTransactionCounts")
	}
	// Return 0 for addresses that don't exist in address_stats so callers
	// get a full map matching their input.
	for _, a := range addrs {
		lk := strings.ToLower(a)
		if _, ok := counts[lk]; !ok {
			counts[lk] = 0
		}
	}
	return &indexerv1.BatchGetAddressTransactionCountsResponse{Counts: counts}, nil
}
