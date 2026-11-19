package grpcserver

import (
	"context"
	"sort"
	"strconv"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/nodelookup"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

// The existing optional archive-node fallback remains a single-block query.
// It uses complete keys and applies the same supported V1 filter semantics.
func (s *Server) v1NodeTransactions(ctx context.Context, b *nodelookup.Block, f db.V1TransactionFilter, in db.V1PageInput, binding string) (*indexerv1.ListTransactionsResponse, error) {
	source, err := s.db.V1PageSource(ctx)
	if err != nil {
		return nil, v1PagingError(err, "ListTransactions")
	}
	source += ":" + b.Block.Hash
	if in.Position.Source != "" && in.Position.Source != source {
		return nil, invalidArgument("cursor does not match this source; restart pagination")
	}
	var index int64 = -1
	hash := ""
	if len(in.Position.Key) > 0 {
		if len(in.Position.Key) != 2 {
			return nil, invalidArgument("cursor malformed")
		}
		index, err = strconv.ParseInt(in.Position.Key[0], 10, 32)
		if err != nil || index < 0 {
			return nil, invalidArgument("cursor malformed")
		}
		hash = in.Position.Key[1]
	}
	if f.CategoryMask != 0 {
		return nil, invalidArgument("category filtering is not supported by the archive-node fallback")
	}
	rows := make([]types.Transaction, 0, len(b.Txs))
	for _, tx := range b.Txs {
		hidden := false
		for _, kind := range s.db.HiddenTxTypes {
			if tx.TxType == kind {
				hidden = true
				break
			}
		}
		if hidden {
			continue
		}
		if f.From > 0 && tx.BlockNumber < f.From || f.To > 0 && tx.BlockNumber >= f.To || f.TimeFrom != nil && int64(tx.BlockTimestamp) < *f.TimeFrom || f.TimeTo != nil && int64(tx.BlockTimestamp) >= *f.TimeTo || f.Status != nil && tx.Status != *f.Status {
			continue
		}
		if int64(tx.TxIndex) < index || int64(tx.TxIndex) == index && tx.Hash <= hash {
			continue
		}
		rows = append(rows, tx)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TxIndex != rows[j].TxIndex {
			return rows[i].TxIndex < rows[j].TxIndex
		}
		return rows[i].Hash < rows[j].Hash
	})
	var next *db.V1PagePosition
	if len(rows) > in.Limit {
		rows = rows[:in.Limit]
		last := rows[len(rows)-1]
		next = &db.V1PagePosition{Key: []string{strconv.Itoa(last.TxIndex), last.Hash}, Source: source, Block: last.BlockNumber, Page: max(1, in.Position.Page+1)}
	}
	servedFromNode(ctx)
	return &indexerv1.ListTransactionsResponse{Transactions: mapTransactions(rows), Page: &indexerv1.PageResponse{NextCursor: v1Next(binding, next)}}, nil
}
