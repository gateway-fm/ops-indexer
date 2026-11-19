package indexerv2

import (
	"context"
	"encoding/hex"
	"errors"
	"math"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
)

func (s *Service) GetChainView(ctx context.Context, r *pb.GetChainViewRequest) (*pb.GetChainViewResponse, error) {
	v, e := s.begin(ctx, r, r.GetRead(), nil)
	if e != nil {
		return nil, e
	}
	defer v.close()
	var finalized, verified *int64
	var syncing *bool
	e = v.tx.QueryRow(v.ctx, `SELECT last_finalized_block,last_verified_block,is_syncing FROM sync_status WHERE id=1`).Scan(&finalized, &verified, &syncing)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return nil, mapError(v.ctx, e)
	}
	out := &pb.GetChainViewResponse{Page: v.page(pb.Consistency_CONSISTENCY_SNAPSHOT), ChainId: v.chainID, GenesisHash: v.genesis, SafeHead: v.anchor, SafeHeadTimestamp: v.anchorTime, LatestIndexed: v.head, LowestIndexed: v.lowest, BackfillComplete: v.runFloor <= v.history}
	if finalized != nil {
		if *finalized < 0 {
			return nil, dataError()
		}
		n := uint64(*finalized)
		out.LastFinalized = &n
	}
	if verified != nil {
		if *verified < 0 {
			return nil, dataError()
		}
		n := uint64(*verified)
		out.LastVerified = &n
	}
	if syncing != nil {
		out.IsSyncing = *syncing
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) GetBlock(ctx context.Context, r *pb.GetBlockRequest) (*pb.GetBlockResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	q := query{family: "blocks", retain: true, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}
	var number *uint64
	var hash string
	switch x := r.Selector.(type) {
	case *pb.GetBlockRequest_Number:
		if x.Number > math.MaxInt64 {
			return nil, invalid("INVALID_FILTER")
		}
		n := x.Number
		number = &n
		q.where = "r.number=@1@"
		q.args = []any{int64(n)}
	case *pb.GetBlockRequest_Hash:
		if len(x.Hash) != 32 {
			return nil, invalid("INVALID_FILTER")
		}
		hash = hexString(x.Hash)
		q.where = "r.hash=@1@"
		q.args = []any{hash}
	default:
		return nil, invalid("INVALID_FILTER")
	}
	v, e := s.begin(ctx, r, r.Read, nil)
	if e != nil {
		return nil, e
	}
	defer v.close()
	if number != nil && *number < v.history && s.lookup == nil {
		return nil, v.pruned()
	}
	rows, p, e := v.scan(q, true)
	if e != nil {
		return nil, e
	}
	if len(rows) > 1 {
		return nil, dataError()
	}
	out := &pb.GetBlockResponse{Page: p, Source: pb.Source_SOURCE_INDEXER}
	if len(rows) == 1 {
		out.Block = rows[0].(*pb.Block)
	} else if s.lookup != nil && v.history > 0 {
		var nodeBlock *pb.Block
		if number != nil && *number < v.history {
			b, e := s.lookup.BlockByNumber(v.ctx, *number)
			if e != nil {
				return nil, mapError(v.ctx, e)
			}
			if b != nil {
				nodeBlock, e = nativeBlock(b.Block)
				if e != nil {
					return nil, e
				}
			}
		} else if hash != "" {
			b, e := s.lookup.BlockByHash(v.ctx, hash, v.history-1)
			if e != nil {
				return nil, mapError(v.ctx, e)
			}
			if b != nil {
				nodeBlock, e = nativeBlock(b.Block)
				if e != nil {
					return nil, e
				}
			}
		}
		if nodeBlock != nil {
			out.Block = nodeBlock
			out.Source = pb.Source_SOURCE_NODE
		}
	}
	if out.Block == nil {
		return nil, failure(codes.NotFound, "OBJECT_NOT_FOUND")
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) GetTransaction(ctx context.Context, r *pb.GetTransactionRequest) (*pb.GetTransactionResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if len(r.Hash) != 32 {
		return nil, invalid("INVALID_FILTER")
	}
	v, e := s.begin(ctx, r, r.Read, nil)
	if e != nil {
		return nil, e
	}
	defer v.close()
	rows, p, e := v.scan(query{family: "transactions", where: "r.hash=@1@", args: []any{hexString(r.Hash)}, retain: true, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}, true)
	if e != nil {
		return nil, e
	}
	if len(rows) > 1 {
		return nil, dataError()
	}
	out := &pb.GetTransactionResponse{Page: p, Source: pb.Source_SOURCE_INDEXER}
	if len(rows) == 1 {
		out.Transaction = rows[0].(*pb.Transaction)
	} else if s.lookup != nil && v.history > 0 {
		t, e := s.lookup.Transaction(v.ctx, "0x"+hex.EncodeToString(r.Hash), v.history-1)
		if e != nil {
			return nil, mapError(v.ctx, e)
		}
		if t != nil {
			out.Transaction, e = nativeTransaction(t)
			if e != nil {
				return nil, e
			}
			out.Source = pb.Source_SOURCE_NODE
		}
	}
	if out.Transaction == nil {
		return nil, failure(codes.NotFound, "OBJECT_NOT_FOUND")
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
