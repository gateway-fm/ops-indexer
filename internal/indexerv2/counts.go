package indexerv2

import (
	"context"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/protobuf/proto"
)

func (s *Service) count(ctx context.Context, r proto.Message, read *pb.ReadContext, q query) (*pb.CountResponse, error) {
	v, e := s.begin(ctx, r, read, nil)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q.consistency = pb.Consistency_CONSISTENCY_SNAPSHOT
	q.aggregate = true
	rows, p, e := v.scan(q, true)
	if e != nil {
		return nil, countError(v.ctx, e)
	}
	out := &pb.CountResponse{Page: p, Count: uint64(len(rows))}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) CountTransactions(ctx context.Context, r *pb.CountTransactionsRequest) (*pb.CountResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateTx(r.Filter); e != nil {
		return nil, e
	}
	q := query{family: "transactions", blocks: r.GetFilter().GetBlockRange(), times: r.GetFilter().GetTimeRange(), match: func(m proto.Message) bool { return matchesTx(r.Filter, m.(*pb.Transaction)) }}
	if h := r.GetFilter().GetTransactionHash(); h != nil {
		q.where = "r.hash=@1@"
		q.args = []any{hexString(h)}
	}
	return s.count(ctx, r, r.Read, q)
}
func (s *Service) CountLogs(ctx context.Context, r *pb.CountLogsRequest) (*pb.CountResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateLog(r.Filter); e != nil {
		return nil, e
	}
	q := query{family: "logs", blocks: r.GetFilter().GetBlockRange(), match: func(m proto.Message) bool { return matchesLog(r.Filter, m.(*pb.Log)) }}
	if h := r.GetFilter().GetTransactionHash(); h != nil {
		q.where = "r.tx_hash=@1@"
		q.args = []any{hexString(h)}
	}
	return s.count(ctx, r, r.Read, q)
}
func (s *Service) CountTokenTransfers(ctx context.Context, r *pb.CountTokenTransfersRequest) (*pb.CountResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateTransfer(r.Filter); e != nil {
		return nil, e
	}
	q := query{family: "transfers", blocks: r.GetFilter().GetBlockRange(), match: func(m proto.Message) bool { return matchesTransfer(r.Filter, m.(*pb.TokenTransfer)) }}
	q.where, q.args = transferSourceWhere(r.Filter)
	return s.count(ctx, r, r.Read, q)
}
