package indexerv2

import (
	"context"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/protobuf/proto"
)

func validateAddressPair(a, b *pb.AddressSet) error {
	if e := addresses(a); e != nil {
		return e
	}
	return addresses(b)
}
func validateHolder(r *pb.ListHoldersRequest) error {
	if r == nil || len(r.TokenAddress) != 20 {
		return invalid("INVALID_FILTER")
	}
	return addresses(r.Addresses)
}
func (s *Service) ListBlocks(ctx context.Context, r *pb.ListBlocksRequest) (*pb.ListBlocksResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "blocks", page: r.Page, blocks: r.BlockRange, times: nil, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListBlocksResponse{Page: p}
	for _, m := range rows {
		out.Blocks = append(out.Blocks, m.(*pb.Block))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListTransactions(ctx context.Context, r *pb.ListTransactionsRequest) (*pb.ListTransactionsResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateTx(r.Filter); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "transactions", page: r.Page, blocks: r.GetFilter().GetBlockRange(), times: r.GetFilter().GetTimeRange(), consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}
	q.match = func(m proto.Message) bool { return matchesTx(r.Filter, m.(*pb.Transaction)) }
	if h := r.GetFilter().GetTransactionHash(); h != nil {
		q.where = "r.hash=@1@"
		q.args = []any{hexString(h)}
	}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListTransactionsResponse{Page: p}
	for _, m := range rows {
		out.Transactions = append(out.Transactions, m.(*pb.Transaction))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListLogs(ctx context.Context, r *pb.ListLogsRequest) (*pb.ListLogsResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateLog(r.Filter); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "logs", page: r.Page, blocks: r.GetFilter().GetBlockRange(), times: nil, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}
	q.match = func(m proto.Message) bool { return matchesLog(r.Filter, m.(*pb.Log)) }
	if h := r.GetFilter().GetTransactionHash(); h != nil {
		q.where = "r.tx_hash=@1@"
		q.args = []any{hexString(h)}
	}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListLogsResponse{Page: p}
	for _, m := range rows {
		out.Logs = append(out.Logs, m.(*pb.Log))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListTokenTransfers(ctx context.Context, r *pb.ListTokenTransfersRequest) (*pb.ListTokenTransfersResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateTransfer(r.Filter); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "transfers", page: r.Page, blocks: r.GetFilter().GetBlockRange(), times: nil, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}
	q.match = func(m proto.Message) bool { return matchesTransfer(r.Filter, m.(*pb.TokenTransfer)) }
	if h := r.GetFilter().GetTransactionHash(); h != nil {
		q.where = "r.tx_hash=@1@"
		q.args = []any{hexString(h)}
	}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListTokenTransfersResponse{Page: p}
	for _, m := range rows {
		out.Transfers = append(out.Transfers, m.(*pb.TokenTransfer))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListInternalTransactions(ctx context.Context, r *pb.ListInternalTransactionsRequest) (*pb.ListInternalTransactionsResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateInternal(r.Filter); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "internals", page: r.Page, blocks: r.GetFilter().GetBlockRange(), times: nil, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}
	q.match = func(m proto.Message) bool { return matchesInternal(r.Filter, m.(*pb.InternalTransaction)) }
	if h := r.GetFilter().GetTransactionHash(); h != nil {
		q.where = "r.tx_hash=@1@"
		q.args = []any{hexString(h)}
	}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListInternalTransactionsResponse{Page: p}
	for _, m := range rows {
		out.InternalTransactions = append(out.InternalTransactions, m.(*pb.InternalTransaction))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListContracts(ctx context.Context, r *pb.ListContractsRequest) (*pb.ListContractsResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := addresses(r.Addresses); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "contracts", page: r.Page, blocks: r.BlockRange, times: nil, consistency: pb.Consistency_CONSISTENCY_CURRENT}
	q.match = func(m proto.Message) bool {
		x := m.(*pb.Contract)
		return addressMatch(r.Addresses, x.Key.Address) && (r.Verified == nil || *r.Verified == x.GetVerified())
	}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListContractsResponse{Page: p}
	for _, m := range rows {
		out.Contracts = append(out.Contracts, m.(*pb.Contract))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListTokens(ctx context.Context, r *pb.ListTokensRequest) (*pb.ListTokensResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := addresses(r.Addresses); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "tokens", page: r.Page, blocks: r.BlockRange, times: nil, consistency: pb.Consistency_CONSISTENCY_CURRENT}
	q.match = func(m proto.Message) bool {
		x := m.(*pb.Token)
		return addressMatch(r.Addresses, x.Key.Address) && (r.TokenType == "" || r.TokenType == x.TokenType) && (r.Search == "" || containsFold(x.Symbol, r.Search) || containsFold(x.GetName(), r.Search) || containsFold(hexString(x.Key.Address), r.Search))
	}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListTokensResponse{Page: p}
	for _, m := range rows {
		out.Tokens = append(out.Tokens, m.(*pb.Token))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListAddresses(ctx context.Context, r *pb.ListAddressesRequest) (*pb.ListAddressesResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := addresses(r.Addresses); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "addresses", page: r.Page, blocks: nil, times: nil, consistency: pb.Consistency_CONSISTENCY_CURRENT}
	q.match = func(m proto.Message) bool {
		x := m.(*pb.Address)
		return addressMatch(r.Addresses, x.Key.Address) && (r.ContractsOnly == nil || *r.ContractsOnly == x.GetIsContract())
	}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListAddressesResponse{Page: p}
	for _, m := range rows {
		out.Addresses = append(out.Addresses, m.(*pb.Address))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListBalances(ctx context.Context, r *pb.ListBalancesRequest) (*pb.ListBalancesResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateAddressPair(r.Addresses, r.Tokens); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "balances", page: r.Page, blocks: nil, times: nil, consistency: pb.Consistency_CONSISTENCY_CURRENT}
	q.match = func(m proto.Message) bool {
		x := m.(*pb.Balance)
		return addressMatch(r.Addresses, x.Key.Address) && addressMatch(r.Tokens, x.Key.TokenAddress)
	}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListBalancesResponse{Page: p}
	for _, m := range rows {
		out.Balances = append(out.Balances, m.(*pb.Balance))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListHolders(ctx context.Context, r *pb.ListHoldersRequest) (*pb.ListHoldersResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if e := validateHolder(r); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{family: "holders", page: r.Page, blocks: nil, times: nil, consistency: pb.Consistency_CONSISTENCY_CURRENT}
	q.match = func(m proto.Message) bool { return addressMatch(r.Addresses, m.(*pb.Holder).Key.Address) }
	q.where = `r.token_address=@1@`
	q.args = []any{hexString(r.TokenAddress)}
	rows, p, e := v.scan(q, false)
	if e != nil {
		return nil, e
	}
	out := &pb.ListHoldersResponse{Page: p}
	for _, m := range rows {
		out.Holders = append(out.Holders, m.(*pb.Holder))
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
