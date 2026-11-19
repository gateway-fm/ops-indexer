package indexerv2

import (
	"bytes"
	"context"
	"sort"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func (s *Service) BatchFirstActivity(ctx context.Context, r *pb.BatchFirstActivityRequest) (*pb.BatchFirstActivityResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if r.Targets == nil {
		return nil, invalid("INVALID_FILTER")
	}
	if e := addresses(r.Targets); e != nil {
		return nil, e
	}
	if e := validateTx(r.Filter); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, nil)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{aggregate: true, family: "transactions", page: &pb.PageRequest{Order: pb.Order_ORDER_ASCENDING}, blocks: r.GetFilter().GetBlockRange(), times: r.GetFilter().GetTimeRange(), retain: true, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT, match: func(m proto.Message) bool {
		t := m.(*pb.Transaction)
		return addressMatch(r.Targets, t.From, t.To) && matchesTx(r.Filter, t)
	}}
	rows, p, e := v.scan(q, true)
	if e != nil {
		return nil, e
	}
	witnesses := make(map[string]*pb.Transaction, len(r.Targets.Addresses))
	targets := make(map[string]struct{}, len(r.Targets.Addresses))
	for _, a := range r.Targets.Addresses {
		targets[string(a)] = struct{}{}
	}
	for _, m := range rows {
		t := m.(*pb.Transaction)
		for _, a := range [][]byte{t.From, t.To} {
			k := string(a)
			if _, ok := targets[k]; ok && witnesses[k] == nil {
				witnesses[k] = t
			}
		}
	}
	out := &pb.BatchFirstActivityResponse{Page: p}
	for _, a := range r.Targets.Addresses {
		out.Activities = append(out.Activities, &pb.FirstActivity{Target: a, Transaction: witnesses[string(a)]})
	}
	if proto.Size(out) > maxSend {
		return nil, budget()
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) ListCounterparties(ctx context.Context, r *pb.ListCounterpartiesRequest) (*pb.ListCounterpartiesResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if r.Targets == nil {
		return nil, invalid("INVALID_FILTER")
	}
	if e := addresses(r.Targets); e != nil {
		return nil, e
	}
	if e := validateRange(r.BlockRange, r.TimeRange); e != nil {
		return nil, e
	}
	v, e := s.begin(ctx, r, r.Read, nil)
	if e != nil {
		return nil, e
	}
	defer v.close()
	q := query{aggregate: true, family: "transactions", blocks: r.BlockRange, times: r.TimeRange, retain: true, consistency: pb.Consistency_CONSISTENCY_CURRENT, match: func(m proto.Message) bool {
		t := m.(*pb.Transaction)
		return addressMatch(r.Targets, t.From, t.To) && matchesTx(&pb.TransactionFilter{TimeRange: r.TimeRange}, t)
	}}
	rows, p, e := v.scan(q, true)
	if e != nil {
		return nil, e
	}
	var first, last *uint64
	for _, m := range rows {
		n := m.(*pb.Transaction).Key.BlockNumber
		if first == nil {
			a, b := n, n
			first = &a
			last = &b
		} else {
			*first = min(*first, n)
			*last = max(*last, n)
		}
	}
	dest := map[string]*pb.Counterparty{}
	creations := map[string]*pb.Transaction{}
	add := func(a []byte, n uint64) error {
		key := string(a)
		c := dest[key]
		if c == nil {
			if len(dest) >= maxSet {
				return budget()
			}
			c = &pb.Counterparty{Key: &pb.AddressKey{Address: a}, FirstBlock: n, LastBlock: n}
			dest[key] = c
		} else {
			c.FirstBlock = min(c.FirstBlock, n)
			c.LastBlock = max(c.LastBlock, n)
		}
		return nil
	}
	for _, m := range rows {
		t := m.(*pb.Transaction)
		if t.To != nil {
			if e = add(t.To, t.Key.BlockNumber); e != nil {
				return nil, e
			}
		} else {
			creations[hexString(t.Key.Hash)] = t
		}
	}
	if len(creations) > 0 {
		hs := make([]string, 0, len(creations))
		for h := range creations {
			hs = append(hs, h)
		}
		cr, e := v.tx.Query(v.ctx, `SELECT left(address,43),creation_tx,block_number FROM contracts WHERE creation_tx=ANY($1::text[]) ORDER BY address COLLATE "C" LIMIT 20001`, hs)
		if e != nil {
			return nil, mapError(v.ctx, e)
		}
		n := 0
		for cr.Next() {
			if e := v.ctx.Err(); e != nil {
				cr.Close()
				return nil, mapError(v.ctx, e)
			}
			n++
			v.scanned++
			if n > maxRows || v.scanned > maxRows {
				cr.Close()
				return nil, budget()
			}
			var a, h string
			var b int64
			if e = cr.Scan(&a, &h, &b); e != nil {
				cr.Close()
				return nil, mapError(v.ctx, e)
			}
			t := creations[h]
			if t == nil || b < 0 || uint64(b) != t.Key.BlockNumber {
				continue
			}
			address, e := decodeHex(a, 20)
			if e != nil {
				cr.Close()
				return nil, e
			}
			if e = add(address, uint64(b)); e != nil {
				cr.Close()
				return nil, e
			}
		}
		e = cr.Err()
		cr.Close()
		if e != nil {
			return nil, mapError(v.ctx, e)
		}
	}
	out := &pb.ListCounterpartiesResponse{Page: p, FirstBlock: first, LastBlock: last}
	for _, d := range dest {
		out.Counterparties = append(out.Counterparties, d)
	}
	sort.Slice(out.Counterparties, func(i, j int) bool {
		return bytes.Compare(out.Counterparties[i].Key.Address, out.Counterparties[j].Key.Address) < 0
	})
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Service) BatchGetTransactions(ctx context.Context, r *pb.BatchGetTransactionsRequest) (*pb.BatchGetTransactionsResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if len(r.Hashes) > 256 {
		return nil, invalid("FILTER_SET_TOO_LARGE")
	}
	hs := make([]string, 0, len(r.Hashes))
	for _, h := range r.Hashes {
		if len(h) != 32 {
			return nil, invalid("INVALID_FILTER")
		}
		hs = append(hs, hexString(h))
	}
	v, e := s.begin(ctx, r, r.Read, nil)
	if e != nil {
		return nil, e
	}
	defer v.close()
	parents, p, e := v.scan(query{family: "transactions", where: "r.hash=ANY(@1@::text[])", args: []any{hs}, retain: true, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}, true)
	if e != nil {
		return nil, e
	}
	byHash := make(map[string]*pb.TransactionBundle, len(parents))
	for _, m := range parents {
		t := m.(*pb.Transaction)
		h := string(t.Key.Hash)
		if byHash[h] != nil {
			return nil, dataError()
		}
		byHash[h] = &pb.TransactionBundle{Transaction: t}
	}
	for _, h := range r.Hashes {
		if byHash[string(h)] == nil {
			return nil, failure(codes.NotFound, "OBJECT_NOT_FOUND")
		}
	}
	blocks := make([]int64, 0, len(byHash))
	seenBlocks := map[uint64]bool{}
	for _, b := range byHash {
		n := b.Transaction.Key.BlockNumber
		if !seenBlocks[n] {
			blocks = append(blocks, int64(n))
			seenBlocks[n] = true
		}
	}
	for _, family := range []string{"logs", "transfers", "internals"} {
		rows, _, e := v.scan(query{family: family, where: "r.tx_hash=ANY(@1@::text[]) AND r.block_number=ANY(@2@::bigint[])", args: []any{hs, blocks}, retain: true, consistency: pb.Consistency_CONSISTENCY_SNAPSHOT}, true)
		if e != nil {
			if statusResource(e) {
				return nil, failure(codes.ResourceExhausted, "CHILD_LIST_TOO_LARGE")
			}
			return nil, e
		}
		for _, m := range rows {
			var h []byte
			var number uint64
			switch t := m.(type) {
			case *pb.Log:
				h = t.TransactionHash
				number = t.Key.BlockNumber
			case *pb.TokenTransfer:
				h = t.TransactionHash
				number = t.Key.BlockNumber
			case *pb.InternalTransaction:
				h = t.Key.TransactionHash
				number = t.Key.BlockNumber
			}
			b := byHash[string(h)]
			if b == nil || b.Transaction.Key.BlockNumber != number {
				return nil, dataError()
			}
			switch t := m.(type) {
			case *pb.Log:
				b.Logs = append(b.Logs, t)
			case *pb.TokenTransfer:
				b.Transfers = append(b.Transfers, t)
			case *pb.InternalTransaction:
				b.InternalTransactions = append(b.InternalTransactions, t)
			}
		}
	}
	out := &pb.BatchGetTransactionsResponse{Page: p}
	total := 0
	for _, h := range r.Hashes {
		b := byHash[string(h)]
		total += 1 + len(b.Logs) + len(b.Transfers) + len(b.InternalTransactions)
		if total > maxRows {
			return nil, failure(codes.ResourceExhausted, "CHILD_LIST_TOO_LARGE")
		}
		out.Transactions = append(out.Transactions, b)
	}
	if proto.Size(out) > maxSend {
		return nil, failure(codes.ResourceExhausted, "CHILD_LIST_TOO_LARGE")
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
