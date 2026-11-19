package indexerv2

import (
	"fmt"
	"math"
	"math/big"
	"strings"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type query struct {
	family      string
	page        *pb.PageRequest
	blocks      *pb.BlockRange
	times       *pb.TimeRange
	consistency pb.Consistency
	match       func(proto.Message) bool
	where       string
	args        []any
	retain      bool
	aggregate   bool
	searchKind  pb.SearchKind
	onScanned   func(*pb.Key)
}

func keyValues(f string, k *pb.Key) ([]any, error) {
	if k == nil {
		return nil, nil
	}
	bad := func() ([]any, error) { return nil, invalid("INVALID_KEY") }
	block := func(n uint64) bool { return n <= math.MaxInt64 }
	switch f {
	case "blocks":
		x := k.GetBlock()
		if x == nil || !block(x.Number) {
			return bad()
		}
		return []any{int64(x.Number)}, nil
	case "transactions":
		x := k.GetTransaction()
		if x == nil || !block(x.BlockNumber) || len(x.Hash) != 32 {
			return bad()
		}
		return []any{int64(x.BlockNumber), int64(x.TransactionIndex), hexString(x.Hash)}, nil
	case "logs":
		x := k.GetLog()
		if x == nil || !block(x.BlockNumber) {
			return bad()
		}
		return []any{int64(x.BlockNumber), int64(x.LogIndex)}, nil
	case "transfers":
		x := k.GetTransfer()
		if x == nil || !block(x.BlockNumber) {
			return bad()
		}
		return []any{int64(x.BlockNumber), int64(x.LogIndex)}, nil
	case "internals":
		x := k.GetInternal()
		if x == nil || !block(x.BlockNumber) || len(x.TransactionHash) != 32 || len(x.TraceAddress) > maxField {
			return bad()
		}
		return []any{int64(x.BlockNumber), hexString(x.TransactionHash), x.TraceAddress}, nil
	case "tokens", "contracts":
		x := k.GetCreation()
		if x == nil || !block(x.BlockNumber) || len(x.Address) != 20 {
			return bad()
		}
		return []any{int64(x.BlockNumber), hexString(x.Address)}, nil
	case "addresses":
		x := k.GetAddress()
		if x == nil || len(x.Address) != 20 {
			return bad()
		}
		return []any{hexString(x.Address)}, nil
	case "balances":
		x := k.GetBalance()
		if x == nil || len(x.Address) != 20 || len(x.TokenAddress) != 20 {
			return bad()
		}
		return []any{hexString(x.TokenAddress), hexString(x.Address)}, nil
	case "holders":
		x := k.GetHolder()
		if x == nil || len(x.Address) != 20 || len(x.Balance) == 0 || len(x.Balance) > 78 {
			return bad()
		}
		n, ok := new(big.Int).SetString(x.Balance, 10)
		if !ok || n.Sign() < 0 || n.BitLen() > 256 || n.String() != x.Balance {
			return bad()
		}
		return []any{x.Balance, hexString(x.Address)}, nil
	}
	return bad()
}
func (s *snapshot) scan(q query, count bool) ([]proto.Message, *pb.PageInfo, error) {
	if s.effectiveRange != nil {
		q.blocks = s.effectiveRange
	}
	from, to, end, e := s.scope(q.blocks, q.times, q.aggregate)
	if e != nil {
		return nil, nil, e
	}
	position := func(k *pb.Key) ([]any, error) {
		if q.searchKind != 0 {
			return searchKeyValues(q.searchKind, k)
		}
		return keyValues(q.family, k)
	}
	if _, e = position(q.page.GetStartAfter()); e != nil {
		return nil, nil, e
	}
	p := s.page(q.consistency)
	p.EndedAt = end
	if s.emptyScope {
		p.EndedAt = pb.EndedAt_ENDED_AT_END_OF_RANGE
		return nil, p, nil
	}
	if from > to {
		return nil, p, nil
	}
	d := define(q.family)
	if q.searchKind != 0 {
		d.order = searchOrder(q.family)
	}
	where := []string{}
	args := []any{}
	add := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	historical := q.family != "addresses" && q.family != "balances" && q.family != "holders"
	if historical {
		col := "r.block_number"
		if q.family == "blocks" {
			col = "r.number"
		}
		where = append(where, col+">="+add(int64(from)), col+"<="+add(int64(to)))
	}
	if q.family == "holders" {
		where = append(where, "r.balance>0")
	}
	if q.where != "" {
		for i, v := range q.args {
			q.where = strings.ReplaceAll(q.where, fmt.Sprintf("@%d@", i+1), add(v))
		}
		where = append(where, q.where)
	}
	kv, e := position(q.page.GetStartAfter())
	if e != nil {
		return nil, nil, e
	}
	asc := q.page.GetOrder() == pb.Order_ORDER_ASCENDING
	if kv != nil {
		ps := make([]string, len(kv))
		for i, v := range kv {
			ps[i] = add(v)
		}
		op := "<"
		if asc {
			op = ">"
		}
		where = append(where, "("+strings.Join(d.order, ",")+")"+op+"("+strings.Join(ps, ",")+")")
	}
	if len(where) == 0 {
		where = []string{"TRUE"}
	}
	ordering := make([]string, len(d.order))
	for i, o := range d.order {
		ordering[i] = o + " DESC"
		if asc {
			ordering[i] = o + " ASC"
		}
	}
	sql := "SELECT " + d.columns + " FROM " + d.table + " WHERE " + strings.Join(where, " AND ") + " ORDER BY " + strings.Join(ordering, ",") + " LIMIT 20001"
	rows, e := s.tx.Query(s.ctx, sql, args...)
	if e != nil {
		if count && !q.retain {
			return nil, nil, countError(s.ctx, e)
		}
		return nil, nil, mapError(s.ctx, e)
	}
	defer rows.Close()
	size := q.page.GetPageSize()
	if size == 0 {
		size = 25
	}
	if size > 100 {
		size = 100
	}
	if count {
		size = maxRows + 1
	}
	found := []proto.Message{}
	scanned := 0
	var last *pb.Key
	for rows.Next() {
		if e := s.ctx.Err(); e != nil {
			return nil, nil, mapError(s.ctx, e)
		}
		if scanned >= maxRows || s.scanned >= maxRows {
			if count {
				if q.retain {
					return nil, nil, budget()
				}
				return nil, nil, failureCount()
			}
			p.NextStartAfter = last
			p.EndedAt = pb.EndedAt_ENDED_AT_UNSPECIFIED
			break
		}
		if !count && len(found) >= int(size) {
			p.NextStartAfter = last
			p.EndedAt = pb.EndedAt_ENDED_AT_UNSPECIFIED
			break
		}
		m, key, e := rowFact(rows, q.family)
		if e != nil {
			return nil, nil, e
		}
		scanned++
		s.scanned++
		if q.searchKind != 0 {
			key = &pb.Key{Value: &pb.Key_Search{Search: searchResult(q.searchKind, m).Key}}
		}
		last = key
		if q.onScanned != nil {
			q.onScanned(key)
		}
		if q.match != nil && !q.match(m) {
			continue
		}
		if !count || q.retain {
			if e = s.charge(m); e != nil {
				return nil, nil, e
			}
			found = append(found, m)
		} else {
			found = append(found, nil)
		}
	}
	if e = rows.Err(); e != nil {
		if count && !q.retain {
			return nil, nil, countError(s.ctx, e)
		}
		return nil, nil, mapError(s.ctx, e)
	}
	return found, p, nil
}
func failureCount() error { return failure(codes.ResourceExhausted, "COUNT_BUDGET_EXCEEDED") }
