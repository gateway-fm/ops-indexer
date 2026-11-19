package indexerv2

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/protobuf/proto"
)

func searchCompare(a, b *pb.SearchKey) int {
	if a.Kind < b.Kind {
		return -1
	}
	if a.Kind > b.Kind {
		return 1
	}
	if a.BlockNumber < b.BlockNumber {
		return -1
	}
	if a.BlockNumber > b.BlockNumber {
		return 1
	}
	return bytes.Compare(a.Identity, b.Identity)
}
func validSearchKey(k *pb.SearchKey) bool {
	if k == nil || k.Kind < pb.SearchKind_SEARCH_KIND_BLOCK || k.Kind > pb.SearchKind_SEARCH_KIND_ADDRESS || k.BlockNumber > math.MaxInt64 {
		return false
	}
	n := 20
	if k.Kind <= pb.SearchKind_SEARCH_KIND_TRANSACTION {
		n = 32
	}
	return len(k.Identity) == n && (k.Kind != pb.SearchKind_SEARCH_KIND_ADDRESS || k.BlockNumber == 0)
}
func searchKeyValues(kind pb.SearchKind, key *pb.Key) ([]any, error) {
	if key == nil {
		return nil, nil
	}
	k := key.GetSearch()
	if !validSearchKey(k) || k.Kind != kind {
		return nil, invalid("INVALID_KEY")
	}
	if kind == pb.SearchKind_SEARCH_KIND_ADDRESS {
		return []any{hexString(k.Identity)}, nil
	}
	return []any{int64(k.BlockNumber), hexString(k.Identity)}, nil
}
func searchOrder(family string) []string {
	switch family {
	case "blocks":
		return []string{"r.number", `r.hash COLLATE "C"`}
	case "transactions":
		return []string{"r.block_number", `r.hash COLLATE "C"`}
	default:
		return define(family).order
	}
}
func searchResult(kind pb.SearchKind, m proto.Message) *pb.SearchResult {
	row := &pb.SearchResult{Key: &pb.SearchKey{Kind: kind}}
	switch x := m.(type) {
	case *pb.Block:
		row.Key.BlockNumber = x.Key.Number
		row.Key.Identity = x.Hash
		row.Fact = &pb.SearchResult_Block{Block: x}
	case *pb.Transaction:
		row.Key.BlockNumber = x.Key.BlockNumber
		row.Key.Identity = x.Key.Hash
		row.Fact = &pb.SearchResult_Transaction{Transaction: x}
	case *pb.Contract:
		row.Key.BlockNumber = x.Key.BlockNumber
		row.Key.Identity = x.Key.Address
		row.Fact = &pb.SearchResult_Contract{Contract: x}
	case *pb.Token:
		row.Key.BlockNumber = x.Key.BlockNumber
		row.Key.Identity = x.Key.Address
		row.Fact = &pb.SearchResult_Token{Token: x}
	case *pb.Address:
		row.Key.Identity = x.Key.Address
		row.Fact = &pb.SearchResult_Address{Address: x}
	}
	return row
}
func matchesSearch(query string, m proto.Message) bool {
	switch x := m.(type) {
	case *pb.Block:
		if n, e := strconv.ParseUint(query, 10, 63); e == nil {
			return n == x.Key.Number
		}
		return containsFold(hexString(x.Hash), query)
	case *pb.Transaction:
		return containsFold(hexString(x.Key.Hash), query)
	case *pb.Contract:
		return containsFold(hexString(x.Key.Address), query) || containsFold(x.GetName(), query)
	case *pb.Token:
		return containsFold(hexString(x.Key.Address), query) || containsFold(x.Symbol, query) || containsFold(x.GetName(), query)
	case *pb.Address:
		return containsFold(hexString(x.Key.Address), query)
	}
	return false
}

// Search scans families in the wire key's total order. Each family applies its
// typed position in SQL and stops at a page/scan budget; no full catalog is
// collected or sorted before the first page can be returned.
func (s *Service) Search(ctx context.Context, r *pb.SearchRequest) (*pb.SearchResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if len(r.Query) > 1024 {
		return nil, invalid("INVALID_FILTER")
	}
	selected := map[pb.SearchKind]bool{}
	if r.Kinds == nil {
		for k := pb.SearchKind_SEARCH_KIND_BLOCK; k <= pb.SearchKind_SEARCH_KIND_ADDRESS; k++ {
			selected[k] = true
		}
	} else {
		if len(r.Kinds.Kinds) > maxSet {
			return nil, invalid("FILTER_SET_TOO_LARGE")
		}
		for _, k := range r.Kinds.Kinds {
			if k < 1 || k > 5 {
				return nil, invalid("INVALID_FILTER")
			}
			selected[k] = true
		}
	}
	after := r.Page.GetStartAfter().GetSearch()
	if r.Page.GetStartAfter() != nil && !validSearchKey(after) {
		return nil, invalid("INVALID_KEY")
	}
	v, e := s.begin(ctx, r, r.Read, r.Page)
	if e != nil {
		return nil, e
	}
	defer v.close()
	out := &pb.SearchResponse{Page: v.page(pb.Consistency_CONSISTENCY_CURRENT)}
	for k := range selected {
		if k != pb.SearchKind_SEARCH_KIND_ADDRESS {
			_, _, end, e := v.scope(nil, nil, false)
			if e != nil {
				return nil, e
			}
			out.Page.EndedAt = end
			break
		}
	}
	size := r.Page.GetPageSize()
	if size == 0 {
		size = 25
	}
	if size > 100 {
		size = 100
	}
	kinds := []pb.SearchKind{1, 2, 3, 4, 5}
	asc := r.Page.GetOrder() == pb.Order_ORDER_ASCENDING
	if !asc {
		for i, j := 0, len(kinds)-1; i < j; i, j = i+1, j-1 {
			kinds[i], kinds[j] = kinds[j], kinds[i]
		}
	}
	families := map[pb.SearchKind]string{1: "blocks", 2: "transactions", 3: "contracts", 4: "tokens", 5: "addresses"}
	var last *pb.Key
	continuation := func() { out.Page.NextStartAfter = last; out.Page.EndedAt = pb.EndedAt_ENDED_AT_UNSPECIFIED }
	for _, kind := range kinds {
		if !selected[kind] || after != nil && (asc && kind < after.Kind || !asc && kind > after.Kind) {
			continue
		}
		if len(out.Results) >= int(size) || v.scanned >= maxRows {
			if last != nil {
				continuation()
				break
			}
			return nil, budget()
		}
		family := families[kind]
		page := &pb.PageRequest{PageSize: size - int32(len(out.Results)), Order: r.Page.GetOrder()}
		if after != nil && kind == after.Kind {
			page.StartAfter = r.Page.StartAfter
		}
		q := query{family: family, page: page, searchKind: kind, consistency: pb.Consistency_CONSISTENCY_CURRENT, match: func(m proto.Message) bool { return matchesSearch(r.Query, m) }, onScanned: func(k *pb.Key) { last = k }}
		// Exact block numbers and full transaction/block hashes retain their fixed
		// indexed lookup path; partial searches use bounded candidate scans.
		if family == "blocks" {
			if n, e := strconv.ParseUint(r.Query, 10, 63); e == nil {
				q.where = "r.number=@1@"
				q.args = []any{int64(n)}
			}
		}
		if family == "blocks" || family == "transactions" {
			if len(r.Query) == 66 && strings.HasPrefix(r.Query, "0x") {
				if _, e := decodeHex(r.Query, 32); e == nil {
					q.where = "r.hash=@1@"
					q.args = []any{strings.ToLower(r.Query)}
				}
			}
		}
		rows, p, e := v.scan(q, false)
		if e != nil {
			return nil, e
		}
		for _, m := range rows {
			out.Results = append(out.Results, searchResult(kind, m))
		}
		if p.NextStartAfter != nil {
			out.Page.NextStartAfter = p.NextStartAfter
			out.Page.EndedAt = pb.EndedAt_ENDED_AT_UNSPECIFIED
			break
		}
	}
	if proto.Size(out) > maxSend {
		return nil, budget()
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}
