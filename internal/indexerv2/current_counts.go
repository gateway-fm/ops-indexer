package indexerv2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"time"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func currentCountAddresses(x *pb.CurrentCountSelector) (*pb.AddressSet, error) {
	if x == nil {
		return nil, invalid("INVALID_FILTER")
	}
	var a *pb.AddressSet
	switch v := x.Value.(type) {
	case *pb.CurrentCountSelector_Addresses:
		if v.Addresses == nil {
			return nil, invalid("INVALID_FILTER")
		}
		a = v.Addresses.Addresses
	case *pb.CurrentCountSelector_Tokens:
		if v.Tokens == nil {
			return nil, invalid("INVALID_FILTER")
		}
		a = v.Tokens.Addresses
		if v.Tokens.TokenType != "" && v.Tokens.TokenType != "ERC20" && v.Tokens.TokenType != "ERC721" && v.Tokens.TokenType != "ERC1155" || len(v.Tokens.Search) > 128 || strings.TrimSpace(v.Tokens.Search) != v.Tokens.Search {
			return nil, invalid("INVALID_FILTER")
		}
		if e := validateRange(v.Tokens.BlockRange, nil); e != nil {
			return nil, e
		}
	case *pb.CurrentCountSelector_Holders:
		if v.Holders == nil || len(v.Holders.TokenAddress) != 20 {
			return nil, invalid("INVALID_FILTER")
		}
		a = v.Holders.Addresses
	default:
		return nil, invalid("INVALID_FILTER")
	}
	if a == nil {
		return nil, invalid("INVALID_FILTER")
	}
	if e := addresses(a); e != nil {
		return nil, e
	}
	for i := 1; i < len(a.Addresses); i++ {
		if bytes.Compare(a.Addresses[i-1], a.Addresses[i]) >= 0 {
			return nil, invalid("INVALID_FILTER")
		}
	}
	return a, nil
}

// BatchCountCurrent probes only bounded primary-key sets in one database
// snapshot. Work is charged by selected keys, never by the one COUNT result.
func (s *Service) BatchCountCurrent(ctx context.Context, r *pb.BatchCountCurrentRequest) (*pb.BatchCountCurrentResponse, error) {
	if e := validateMessage(r); e != nil {
		return nil, e
	}
	if len(r.Selectors) < 1 || len(r.Selectors) > 25 {
		return nil, invalid("INVALID_FILTER")
	}
	work := 0
	sets := make([][]string, len(r.Selectors))
	for i, x := range r.Selectors {
		a, e := currentCountAddresses(x)
		if e != nil {
			return nil, e
		}
		work += len(a.Addresses)
		if work > maxRows {
			return nil, budget()
		}
		sets[i] = make([]string, len(a.Addresses))
		for j, address := range a.Addresses {
			sets[i][j] = hexString(address)
		}
	}
	v, e := s.begin(ctx, r, r.Read, nil)
	if e != nil {
		return nil, e
	}
	defer v.close()
	out := &pb.BatchCountCurrentResponse{Page: v.page(pb.Consistency_CONSISTENCY_CURRENT), ObservedAt: timestamppb.New(time.Now().UTC())}
	for i, x := range r.Selectors {
		query := ""
		args := []any{sets[i]}
		var count int64
		counted := false
		switch item := x.Value.(type) {
		case *pb.CurrentCountSelector_Addresses:
			query = `SELECT count(*) FROM unnest($1::text[]) AS selected(address) JOIN address_stats r ON r.address=selected.address`
		case *pb.CurrentCountSelector_Holders:
			query = `SELECT count(*) FROM unnest($1::text[]) AS selected(address) JOIN token_balances_current r ON r.address=selected.address AND r.token_address=$2 WHERE r.balance>0`
			args = append(args, hexString(item.Holders.TokenAddress))
		case *pb.CurrentCountSelector_Tokens:
			filter := item.Tokens
			bounded := queryCurrentTokens(filter, sets[i])
			rows, _, err := v.scan(bounded, true)
			if err != nil {
				return nil, err
			}
			count = int64(len(rows))
			counted = true
		}
		if !counted {
			if e = v.tx.QueryRow(v.ctx, query, args...).Scan(&count); e != nil {
				return nil, mapError(v.ctx, e)
			}
		}
		if count < 0 || count > int64(len(sets[i])) {
			return nil, dataError()
		}
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(x)
		if err != nil {
			return nil, dataError()
		}
		digest := sha256.Sum256(raw)
		out.Counts = append(out.Counts, &pb.CurrentCountResult{SelectorDigest: digest[:], Count: uint64(count)})
	}
	if e = v.commit(); e != nil {
		return nil, e
	}
	return out, nil
}

// Selected metadata is bounded by the same primary-key filter. Reusing the
// ListTokens matcher avoids differences between Go and database Unicode folds.
func queryCurrentTokens(filter *pb.CurrentTokens, addresses []string) query {
	return query{family: "tokens", blocks: filter.BlockRange, consistency: pb.Consistency_CONSISTENCY_CURRENT, where: "r.address=ANY(@1@::text[])", args: []any{addresses}, match: func(m proto.Message) bool {
		x := m.(*pb.Token)
		return (filter.TokenType == "" || filter.TokenType == x.TokenType) && (filter.Search == "" || containsFold(x.Symbol, filter.Search) || containsFold(x.GetName(), filter.Search) || containsFold(hexString(x.Key.Address), filter.Search))
	}}
}
