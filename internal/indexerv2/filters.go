package indexerv2

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
)

func addresses(s *pb.AddressSet) error {
	if s == nil {
		return nil
	}
	if len(s.Addresses) > maxSet {
		return invalid("FILTER_SET_TOO_LARGE")
	}
	for _, a := range s.Addresses {
		if len(a) != 20 {
			return invalid("INVALID_FILTER")
		}
	}
	return nil
}
func hashes(s *pb.HashSet) error {
	if s == nil {
		return nil
	}
	if len(s.Hashes) > maxSet {
		return invalid("FILTER_SET_TOO_LARGE")
	}
	for _, a := range s.Hashes {
		if len(a) != 32 {
			return invalid("INVALID_FILTER")
		}
	}
	return nil
}
func addressMatch(s *pb.AddressSet, vs ...[]byte) bool {
	if s == nil {
		return true
	}
	for _, a := range s.Addresses {
		for _, v := range vs {
			if bytes.Equal(a, v) {
				return true
			}
		}
	}
	return false
}
func hashMatch(s *pb.HashSet, b []byte) bool {
	if s == nil {
		return true
	}
	for _, a := range s.Hashes {
		if bytes.Equal(a, b) {
			return true
		}
	}
	return false
}
func validateTx(f *pb.TransactionFilter) error {
	if f == nil {
		return nil
	}
	for _, s := range []*pb.AddressSet{f.AnyParty, f.From, f.To} {
		if e := addresses(s); e != nil {
			return e
		}
	}
	if f.TransactionHash != nil && len(f.TransactionHash) != 32 {
		return invalid("INVALID_FILTER")
	}
	if f.TransactionTypes != nil && len(f.TransactionTypes.Types) > maxSet {
		return invalid("FILTER_SET_TOO_LARGE")
	}
	if d := f.Destinations; d != nil {
		if len(d.Intervals)+len(d.Creations) > maxPredicates {
			return invalid("FILTER_SET_TOO_LARGE")
		}
		for _, i := range d.Intervals {
			if i == nil || len(i.ToAddress) != 20 || i.From == nil || i.To == nil || i.From.BlockNumber > math.MaxInt64 || i.To.BlockNumber > math.MaxInt64 || positionLess(i.To, i.From) {
				return invalid("INVALID_FILTER")
			}
		}
		for _, c := range d.Creations {
			if c == nil || c.Transaction == nil || len(c.Transaction.Hash) != 32 || c.Transaction.BlockNumber > math.MaxInt64 {
				return invalid("INVALID_FILTER")
			}
		}
	}
	return validateRange(f.BlockRange, f.TimeRange)
}
func validateRange(b *pb.BlockRange, t *pb.TimeRange) error {
	if b != nil && (b.FromBlock != nil && *b.FromBlock > math.MaxInt64 || b.ToBlock != nil && *b.ToBlock > math.MaxInt64 || b.FromBlock != nil && b.ToBlock != nil && *b.FromBlock > *b.ToBlock) {
		return invalid("INVALID_FILTER")
	}
	if t != nil && (t.FromTimestamp != nil && *t.FromTimestamp > math.MaxInt64 || t.ToTimestamp != nil && *t.ToTimestamp > math.MaxInt64 || t.FromTimestamp != nil && t.ToTimestamp != nil && *t.FromTimestamp > *t.ToTimestamp) {
		return invalid("INVALID_FILTER")
	}
	return nil
}
func positionLess(a, b *pb.TransactionPosition) bool {
	return a.BlockNumber < b.BlockNumber || a.BlockNumber == b.BlockNumber && a.TransactionIndex < b.TransactionIndex
}
func matchesTx(f *pb.TransactionFilter, t *pb.Transaction) bool {
	if f == nil {
		return true
	}
	if !addressMatch(f.AnyParty, t.From, t.To, t.ContractAddress) || !addressMatch(f.From, t.From) || !addressMatch(f.To, t.To) {
		return false
	}
	if f.TransactionHash != nil && !bytes.Equal(f.TransactionHash, t.Key.Hash) {
		return false
	}
	if s := f.TransactionTypes; s != nil {
		found := false
		for _, n := range s.Types {
			if n == t.GetTransactionType() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if r := f.TimeRange; r != nil {
		if r.FromTimestamp != nil && t.BlockTimestamp < *r.FromTimestamp || r.ToTimestamp != nil && t.BlockTimestamp > *r.ToTimestamp {
			return false
		}
	}
	if d := f.Destinations; d != nil {
		matched := false
		p := &pb.TransactionPosition{BlockNumber: t.Key.BlockNumber, TransactionIndex: t.Key.TransactionIndex}
		for _, i := range d.Intervals {
			if bytes.Equal(t.To, i.ToAddress) && !positionLess(p, i.From) && positionLess(p, i.To) {
				matched = true
				break
			}
		}
		if !matched {
			for _, c := range d.Creations {
				if t.To == nil && c.Transaction.BlockNumber == t.Key.BlockNumber && c.Transaction.TransactionIndex == t.Key.TransactionIndex && bytes.Equal(c.Transaction.Hash, t.Key.Hash) {
					matched = true
					break
				}
			}
		}
		if !matched {
			return false
		}
	}
	return true
}
func validateLog(f *pb.LogFilter) error {
	if f == nil {
		return nil
	}
	if e := addresses(f.Addresses); e != nil {
		return e
	}
	for _, s := range []*pb.HashSet{f.Topic0, f.Topic1, f.Topic2, f.Topic3} {
		if e := hashes(s); e != nil {
			return e
		}
	}
	if f.TransactionHash != nil && len(f.TransactionHash) != 32 || f.BlockNumber != nil && *f.BlockNumber > math.MaxInt64 {
		return invalid("INVALID_FILTER")
	}
	return validateRange(f.BlockRange, nil)
}
func matchesLog(f *pb.LogFilter, r *pb.Log) bool {
	if f == nil {
		return true
	}
	return addressMatch(f.Addresses, r.Address) && hashMatch(f.Topic0, r.Topic0) && hashMatch(f.Topic1, r.Topic1) && hashMatch(f.Topic2, r.Topic2) && hashMatch(f.Topic3, r.Topic3) && (f.TransactionHash == nil || bytes.Equal(f.TransactionHash, r.TransactionHash)) && (f.BlockNumber == nil || *f.BlockNumber == r.Key.BlockNumber)
}
func validateTransfer(f *pb.TransferFilter) error {
	if f == nil {
		return nil
	}
	for _, s := range []*pb.AddressSet{f.AnyParty, f.From, f.To, f.Tokens} {
		if e := addresses(s); e != nil {
			return e
		}
	}
	if f.TransactionHash != nil && len(f.TransactionHash) != 32 || f.BlockNumber != nil && *f.BlockNumber > math.MaxInt64 {
		return invalid("INVALID_FILTER")
	}
	if f.TokenKind != nil && transferTokenKind(*f.TokenKind) == "" {
		return invalid("INVALID_FILTER")
	}
	return validateRange(f.BlockRange, nil)
}
func transferTokenKind(kind pb.TokenKind) string {
	switch kind {
	case pb.TokenKind_TOKEN_KIND_ERC20:
		return "ERC20"
	case pb.TokenKind_TOKEN_KIND_ERC721:
		return "ERC721"
	case pb.TokenKind_TOKEN_KIND_ERC1155:
		return "ERC1155"
	default:
		return ""
	}
}

// List and Count narrow the same source population before seek/scan budgets.
// The matching guard also checks the typed field on every returned fact.
func transferSourceWhere(f *pb.TransferFilter) (string, []any) {
	conditions, args := []string{}, []any{}
	if f.GetTransactionHash() != nil {
		args = append(args, hexString(f.GetTransactionHash()))
		conditions = append(conditions, fmt.Sprintf("r.tx_hash=@%d@", len(args)))
	}
	if f != nil && f.TokenKind != nil {
		args = append(args, transferTokenKind(*f.TokenKind))
		conditions = append(conditions, fmt.Sprintf("r.token_type=@%d@", len(args)))
	}
	return strings.Join(conditions, " AND "), args
}
func matchesTransfer(f *pb.TransferFilter, r *pb.TokenTransfer) bool {
	if f == nil {
		return true
	}
	return addressMatch(f.AnyParty, r.From, r.To) && addressMatch(f.From, r.From) && addressMatch(f.To, r.To) && addressMatch(f.Tokens, r.TokenAddress) && (f.TransactionHash == nil || bytes.Equal(f.TransactionHash, r.TransactionHash)) && (f.BlockNumber == nil || *f.BlockNumber == r.Key.BlockNumber) && (f.TokenKind == nil || transferTokenKind(*f.TokenKind) == r.TokenType)
}
func validateInternal(f *pb.InternalFilter) error {
	if f == nil {
		return nil
	}
	if e := addresses(f.AnyParty); e != nil {
		return e
	}
	if f.TransactionHash != nil && len(f.TransactionHash) != 32 || f.BlockNumber != nil && *f.BlockNumber > math.MaxInt64 {
		return invalid("INVALID_FILTER")
	}
	return validateRange(f.BlockRange, nil)
}
func matchesInternal(f *pb.InternalFilter, r *pb.InternalTransaction) bool {
	if f == nil {
		return true
	}
	return addressMatch(f.AnyParty, r.From, r.To) && (f.TransactionHash == nil || bytes.Equal(f.TransactionHash, r.Key.TransactionHash)) && (f.BlockNumber == nil || *f.BlockNumber == r.Key.BlockNumber)
}
func containsFold(s, q string) bool { return strings.Contains(strings.ToLower(s), strings.ToLower(q)) }
