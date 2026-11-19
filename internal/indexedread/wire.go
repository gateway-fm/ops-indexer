package indexedread

import (
	"bytes"
	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	q "github.com/gateway-fm/chain-indexer/internal/indexedquery"
)

func decodeQuery(v *pb.IndexedQuery) (q.Query, error) {
	if v == nil || len(v.ProtoReflect().GetUnknown()) != 0 || v.RawLimit > 1024 {
		return q.Query{}, ErrInvalid
	}
	out := q.Query{Family: q.Family(v.Family), Address: v.Address, Hash: v.Hash, Token: v.Token, Topic0: v.Topic0, Search: v.Search, Block: v.Block, From: v.From, Through: v.Through, RawLimit: int(v.RawLimit)}
	if v.Upper != nil {
		if len(v.Upper.ProtoReflect().GetUnknown()) != 0 {
			return q.Query{}, ErrInvalid
		}
		out.Upper = &q.Anchor{Number: v.Upper.Number, Hash: v.Upper.Hash}
	}
	if v.After != nil {
		if len(v.After.ProtoReflect().GetUnknown()) != 0 {
			return q.Query{}, ErrInvalid
		}
		out.After = &q.Position{Block: v.After.Block, Index: v.After.Index, Hash: v.After.Hash, Subkey: v.After.Subkey, Amount: v.After.Amount}
	}
	return q.Normalize(out)
}
func encodePosition(p q.Position) *pb.IndexedPosition {
	return &pb.IndexedPosition{Block: p.Block, Index: p.Index, Hash: p.Hash, Subkey: p.Subkey, Amount: p.Amount}
}
func encodeQuery(v q.Query) *pb.IndexedQuery {
	out := &pb.IndexedQuery{Family: string(v.Family), Address: v.Address, Hash: v.Hash, Token: v.Token, Topic0: v.Topic0, Search: v.Search, Block: v.Block, From: v.From, Through: v.Through, RawLimit: uint32(v.RawLimit)}
	if v.Upper != nil {
		out.Upper = &pb.IndexedAnchor{Number: v.Upper.Number, Hash: v.Upper.Hash}
	}
	if v.After != nil {
		out.After = encodePosition(*v.After)
	}
	return out
}
func encodeData(v q.Data) *pb.IndexedCohort {
	out := &pb.IndexedCohort{Exhausted: v.Exhausted}
	if v.Upper != nil {
		out.Upper = &pb.IndexedAnchor{Number: v.Upper.Number, Hash: v.Upper.Hash}
	}
	for _, r := range v.Rows {
		out.Rows = append(out.Rows, &pb.IndexedRecord{Family: string(r.Family), Key: r.Key, Position: encodePosition(r.Position), Json: r.JSON})
	}
	for _, r := range v.Context {
		out.Context = append(out.Context, &pb.IndexedRecord{Family: string(r.Family), Key: r.Key, Position: encodePosition(r.Position), Json: r.JSON})
	}
	return out
}
func decodeSelection(v *pb.IndexedProtectedSelection) (ProtectedSelection, error) {
	if v == nil || len(v.ProtoReflect().GetUnknown()) != 0 || len(v.Targets) > maxProtectedAddresses {
		return ProtectedSelection{}, ErrInvalid
	}
	out := ProtectedSelection{FromTime: v.FromTime, ThroughTime: v.ThroughTime, FromNumber: v.FromNumber, ThroughNumber: v.ThroughNumber}
	for _, a := range v.Targets {
		if len(a) != 20 {
			return ProtectedSelection{}, ErrInvalid
		}
		var address [20]byte
		copy(address[:], a)
		out.Targets = append(out.Targets, address)
	}
	return out, nil
}
func decodeProtectedPosition(v *pb.IndexedProtectedPosition) (ProtectedPosition, error) {
	if v == nil || len(v.ProtoReflect().GetUnknown()) != 0 || len(v.Hash) != 32 {
		return ProtectedPosition{}, ErrInvalid
	}
	out := ProtectedPosition{Number: v.Number, Index: v.Index}
	copy(out.Hash[:], v.Hash)
	return out, nil
}
func decodePredicate(v *pb.IndexedPredicate) (ProtectedPredicate, error) {
	if v == nil || len(v.ProtoReflect().GetUnknown()) != 0 || len(v.Intervals) > maxProtectedPredicates || len(v.Creations) > maxProtectedPredicates-len(v.Intervals) {
		return ProtectedPredicate{}, ErrInvalid
	}
	out := ProtectedPredicate{}
	for _, x := range v.Intervals {
		if x == nil || len(x.ProtoReflect().GetUnknown()) != 0 || len(x.Address) != 20 {
			return ProtectedPredicate{}, ErrInvalid
		}
		a := ProtectedInterval{FromNumber: x.FromNumber, FromIndex: x.FromIndex, BeforeNumber: x.BeforeNumber, BeforeIndex: x.BeforeIndex}
		copy(a.Address[:], x.Address)
		out.Intervals = append(out.Intervals, a)
	}
	for _, x := range v.Creations {
		if x == nil || len(x.ProtoReflect().GetUnknown()) != 0 || len(x.Address) != 20 {
			return ProtectedPredicate{}, ErrInvalid
		}
		pos, e := decodeProtectedPosition(x.Position)
		if e != nil {
			return ProtectedPredicate{}, e
		}
		a := ProtectedCreation{Position: pos}
		copy(a.Address[:], x.Address)
		out.Creations = append(out.Creations, a)
	}
	return out, nil
}
func decodeTransactionQuery(v *pb.IndexedTransactionQuery) (ProtectedTransactionQuery, error) {
	if v == nil || len(v.ProtoReflect().GetUnknown()) != 0 || v.Limit > 100 {
		return ProtectedTransactionQuery{}, ErrInvalid
	}
	out := ProtectedTransactionQuery{Limit: uint16(v.Limit)}
	if v.Address != nil {
		if len(v.Address) != 20 {
			return out, ErrInvalid
		}
		out.Address = new([20]byte)
		copy(out.Address[:], v.Address)
	}
	if v.Hash != nil {
		if len(v.Hash) != 32 {
			return out, ErrInvalid
		}
		out.Hash = new([32]byte)
		copy(out.Hash[:], v.Hash)
	}
	if v.After != nil {
		p, e := decodeProtectedPosition(v.After)
		if e != nil {
			return out, e
		}
		out.After = &p
	}
	return out, nil
}
func encodeRow(v ProtectedRow) *pb.IndexedProtectedRow {
	out := &pb.IndexedProtectedRow{Position: &pb.IndexedProtectedPosition{Number: v.Position.Number, Index: v.Position.Index, Hash: bytes.Clone(v.Position.Hash[:])}, BlockHash: bytes.Clone(v.BlockHash[:]), Timestamp: v.Timestamp, From: bytes.Clone(v.From[:])}
	if v.To != nil {
		out.To = bytes.Clone(v.To[:])
	}
	return out
}
