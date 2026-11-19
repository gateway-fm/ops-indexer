package indexedread

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/indexedquery"
)

const maxProtectedAddresses = 4096
const maxProtectedPredicates = 16384

// ProtectedSelection describes a fixed retained read. Its address set contains
// query TARGET wallets only, never the recipient's unrelated wallets. The
// configured epoch identifies a deployment selection, not an MVCC generation.
type ProtectedSelection struct {
	Expected                  indexedquery.Metadata
	Targets                   [][20]byte
	FromTime, ThroughTime     uint64
	FromNumber, ThroughNumber uint64
}

type ProtectedPosition struct {
	Number uint64
	Index  uint32
	Hash   [32]byte
}
type ProtectedRow struct {
	Position  ProtectedPosition
	BlockHash [32]byte
	Timestamp uint64
	From      [20]byte
	To        *[20]byte
}
type ProtectedDestinations struct {
	Addresses   [][20]byte
	First, Last *uint64
}
type ProtectedWitness struct {
	Address [20]byte
	Row     ProtectedRow
}

// Intervals are half-open (block, transaction-index) ranges from the qualified
// namespace pass. Creation coordinates identify the original ROOT only.
type ProtectedInterval struct {
	Address      [20]byte
	FromNumber   uint64
	FromIndex    uint32
	BeforeNumber uint64
	BeforeIndex  uint32
}
type ProtectedCreation struct {
	Address  [20]byte
	Position ProtectedPosition
}
type ProtectedPredicate struct {
	Intervals []ProtectedInterval
	Creations []ProtectedCreation
}
type ProtectedTransactionQuery struct {
	Address *[20]byte
	Hash    *[32]byte
	After   *ProtectedPosition
	Limit   uint16
}

// ProtectedRead is an owned purpose, not a SQL callback or a coverage proof.
// All SQL samples, detached copies and the fresh recheck share one budget.
type ProtectedRead struct {
	mu                 sync.Mutex
	owner              *Reader
	selection          ProtectedSelection
	targets            []string
	b                  *budget
	closed             bool
	destinations       *ProtectedDestinations
	destinationsDigest [32]byte
	queries            []protectedQueryReceipt
}

type protectedQueryReceipt struct {
	predicate protectedPredicate
	query     ProtectedTransactionQuery
	witness   bool
	digest    [32]byte
}
type protectedPredicate struct{ intervals, creations string }

func protectedAddress(a [20]byte) string { return "0x" + hex.EncodeToString(a[:]) }
func protectedHash(h [32]byte) string    { return "0x" + hex.EncodeToString(h[:]) }
func decodeProtectedHex(s string, n int) ([]byte, error) {
	if len(s) != 2+2*n || len(s) < 2 || s[:2] != "0x" {
		return nil, ErrUnavailable
	}
	raw, err := hex.DecodeString(s[2:])
	if err != nil || hex.EncodeToString(raw) != s[2:] {
		return nil, ErrUnavailable
	}
	return raw, nil
}

func (r *Reader) BeginProtected(ctx context.Context, selection ProtectedSelection) (*ProtectedRead, error) {
	if r == nil || r.pool == nil || ctx == nil || ctx.Err() != nil || selection.Expected != r.Metadata() || !selection.Expected.Identity.Valid() || !selection.Expected.Binding.Valid() ||
		len(selection.Targets) > maxProtectedAddresses || selection.FromTime > selection.ThroughTime || selection.ThroughTime > 253402300799 || selection.FromNumber > selection.ThroughNumber || selection.ThroughNumber >= math.MaxInt64 {
		return nil, ErrInvalid
	}
	for i, a := range selection.Targets {
		if i > 0 && bytes.Compare(selection.Targets[i-1][:], a[:]) >= 0 {
			return nil, ErrInvalid
		}
	}
	b := newBudgetForContext(ctx, r.limits)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(b.deadline) {
		b.deadline = deadline
	}
	if err := b.retain(len(selection.Targets) * (20 + 42)); err != nil {
		return nil, err
	}
	selection.Targets = slices.Clone(selection.Targets)
	targets := make([]string, len(selection.Targets))
	for i, a := range selection.Targets {
		targets[i] = protectedAddress(a)
	}
	return &ProtectedRead{owner: r, selection: selection, targets: targets, b: b}, nil
}

func (p *ProtectedRead) valid(ctx context.Context) bool {
	return p != nil && !p.closed && p.owner != nil && p.owner.Metadata() == p.selection.Expected && ctx != nil && ctx.Err() == nil && time.Now().Before(p.b.deadline)
}
func (p *ProtectedRead) args() []any {
	s := p.selection
	return []any{p.targets, int64(s.FromTime), int64(s.ThroughTime), int64(s.FromNumber), int64(s.ThroughNumber)}
}
func (p *ProtectedRead) Close() {
	if p != nil {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.closed = true
	}
}

func cloneProtectedDestinations(d ProtectedDestinations) ProtectedDestinations {
	d.Addresses = slices.Clone(d.Addresses)
	if d.First != nil {
		n := *d.First
		d.First = &n
	}
	if d.Last != nil {
		n := *d.Last
		d.Last = &n
	}
	return d
}
func protectedObservation(v any) [32]byte {
	h := sha256.New()
	writeNumber := func(n uint64) { var raw [8]byte; binary.BigEndian.PutUint64(raw[:], n); h.Write(raw[:]) }
	writeRow := func(row ProtectedRow) {
		writeNumber(row.Position.Number)
		writeNumber(uint64(row.Position.Index))
		h.Write(row.Position.Hash[:])
		h.Write(row.BlockHash[:])
		writeNumber(row.Timestamp)
		h.Write(row.From[:])
		if row.To == nil {
			h.Write([]byte{0})
		} else {
			h.Write([]byte{1})
			h.Write(row.To[:])
		}
	}
	switch rows := v.(type) {
	case ProtectedDestinations:
		h.Write([]byte("ops.protected-index.destinations/v1"))
		writeNumber(uint64(len(rows.Addresses)))
		for _, a := range rows.Addresses {
			h.Write(a[:])
		}
		if rows.First == nil {
			h.Write([]byte{0})
		} else {
			h.Write([]byte{1})
			writeNumber(*rows.First)
			writeNumber(*rows.Last)
		}
	case []ProtectedRow:
		h.Write([]byte("ops.protected-index.transactions/v1"))
		writeNumber(uint64(len(rows)))
		for _, row := range rows {
			writeRow(row)
		}
	case []ProtectedWitness:
		h.Write([]byte("ops.protected-index.witnesses/v1"))
		writeNumber(uint64(len(rows)))
		for _, row := range rows {
			h.Write(row.Address[:])
			writeRow(row.Row)
		}
	default:
		panic("internal protected observation type")
	}
	return protectedSum(h)
}
func protectedSum(h hash.Hash) (out [32]byte) { h.Sum(out[:0]); return out }

func (p *ProtectedRead) ReadDestinations(ctx context.Context) (ProtectedDestinations, error) {
	if p == nil {
		return ProtectedDestinations{}, ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid(ctx) || p.destinations != nil {
		return ProtectedDestinations{}, ErrInvalid
	}
	d, err := p.destinationsRead(ctx, nil)
	if err != nil {
		return ProtectedDestinations{}, err
	}
	if err = p.b.retain(len(d.Addresses) * 20); err != nil {
		return ProtectedDestinations{}, err
	}
	p.destinations = &d
	p.destinationsDigest = protectedObservation(d)
	return cloneProtectedDestinations(d), nil
}

func (p *ProtectedRead) destinationsRead(ctx context.Context, existing *snapshot) (ProtectedDestinations, error) {
	out := ProtectedDestinations{Addresses: [][20]byte{}}
	// Empty target selection is explicit and cannot broaden to all traffic.
	if len(p.targets) == 0 {
		return out, nil
	}
	read := func(s *snapshot) error {
		var valid bool
		var first, last *int64
		if err := s.one(protectedDestinationBoundsSQL, p.args(), &valid, &first, &last); err != nil {
			return err
		}
		if !valid || (first == nil) != (last == nil) {
			return ErrUnavailable
		}
		if first != nil {
			if *first < 0 || *last < *first {
				return ErrUnavailable
			}
			lo, hi := uint64(*first), uint64(*last)
			out.First, out.Last = &lo, &hi
		}
		rows, err := s.query(protectedDestinationsSQL, p.args()...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if len(out.Addresses) >= maxProtectedAddresses {
				return ErrTooBroad
			}
			var address *string
			if err = rows.Scan(&address); err != nil {
				return ErrUnavailable
			}
			if address == nil {
				return ErrUnavailable
			}
			raw, err := decodeProtectedHex(*address, 20)
			if err != nil {
				return err
			}
			if err = p.b.retain(20); err != nil {
				return err
			}
			var a [20]byte
			copy(a[:], raw)
			if len(out.Addresses) > 0 && bytes.Compare(out.Addresses[len(out.Addresses)-1][:], a[:]) >= 0 {
				return ErrUnavailable
			}
			out.Addresses = append(out.Addresses, a)
		}
		return publicError(rows.Err())
	}
	var err error
	if existing != nil {
		err = read(existing)
	} else {
		err = p.owner.withSnapshot(ctx, p.b, read)
	}
	return out, err
}

func compileProtectedPredicate(p ProtectedPredicate) (protectedPredicate, error) {
	if len(p.Intervals) > maxProtectedPredicates || len(p.Creations) > maxProtectedPredicates-len(p.Intervals) {
		return protectedPredicate{}, ErrTooBroad
	}
	type interval struct {
		A string `json:"a"`
		N uint64 `json:"n"`
		I uint32 `json:"i"`
		Z uint64 `json:"z"`
		J uint32 `json:"j"`
	}
	type creation struct {
		A string `json:"a"`
		H string `json:"h"`
		N uint64 `json:"n"`
		I uint32 `json:"i"`
	}
	intervals := make([]interval, 0, len(p.Intervals))
	creations := make([]creation, 0, len(p.Creations))
	for _, v := range p.Intervals {
		if v.FromNumber >= math.MaxInt64 || v.BeforeNumber > math.MaxInt64 || v.FromNumber > v.BeforeNumber || v.FromNumber == v.BeforeNumber && v.FromIndex >= v.BeforeIndex {
			return protectedPredicate{}, ErrInvalid
		}
		intervals = append(intervals, interval{protectedAddress(v.Address), v.FromNumber, v.FromIndex, v.BeforeNumber, v.BeforeIndex})
	}
	for _, v := range p.Creations {
		if v.Position.Number >= math.MaxInt64 || v.Position.Hash == ([32]byte{}) {
			return protectedPredicate{}, ErrInvalid
		}
		creations = append(creations, creation{protectedAddress(v.Address), protectedHash(v.Position.Hash), v.Position.Number, v.Position.Index})
	}
	left, _ := json.Marshal(intervals)
	right, _ := json.Marshal(creations)
	return protectedPredicate{string(left), string(right)}, nil
}

func cloneProtectedQuery(q ProtectedTransactionQuery) ProtectedTransactionQuery {
	if q.Address != nil {
		a := *q.Address
		q.Address = &a
	}
	if q.Hash != nil {
		h := *q.Hash
		q.Hash = &h
	}
	if q.After != nil {
		v := *q.After
		q.After = &v
	}
	return q
}

func (p *ProtectedRead) prepareQuery(predicate ProtectedPredicate, q ProtectedTransactionQuery, witness bool) (protectedQueryReceipt, error) {
	if p.destinations == nil || q.Limit < 1 || q.Limit > 100 || q.Hash != nil && (q.After != nil || q.Limit != 1) || q.After != nil && q.After.Number >= math.MaxInt64 {
		return protectedQueryReceipt{}, ErrInvalid
	}
	if len(predicate.Intervals) > maxProtectedPredicates || len(predicate.Creations) > maxProtectedPredicates-len(predicate.Intervals) {
		return protectedQueryReceipt{}, ErrTooBroad
	}
	if len(p.queries) >= 2 {
		return protectedQueryReceipt{}, ErrInvalid
	}
	for _, old := range p.queries {
		if old.witness == witness {
			return protectedQueryReceipt{}, ErrInvalid
		}
	}
	selectedDestination := func(a [20]byte) bool {
		_, ok := slices.BinarySearchFunc(p.destinations.Addresses, a, func(a, b [20]byte) int { return bytes.Compare(a[:], b[:]) })
		return ok
	}
	for _, v := range predicate.Intervals {
		if !selectedDestination(v.Address) || v.FromNumber < p.selection.FromNumber || v.BeforeNumber > p.selection.ThroughNumber+1 {
			return protectedQueryReceipt{}, ErrInvalid
		}
	}
	for _, v := range predicate.Creations {
		if !selectedDestination(v.Address) || v.Position.Number < p.selection.FromNumber || v.Position.Number > p.selection.ThroughNumber {
			return protectedQueryReceipt{}, ErrInvalid
		}
	}
	if q.Address != nil {
		if _, ok := slices.BinarySearchFunc(p.selection.Targets, *q.Address, func(a, b [20]byte) int { return bytes.Compare(a[:], b[:]) }); !ok {
			return protectedQueryReceipt{}, ErrInvalid
		}
	}
	// Predicate strings and driver expansion are precharged before formatting.
	if err := p.b.retain((len(predicate.Intervals)+len(predicate.Creations))*512 + 256); err != nil {
		return protectedQueryReceipt{}, err
	}
	compiled, err := compileProtectedPredicate(predicate)
	if err != nil {
		return protectedQueryReceipt{}, err
	}
	return protectedQueryReceipt{predicate: compiled, query: cloneProtectedQuery(q), witness: witness}, nil
}

func (p *ProtectedRead) ReadWitnesses(ctx context.Context, predicate ProtectedPredicate) ([]ProtectedWitness, error) {
	if p == nil {
		return nil, ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid(ctx) {
		return nil, ErrInvalid
	}
	receipt, err := p.prepareQuery(predicate, ProtectedTransactionQuery{Limit: 1}, true)
	if err != nil {
		return nil, err
	}
	_, witnesses, err := p.queryRead(ctx, receipt, nil)
	if err != nil {
		return nil, err
	}
	receipt.digest = protectedObservation(witnesses)
	p.queries = append(p.queries, receipt)
	if err = p.b.retain(len(witnesses) * 180); err != nil {
		return nil, err
	}
	return cloneProtectedWitnesses(witnesses), nil
}

func (p *ProtectedRead) ReadTransactions(ctx context.Context, predicate ProtectedPredicate, q ProtectedTransactionQuery) ([]ProtectedRow, bool, error) {
	if p == nil {
		return nil, false, ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid(ctx) {
		return nil, false, ErrInvalid
	}
	receipt, err := p.prepareQuery(predicate, q, false)
	if err != nil {
		return nil, false, err
	}
	rows, _, err := p.queryRead(ctx, receipt, nil)
	if err != nil {
		return nil, false, err
	}
	receipt.digest = protectedObservation(rows)
	p.queries = append(p.queries, receipt)
	end := len(rows) <= int(q.Limit)
	if !end {
		rows = rows[:q.Limit]
	}
	if err = p.b.retain(len(rows) * 160); err != nil {
		return nil, false, err
	}
	return cloneProtectedRows(rows), end, nil
}

func cloneProtectedRows(rows []ProtectedRow) []ProtectedRow {
	out := slices.Clone(rows)
	for i := range out {
		if out[i].To != nil {
			a := *out[i].To
			out[i].To = &a
		}
	}
	return out
}
func cloneProtectedWitnesses(rows []ProtectedWitness) []ProtectedWitness {
	out := slices.Clone(rows)
	for i := range out {
		if out[i].Row.To != nil {
			a := *out[i].Row.To
			out[i].Row.To = &a
		}
	}
	return out
}

func (p *ProtectedRead) queryRead(ctx context.Context, r protectedQueryReceipt, existing *snapshot) ([]ProtectedRow, []ProtectedWitness, error) {
	out := []ProtectedRow{}
	witnesses := []ProtectedWitness{}
	if len(p.targets) == 0 {
		return out, witnesses, nil
	}
	args := append(p.args(), r.predicate.intervals, r.predicate.creations)
	sql := protectedWitnessesSQL
	if !r.witness {
		var address, hash, afterHash any
		var number int64
		var index int64
		if r.query.Address != nil {
			address = protectedAddress(*r.query.Address)
		}
		if r.query.Hash != nil {
			hash = protectedHash(*r.query.Hash)
		}
		if r.query.After != nil {
			number = int64(r.query.After.Number)
			index = int64(r.query.After.Index)
			afterHash = protectedHash(r.query.After.Hash)
		}
		args = append(args, address, hash, r.query.After != nil, number, index, afterHash, int(r.query.Limit)+1)
		sql = protectedTransactionsSQL
	}
	read := func(s *snapshot) error {
		rows, err := s.query(sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if r.witness && len(witnesses) >= maxProtectedAddresses || !r.witness && len(out) >= int(r.query.Limit)+1 {
				return ErrTooBroad
			}
			var valid bool
			var address, hash, blockHash, from *string
			var to *string
			var number, index, timestamp *int64
			if err = rows.Scan(&valid, &address, &hash, &blockHash, &number, &index, &timestamp, &from, &to); err != nil {
				return ErrUnavailable
			}
			if !valid || hash == nil || blockHash == nil || from == nil || number == nil || index == nil || timestamp == nil || *number < 0 || *index < 0 || *index > math.MaxUint32 || *timestamp < 0 {
				return ErrUnavailable
			}
			if err = p.b.retain(180); err != nil {
				return err
			}
			row := ProtectedRow{Position: ProtectedPosition{Number: uint64(*number), Index: uint32(*index)}, Timestamp: uint64(*timestamp)}
			for _, field := range []struct {
				text   string
				target []byte
			}{{*hash, row.Position.Hash[:]}, {*blockHash, row.BlockHash[:]}, {*from, row.From[:]}} {
				raw, err := decodeProtectedHex(field.text, len(field.target))
				if err != nil {
					return err
				}
				copy(field.target, raw)
			}
			if to != nil {
				raw, err := decodeProtectedHex(*to, 20)
				if err != nil {
					return err
				}
				var a [20]byte
				copy(a[:], raw)
				row.To = &a
			}
			if r.witness {
				if address == nil {
					return ErrUnavailable
				}
				raw, err := decodeProtectedHex(*address, 20)
				if err != nil {
					return err
				}
				var a [20]byte
				copy(a[:], raw)
				if len(witnesses) > 0 && bytes.Compare(witnesses[len(witnesses)-1].Address[:], a[:]) >= 0 {
					return ErrUnavailable
				}
				witnesses = append(witnesses, ProtectedWitness{a, row})
			} else {
				out = append(out, row)
			}
		}
		return publicError(rows.Err())
	}
	var err error
	if existing != nil {
		err = read(existing)
	} else {
		err = p.owner.withSnapshot(ctx, p.b, read)
	}
	return out, witnesses, err
}

// Recheck repeats every exact purpose under fresh known-commit snapshots. Its
// private observation includes empty results and lookahead. It attests neither
// full-chain completeness nor unobservable replacement of the configured DB.
func (p *ProtectedRead) Recheck(ctx context.Context) error {
	if p == nil {
		return ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid(ctx) || p.destinations == nil {
		return ErrInvalid
	}
	return p.owner.withSnapshot(ctx, p.b, func(s *snapshot) error {
		destinations, err := p.destinationsRead(ctx, s)
		if err != nil {
			return err
		}
		if protectedObservation(destinations) != p.destinationsDigest {
			return ErrConflict
		}
		for _, receipt := range p.queries {
			rows, witnesses, err := p.queryRead(ctx, receipt, s)
			if err != nil {
				return err
			}
			var actual [32]byte
			if receipt.witness {
				actual = protectedObservation(witnesses)
			} else {
				actual = protectedObservation(rows)
			}
			if actual != receipt.digest {
				return ErrConflict
			}
		}
		return nil
	})
}
