package indexedread

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func protectedTestAddress(n byte) (a [20]byte) { a[19] = n; return a }
func protectedTestHash(n byte) (a [32]byte)    { a[31] = n; return a }

func TestProtectedFixedRetainedPurposes(t *testing.T) {
	f := indexFixture(t)
	a, b, c, created, contract := protectedTestAddress(1), protectedTestAddress(2), protectedTestAddress(3), protectedTestAddress(7), protectedTestAddress(9)
	for number := 1; number <= 2; number++ {
		f.exec(t, `INSERT INTO public.blocks(number,hash,parent_hash,timestamp,gas_used,gas_limit,transaction_count) VALUES($1,$2,$3,$4,0,30000000,10005)`, number, fmt.Sprintf("0x%064x", number), fmt.Sprintf("0x%064x", number-1), 1000+number)
	}
	insert := func(n byte, block, index int, from [20]byte, to *[20]byte) {
		var destination any
		if to != nil {
			destination = protectedAddress(*to)
		}
		f.exec(t, `INSERT INTO public.transactions(hash,block_number,tx_index,from_address,to_address,value,gas_used,gas_price,status) VALUES($1,$2,$3,$4,$5,0,21000,1,1)`, protectedHash(protectedTestHash(n)), block, index, protectedAddress(from), destination)
	}
	insert(11, 1, 0, a, &contract)
	insert(12, 1, 1, b, &contract)
	insert(13, 2, 0, a, nil)
	insert(14, 2, 1, b, &c)
	f.exec(t, `INSERT INTO public.contracts(address,bytecode,creator,creation_tx,block_number) VALUES($1,'0x00',$2,$3,2)`, protectedAddress(created), protectedAddress(a), protectedHash(protectedTestHash(13)))
	// The fixed purpose searches the whole retained scope. A report witness is
	// not defeated by the former 10,000 total history-transaction ceiling.
	f.exec(t, `INSERT INTO public.transactions(hash,block_number,tx_index,from_address,to_address,value,gas_used,gas_price,status)
 SELECT '0x'||lpad(to_hex(100000+n),64,'0'),2,n+2,$1,$2,0,21000,1,1 FROM generate_series(1,10001) n`, protectedAddress(a), protectedAddress(contract))
	r := f.open(t)
	p, err := r.BeginProtected(t.Context(), ProtectedSelection{Expected: r.Metadata(), Targets: [][20]byte{a, b}, FromTime: 1000, ThroughTime: 1100, FromNumber: 0, ThroughNumber: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	destinations, err := p.ReadDestinations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(destinations.Addresses) != 3 || destinations.First == nil || *destinations.First != 1 || *destinations.Last != 2 {
		t.Fatal("whole destination sample", destinations)
	}
	destinations.Addresses[0] = protectedTestAddress(99)
	predicate := ProtectedPredicate{Intervals: []ProtectedInterval{{Address: contract, FromNumber: 0, BeforeNumber: 3}}, Creations: []ProtectedCreation{{Address: created, Position: ProtectedPosition{Number: 2, Index: 0, Hash: protectedTestHash(13)}}}}
	witnesses, err := p.ReadWitnesses(t.Context(), predicate)
	if err != nil {
		t.Fatal(err)
	}
	if len(witnesses) != 2 || witnesses[0].Address != a || witnesses[0].Row.Position.Hash != protectedTestHash(11) || witnesses[1].Address != b || witnesses[1].Row.Position.Hash != protectedTestHash(12) {
		t.Fatal("strict-min participant witnesses", witnesses)
	}
	rows, end, err := p.ReadTransactions(t.Context(), predicate, ProtectedTransactionQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if end || len(rows) != 1 || rows[0].Position.Hash != protectedTestHash(11) {
		t.Fatal("exact page/lookahead", rows, end)
	}
	*rows[0].To = protectedTestAddress(98)
	*witnesses[0].Row.To = protectedTestAddress(97)
	if err = p.Recheck(t.Context()); err != nil {
		t.Fatal("unchanged retained query or detached data", err)
	}
	f.exec(t, `UPDATE public.transactions SET tx_index=2 WHERE hash=$1`, protectedHash(protectedTestHash(12)))
	if err = p.Recheck(t.Context()); !errors.Is(err, ErrConflict) {
		t.Fatal("changed witness coordinate accepted", err)
	}
	if _, _, err = p.ReadTransactions(t.Context(), predicate, ProtectedTransactionQuery{Limit: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("same purpose replayed", err)
	}
}

func TestProtectedPredicateClosedBounds(t *testing.T) {
	for _, predicate := range []ProtectedPredicate{
		{Intervals: []ProtectedInterval{{FromNumber: 4, BeforeNumber: 3}}},
		{Intervals: []ProtectedInterval{{FromNumber: 4, FromIndex: 2, BeforeNumber: 4, BeforeIndex: 2}}},
		{Creations: []ProtectedCreation{{Position: ProtectedPosition{Number: 1}}}},
	} {
		if _, err := compileProtectedPredicate(predicate); err == nil {
			t.Fatal("invalid ordinal/hash predicate")
		}
	}
	if _, err := compileProtectedPredicate(ProtectedPredicate{Intervals: make([]ProtectedInterval, maxProtectedPredicates+1)}); !errors.Is(err, ErrTooBroad) {
		t.Fatal("unbounded predicate", err)
	}
	if _, err := decodeProtectedHex("0x"+strings.Repeat("A", 40), 20); err == nil {
		t.Fatal("noncanonical indexed address")
	}
}
