package indexedread

import (
	"bytes"
	"context"
	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
	"time"
)

func registryFixture() *Service {
	return &Service{source: &pb.IndexedSource{InstanceId: bytes.Repeat([]byte{1}, 32)}, entries: map[string]*entry{}, calls: make(chan struct{}, 4)}
}
func waitDiscarded(t *testing.T, s *Service) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		s.mu.Lock()
		n := s.reserved
		s.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("retired reservation not released")
}
func TestHandleRegistryCapacityAndPrechargedGrowth(t *testing.T) {
	s := registryFixture()
	var entries []*entry
	for range 32 {
		e, err := s.allocate(t.Context(), "protected")
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
	}
	if _, err := s.allocate(t.Context(), "cohort"); err != ErrTooBroad {
		t.Fatal("unbounded handle capacity", err)
	}
	for _, e := range entries[:3] {
		b := newBudgetForContext(e.ctx, DefaultLimits())
		if err := b.retain(16 << 20); err != nil {
			t.Fatal(err)
		}
	}
	b := newBudgetForContext(entries[3].ctx, DefaultLimits())
	before := b.charged
	if err := b.retain(16 << 20); err != ErrTooBroad || b.charged != before {
		t.Fatal("global growth was accepted/charged after failure", err)
	}
	for _, e := range entries {
		s.retire(e)
		s.retire(e)
	}
	waitDiscarded(t, s)
}
func TestHandleOriginalDeadlineKindInstanceAndSerialUse(t *testing.T) {
	s := registryFixture()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	e, err := s.allocate(ctx, "cohort")
	if err != nil {
		t.Fatal(err)
	}
	h := s.handle(e)
	if _, err = s.find(h, "protected"); err != ErrUnavailable {
		t.Fatal("wrong kind accepted")
	}
	foreign := &pb.IndexedHandle{Id: h.Id, InstanceId: bytes.Repeat([]byte{2}, 32)}
	if _, err = s.find(foreign, "cohort"); err != ErrUnavailable {
		t.Fatal("wrong process instance accepted")
	}
	owned, err := s.find(h, "cohort")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.find(h, "cohort"); err != ErrTooBroad {
		t.Fatal("parallel handle operation accepted")
	}
	owned.mu.Unlock()
	<-ctx.Done()
	waitDiscarded(t, s)
	if _, err = s.find(h, "cohort"); err != ErrUnavailable {
		t.Fatal("expired handle rebound")
	}
}
func TestIndexedRPCClosedQueriesAndPresence(t *testing.T) {
	zero := uint64(0)
	v := &pb.IndexedQuery{Family: "transactions", Block: &zero, From: &zero, RawLimit: 1, Upper: &pb.IndexedAnchor{Hash: "0x" + strings.Repeat("1", 64)}}
	q, err := decodeQuery(v)
	if err != nil {
		t.Fatal(err)
	}
	round := encodeQuery(q)
	if round.Block == nil || *round.Block != 0 || round.From == nil || round.Through != nil {
		t.Fatal("optional zero/absence lost")
	}
	for _, bad := range []*pb.IndexedQuery{{Family: "SELECT", RawLimit: 1}, {Family: "transactions", RawLimit: 1025}, {Family: "blocks", RawLimit: 1, Address: "x"}} {
		if _, err = decodeQuery(bad); err == nil {
			t.Fatal("arbitrary/oversized/invalid query accepted")
		}
	}
	v.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
	if _, err = decodeQuery(v); err == nil {
		t.Fatal("unknown query field accepted")
	}
	if _, err = decodePredicate(&pb.IndexedPredicate{Intervals: []*pb.IndexedInterval{{Address: make([]byte, 19)}}}); err == nil {
		t.Fatal("short address accepted")
	}
}
func TestIndexedRPCActiveCallBound(t *testing.T) {
	s := registryFixture()
	var release []func()
	for range 4 {
		done, err := s.enter(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		release = append(release, done)
	}
	if _, err := s.enter(t.Context()); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded active RPC", err)
	}
	for _, done := range release {
		done()
	}
}
