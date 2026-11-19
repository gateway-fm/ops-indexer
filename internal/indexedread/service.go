package indexedread

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	q "github.com/gateway-fm/chain-indexer/internal/indexedquery"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const WireDigest = "sha256:7044886efe529f0d1169d0ee6adf61f5c2f867761ab196ca5d50890741086eac"
const handleOverheadReserve = 4096
const maxReservedBytes = 64 << 20
const MaxRecvBytes = 4 << 20
const MaxSendBytes = 16 << 20

type ServiceConfig struct{ DSN, SourceReference, Epoch, ChainID, GenesisHash string }
type Service struct {
	pb.UnimplementedIndexedReadServiceServer
	reader   *Reader
	source   *pb.IndexedSource
	mu       sync.Mutex
	entries  map[string]*entry
	reserved int
	calls    chan struct{}
	closed   bool
}
type entry struct {
	mu        sync.Mutex
	id        []byte
	kind      string
	ctx       context.Context
	cancel    context.CancelFunc
	pass      *q.ReadPass
	protected *ProtectedRead
	once      sync.Once
	reserved  int
	reserve   func(int) bool
}

func SchemaDigestString() string { return WireDigest }
func parseDigest(raw string) ([32]byte, error) {
	var out [32]byte
	if len(raw) != 71 || raw[:7] != "sha256:" {
		return out, ErrInvalid
	}
	b, e := hex.DecodeString(raw[7:])
	if e != nil || hex.EncodeToString(b) != raw[7:] {
		return out, ErrInvalid
	}
	copy(out[:], b)
	if out == ([32]byte{}) {
		return out, ErrInvalid
	}
	return out, nil
}
func NewService(ctx context.Context, c ServiceConfig) (*Service, error) {
	if ctx == nil {
		return nil, ErrInvalid
	}
	if _, e := parseDigest(c.SourceReference); e != nil {
		return nil, ErrInvalid
	}
	epoch, e := uuid.Parse(c.Epoch)
	if e != nil || epoch == uuid.Nil || epoch.String() != c.Epoch {
		return nil, ErrInvalid
	}
	network := q.Network{ChainID: c.ChainID, GenesisHash: c.GenesisHash}
	if _, e = network.Key(); e != nil || c.GenesisHash == "0x"+string(bytes.Repeat([]byte{'0'}, 64)) {
		return nil, ErrInvalid
	}
	ref, e := ConnectionReference(c.DSN)
	if e != nil {
		return nil, e
	}
	reader, e := OpenReader(ctx, c.DSN, q.Binding{Network: network, SourceReference: ref, SchemaDigest: SchemaDigest(), ConfiguredEpoch: c.Epoch}, DefaultLimits())
	if e != nil {
		return nil, e
	}
	instance := make([]byte, 32)
	if _, e = rand.Read(instance); e != nil {
		reader.Close()
		return nil, e
	}
	return &Service{reader: reader, source: &pb.IndexedSource{Binding: &pb.IndexedExpectedSource{SourceReference: c.SourceReference, ConfiguredEpoch: c.Epoch, SchemaDigest: WireDigest, ChainId: c.ChainID, GenesisHash: c.GenesisHash}, InstanceId: instance, Ready: true}, entries: make(map[string]*entry), calls: make(chan struct{}, 4)}, nil
}
func (s *Service) CloseService() {
	s.mu.Lock()
	s.closed = true
	all := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		all = append(all, e)
	}
	s.mu.Unlock()
	for _, e := range all {
		s.retire(e)
	}
	s.reader.Close()
}
func (s *Service) enter(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, rpcError(context.Canceled)
	}
	if ctx.Err() != nil {
		return nil, rpcError(ctx.Err())
	}
	if lease, _ := ctx.Value(rpcLeaseKey{}).(*rpcLease); lease != nil && lease.service == s {
		return func() {}, nil
	}
	select {
	case s.calls <- struct{}{}:
		return func() { <-s.calls }, nil
	default:
		return nil, status.Error(codes.ResourceExhausted, "indexed read busy")
	}
}
func rpcError(e error) error {
	switch {
	case e == nil:
		return nil
	case errors.Is(e, context.Canceled):
		return status.Error(codes.Canceled, "indexed read canceled")
	case errors.Is(e, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "indexed read expired")
	case errors.Is(e, ErrInvalid) || errors.Is(e, q.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid indexed read")
	case errors.Is(e, ErrTooBroad) || errors.Is(e, q.ErrTooBroad):
		return status.Error(codes.ResourceExhausted, "indexed read exceeds limits")
	case errors.Is(e, ErrConflict) || errors.Is(e, q.ErrConflict):
		return status.Error(codes.Aborted, "indexed read changed")
	default:
		return status.Error(codes.Unavailable, "indexed read unavailable")
	}
}
func (s *Service) expected(v *pb.IndexedExpectedSource) bool {
	return v != nil && len(v.ProtoReflect().GetUnknown()) == 0 && proto.Equal(v, s.source.Binding)
}
func (s *Service) allocate(ctx context.Context, kind string) (*entry, error) {
	d := time.Now().Add(DefaultLimits().Timeout)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(d) {
		d = deadline
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.entries) >= 32 || s.reserved > maxReservedBytes-handleOverheadReserve {
		return nil, ErrTooBroad
	}
	id := make([]byte, 32)
	if _, e := rand.Read(id); e != nil {
		return nil, ErrUnavailable
	}
	own, cancel := context.WithDeadline(context.Background(), d)
	e := &entry{id: id, kind: kind, cancel: cancel, reserved: handleOverheadReserve}
	e.reserve = func(n int) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if n < 0 || n > maxReservedBytes-s.reserved || e.ctx.Err() != nil {
			return false
		}
		s.reserved += n
		e.reserved += n
		return true
	}
	e.ctx = context.WithValue(own, retentionKey{}, e.reserve)
	s.entries[string(id)] = e
	s.reserved += handleOverheadReserve
	context.AfterFunc(own, func() { s.retire(e) })
	return e, nil
}
func (s *Service) retire(e *entry) {
	if e == nil {
		return
	}
	e.once.Do(func() {
		s.mu.Lock()
		delete(s.entries, string(e.id))
		s.mu.Unlock()
		e.cancel()
		// A canceled in-flight operation still owns its buffers until it exits.
		go func() {
			e.mu.Lock()
			if e.protected != nil {
				e.protected.Close()
				e.protected = nil
			}
			e.pass = nil
			e.mu.Unlock()
			s.mu.Lock()
			s.reserved -= e.reserved
			s.mu.Unlock()
		}()
	})
}
func (s *Service) handle(e *entry) *pb.IndexedHandle {
	return &pb.IndexedHandle{InstanceId: bytes.Clone(s.source.InstanceId), Id: bytes.Clone(e.id)}
}
func (s *Service) find(h *pb.IndexedHandle, kind string) (*entry, error) {
	if h == nil || len(h.ProtoReflect().GetUnknown()) != 0 || len(h.Id) != 32 || !bytes.Equal(h.InstanceId, s.source.InstanceId) {
		return nil, ErrUnavailable
	}
	s.mu.Lock()
	e := s.entries[string(h.Id)]
	s.mu.Unlock()
	if e == nil || e.kind != kind || e.ctx.Err() != nil {
		return nil, ErrUnavailable
	}
	if !e.mu.TryLock() {
		return nil, ErrTooBroad
	}
	if e.ctx.Err() != nil {
		e.mu.Unlock()
		return nil, ErrUnavailable
	}
	return e, nil
}
func (s *Service) GetSource(ctx context.Context, request *pb.Empty) (*pb.IndexedSource, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, rpcError(ErrInvalid)
	}
	done, e := s.enter(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if e = s.reader.CheckReady(ctx); e != nil {
		return nil, rpcError(e)
	}
	return proto.Clone(s.source).(*pb.IndexedSource), nil
}
func (s *Service) OpenCohort(ctx context.Context, r *pb.OpenCohortRequest) (out *pb.OpenCohortResponse, err error) {
	done, e := s.enter(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 || !s.expected(r.Expected) {
		return nil, rpcError(ErrInvalid)
	}
	query, e := decodeQuery(r.Query)
	if e != nil {
		return nil, rpcError(e)
	}
	own, e := s.allocate(ctx, "cohort")
	if e != nil {
		return nil, rpcError(e)
	}
	own.mu.Lock()
	defer own.mu.Unlock()
	defer func() {
		if err != nil {
			s.retire(own)
		}
	}()
	query.Fence = q.NewFence(own.ctx)
	pass, e := s.reader.ReadCohort(context.WithValue(ctx, retentionKey{}, own.reserve), query)
	if e != nil {
		return nil, rpcError(e)
	}
	own.pass = pass
	state, e := pass.StateFor(s.reader.metadata.Identity)
	if e != nil {
		return nil, rpcError(e)
	}
	original, ok := state.(*cohortState)
	if !ok || original.pass != pass {
		return nil, rpcError(ErrUnavailable)
	}
	if e = original.budget.retain(2 * pass.WireCopyBytes()); e != nil {
		return nil, rpcError(e)
	}
	out = &pb.OpenCohortResponse{Handle: s.handle(own), Query: encodeQuery(pass.Query()), Data: encodeData(pass.Data())}
	if proto.Size(out) > MaxSendBytes {
		return nil, rpcError(ErrTooBroad)
	}
	return out, nil
}
func (s *Service) RecheckCohort(ctx context.Context, r *pb.IndexedHandleRequest) (*pb.Empty, error) {
	done, e := s.enter(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, rpcError(ErrInvalid)
	}
	own, e := s.find(r.Handle, "cohort")
	if e != nil {
		return nil, rpcError(e)
	}
	defer own.mu.Unlock()
	if own.pass == nil {
		return nil, rpcError(ErrUnavailable)
	}
	if e = s.reader.Recheck(ctx, own.pass); e != nil {
		s.retire(own)
		return nil, rpcError(e)
	}
	return &pb.Empty{}, nil
}
func (s *Service) OpenProtected(ctx context.Context, r *pb.OpenProtectedRequest) (out *pb.IndexedHandle, err error) {
	done, e := s.enter(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 || !s.expected(r.Expected) {
		return nil, rpcError(ErrInvalid)
	}
	selection, e := decodeSelection(r.Selection)
	if e != nil {
		return nil, rpcError(e)
	}
	own, e := s.allocate(ctx, "protected")
	if e != nil {
		return nil, rpcError(e)
	}
	own.mu.Lock()
	defer own.mu.Unlock()
	defer func() {
		if err != nil {
			s.retire(own)
		}
	}()
	selection.Expected = s.reader.Metadata()
	own.protected, e = s.reader.BeginProtected(own.ctx, selection)
	if e != nil {
		return nil, rpcError(e)
	}
	return s.handle(own), nil
}
func (s *Service) ReadDestinations(ctx context.Context, r *pb.IndexedHandleRequest) (out *pb.IndexedDestinations, err error) {
	done, e := s.enter(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, rpcError(ErrInvalid)
	}
	own, e := s.find(r.Handle, "protected")
	if e != nil {
		return nil, rpcError(e)
	}
	defer own.mu.Unlock()
	defer func() {
		if err != nil {
			s.retire(own)
		}
	}()
	value, e := own.protected.ReadDestinations(ctx)
	if e != nil {
		return nil, rpcError(e)
	}
	if e = own.protected.b.retain(2 * (128 + len(value.Addresses)*64)); e != nil {
		return nil, rpcError(e)
	}
	out = &pb.IndexedDestinations{First: value.First, Last: value.Last}
	for _, a := range value.Addresses {
		out.Addresses = append(out.Addresses, bytes.Clone(a[:]))
	}
	return out, nil
}
func (s *Service) ReadWitnesses(ctx context.Context, r *pb.IndexedWitnessRequest) (out *pb.IndexedWitnesses, err error) {
	done, e := s.enter(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, rpcError(ErrInvalid)
	}
	predicate, e := decodePredicate(r.Predicate)
	if e != nil {
		return nil, rpcError(e)
	}
	own, e := s.find(r.Handle, "protected")
	if e != nil {
		return nil, rpcError(e)
	}
	defer own.mu.Unlock()
	defer func() {
		if err != nil {
			s.retire(own)
		}
	}()
	value, e := own.protected.ReadWitnesses(ctx, predicate)
	if e != nil {
		return nil, rpcError(e)
	}
	if e = own.protected.b.retain(2 * (128 + len(value)*384)); e != nil {
		return nil, rpcError(e)
	}
	out = &pb.IndexedWitnesses{}
	for _, w := range value {
		out.Witnesses = append(out.Witnesses, &pb.IndexedWitness{Address: bytes.Clone(w.Address[:]), Row: encodeRow(w.Row)})
	}
	if proto.Size(out) > MaxSendBytes {
		return nil, rpcError(ErrTooBroad)
	}
	return out, nil
}
func (s *Service) ReadTransactions(ctx context.Context, r *pb.IndexedTransactionsRequest) (out *pb.IndexedTransactions, err error) {
	done, e := s.enter(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, rpcError(ErrInvalid)
	}
	predicate, e := decodePredicate(r.Predicate)
	if e != nil {
		return nil, rpcError(e)
	}
	query, e := decodeTransactionQuery(r.Query)
	if e != nil {
		return nil, rpcError(e)
	}
	own, e := s.find(r.Handle, "protected")
	if e != nil {
		return nil, rpcError(e)
	}
	defer own.mu.Unlock()
	defer func() {
		if err != nil {
			s.retire(own)
		}
	}()
	rows, end, e := own.protected.ReadTransactions(ctx, predicate, query)
	if e != nil {
		return nil, rpcError(e)
	}
	if e = own.protected.b.retain(2 * (128 + len(rows)*320)); e != nil {
		return nil, rpcError(e)
	}
	out = &pb.IndexedTransactions{Exhausted: end}
	for _, row := range rows {
		out.Rows = append(out.Rows, encodeRow(row))
	}
	if proto.Size(out) > MaxSendBytes {
		return nil, rpcError(ErrTooBroad)
	}
	return out, nil
}
func (s *Service) RecheckProtected(ctx context.Context, r *pb.IndexedHandleRequest) (*pb.Empty, error) {
	done, e := s.enter(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, rpcError(ErrInvalid)
	}
	own, e := s.find(r.Handle, "protected")
	if e != nil {
		return nil, rpcError(e)
	}
	defer own.mu.Unlock()
	if e = own.protected.Recheck(ctx); e != nil {
		s.retire(own)
		return nil, rpcError(e)
	}
	return &pb.Empty{}, nil
}
func (s *Service) Close(ctx context.Context, r *pb.IndexedHandleRequest) (*pb.Empty, error) {
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 || r.Handle == nil || len(r.Handle.ProtoReflect().GetUnknown()) != 0 || len(r.Handle.Id) != 32 || !bytes.Equal(r.Handle.InstanceId, s.source.InstanceId) {
		return nil, rpcError(ErrUnavailable)
	}
	s.mu.Lock()
	own := s.entries[string(r.Handle.Id)]
	s.mu.Unlock()
	if own == nil {
		return nil, rpcError(ErrUnavailable)
	}
	s.retire(own)
	return &pb.Empty{}, nil
}
