package indexedread

import (
	"context"
	"sync"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"google.golang.org/protobuf/proto"
)

type rpcLeaseKey struct{}
type rpcConnKey struct{}
type rpcConnection struct {
	mu     sync.Mutex
	ended  bool
	leases map[*rpcLease]struct{}
}

func (c *rpcConnection) end() {
	c.mu.Lock()
	c.ended = true
	all := make([]*rpcLease, 0, len(c.leases))
	for l := range c.leases {
		all = append(all, l)
	}
	c.mu.Unlock()
	for _, l := range all {
		l.mu.Lock()
		l.connEnded = true
		l.maybeRelease()
		l.mu.Unlock()
	}
}

type rpcLease struct {
	service      *Service
	mu           sync.Mutex
	begun, ended bool
	connEnded    bool
	conn         *rpcConnection
	buffers      int
	cancel       context.CancelFunc
	release      sync.Once
}

func indexedMethod(name string) bool {
	switch name {
	case pb.IndexedReadService_GetSource_FullMethodName, pb.IndexedReadService_OpenCohort_FullMethodName, pb.IndexedReadService_RecheckCohort_FullMethodName, pb.IndexedReadService_OpenProtected_FullMethodName, pb.IndexedReadService_ReadDestinations_FullMethodName, pb.IndexedReadService_ReadWitnesses_FullMethodName, pb.IndexedReadService_ReadTransactions_FullMethodName, pb.IndexedReadService_RecheckProtected_FullMethodName, pb.IndexedReadService_Close_FullMethodName:
		return true
	}
	return false
}
func (l *rpcLease) maybeRelease() {
	if l.ended && (l.buffers == 0 || l.connEnded) {
		l.release.Do(func() {
			l.cancel()
			<-l.service.calls
			if l.conn != nil {
				l.conn.mu.Lock()
				delete(l.conn.leases, l)
				l.conn.mu.Unlock()
			}
		})
	}
}
func (l *rpcLease) end()         { l.mu.Lock(); l.ended = true; l.maybeRelease(); l.mu.Unlock() }
func (l *rpcLease) bufferStart() { l.mu.Lock(); l.buffers++; l.mu.Unlock() }
func (l *rpcLease) bufferDone()  { l.mu.Lock(); l.buffers--; l.maybeRelease(); l.mu.Unlock() }
func (s *Service) transportTap(ctx context.Context, info *tap.Info) (context.Context, error) {
	if !indexedMethod(info.FullMethodName) {
		return ctx, nil
	}
	if ctx.Err() != nil {
		return ctx, status.FromContextError(ctx.Err()).Err()
	}
	select {
	case s.calls <- struct{}{}:
	default:
		return ctx, status.Error(codes.ResourceExhausted, "indexed read busy")
	}
	bound, cancel := context.WithTimeout(ctx, DefaultLimits().Timeout)
	lease := &rpcLease{service: s, cancel: cancel}
	if conn, _ := ctx.Value(rpcConnKey{}).(*rpcConnection); conn != nil {
		lease.conn = conn
		conn.mu.Lock()
		lease.connEnded = conn.ended
		conn.leases[lease] = struct{}{}
		conn.mu.Unlock()
	}
	context.AfterFunc(bound, func() {
		lease.mu.Lock()
		if !lease.begun {
			lease.ended = true
			lease.maybeRelease()
		}
		lease.mu.Unlock()
	})
	// InTapHandle runs before transport caches ctx.Done for receive/write quota.
	return context.WithValue(bound, rpcLeaseKey{}, lease), nil
}

type lifecycleStats struct{}

func (lifecycleStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (lifecycleStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	lease, _ := ctx.Value(rpcLeaseKey{}).(*rpcLease)
	if lease == nil {
		return
	}
	switch event.(type) {
	case *stats.Begin:
		lease.mu.Lock()
		lease.begun = true
		lease.mu.Unlock()
	case *stats.End:
		lease.end()
	}
}
func (lifecycleStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, rpcConnKey{}, &rpcConnection{leases: make(map[*rpcLease]struct{})})
}
func (lifecycleStats) HandleConn(ctx context.Context, event stats.ConnStats) {
	if _, ok := event.(*stats.ConnEnd); ok {
		if conn, _ := ctx.Value(rpcConnKey{}).(*rpcConnection); conn != nil {
			conn.end()
		}
	}
}

type leasedReply struct {
	message proto.Message
	lease   *rpcLease
}

func (s *Service) wrapReply(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	response, err := handler(ctx, request)
	if err != nil || !indexedMethod(info.FullMethod) {
		return response, err
	}
	lease, _ := ctx.Value(rpcLeaseKey{}).(*rpcLease)
	message, ok := response.(proto.Message)
	if lease == nil || !ok {
		return nil, status.Error(codes.Internal, "indexed transport unavailable")
	}
	return &leasedReply{message, lease}, nil
}

// A pool callback runs only after gRPC's last transport Buffer reference is
// freed. stats.End alone can precede a flow-controlled queue draining. ConnEnd
// is the alternative terminal barrier: the closed connection cannot send its
// queued payload; RPC End additionally waits for the handler/marshal to exit.
type releasePool struct{ lease *rpcLease }

func (p releasePool) Get(n int) *[]byte { b := make([]byte, n); return &b }
func (p releasePool) Put(b *[]byte)     { *b = nil; p.lease.bufferDone() }

type boundedCodec struct{ base encoding.CodecV2 }

func (c boundedCodec) Name() string                             { return c.base.Name() }
func (c boundedCodec) Unmarshal(b mem.BufferSlice, v any) error { return c.base.Unmarshal(b, v) }
func (c boundedCodec) Marshal(v any) (mem.BufferSlice, error) {
	reply, ok := v.(*leasedReply)
	if !ok {
		return c.base.Marshal(v)
	}
	size := proto.Size(reply.message)
	if size > MaxSendBytes {
		return nil, status.Error(codes.ResourceExhausted, "indexed response exceeds limit")
	}
	capacity := size
	// Small SliceBuffers have no reference-counted Free callback. Pad capacity
	// (not wire length) above the pinned gRPC pooling threshold for every reply.
	for mem.IsBelowBufferPoolingThreshold(capacity) {
		if capacity == 0 {
			capacity = 1
		} else {
			capacity *= 2
		}
	}
	if capacity > MaxSendBytes {
		return nil, status.Error(codes.ResourceExhausted, "indexed response exceeds limit")
	}
	reply.lease.bufferStart()
	raw := make([]byte, 0, capacity)
	data, err := (proto.MarshalOptions{}).MarshalAppend(raw, reply.message)
	if err != nil {
		reply.lease.bufferDone()
		return nil, err
	}
	return mem.BufferSlice{mem.NewBuffer(&data, releasePool{reply.lease})}, nil
}

// ServerOptions must be installed wherever this service is registered. V1
// methods retain their original contexts/admission and default protobuf codec.
func (s *Service) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.InTapHandle(s.transportTap), grpc.StatsHandler(lifecycleStats{}), grpc.ChainUnaryInterceptor(s.wrapReply), grpc.ForceServerCodecV2(boundedCodec{encoding.GetCodecV2("proto")})}
}
