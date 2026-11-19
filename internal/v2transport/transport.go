// Package v2transport bounds V2 admission before decoding and retains each
// response reservation until its final transport buffer is released.
package v2transport

import (
	"context"
	"strings"
	"sync"
	"time"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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

const MaxRecvBytes = 4 << 20
const MaxSendBytes = 16 << 20
const MaxActiveCalls = 4
const MaxWaitingCalls = 32
const CallTimeout = 5 * time.Second

// Governor belongs to one gRPC server, never to a viewer or database snapshot.
// The fixed admission limits apply to the V2 service; existing V1 is unchanged.
type Governor struct {
	calls   chan struct{}
	waiting chan struct{}
}

func New() *Governor {
	return &Governor{calls: make(chan struct{}, MaxActiveCalls), waiting: make(chan struct{}, MaxWaitingCalls)}
}

func budgetError() error {
	s, err := status.New(codes.ResourceExhausted, "indexed read resource limit").WithDetails(&errdetails.ErrorInfo{Domain: "indexer.ops", Reason: "READ_BUDGET_EXCEEDED"})
	if err != nil {
		return status.Error(codes.ResourceExhausted, "indexed read resource limit")
	}
	return s.Err()
}

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
	service      *Governor
	mu           sync.Mutex
	begun, ended bool
	admitted     bool
	queued       bool
	connEnded    bool
	conn         *rpcConnection
	buffers      int
	cancel       context.CancelFunc
	release      sync.Once
}

func indexedMethod(name string) bool {
	const prefix = "/chain_indexer.v2.IndexerService/"
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	method := strings.TrimPrefix(name, prefix)
	for _, descriptor := range pb.IndexerService_ServiceDesc.Methods {
		if method == descriptor.MethodName {
			return true
		}
	}
	return false
}

func (l *rpcLease) maybeRelease() {
	if l.ended && (l.buffers == 0 || l.connEnded) {
		l.release.Do(func() {
			l.cancel()
			if l.admitted {
				<-l.service.calls
			}
			if l.queued {
				<-l.service.waiting
			}
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
func (s *Governor) transportTap(ctx context.Context, info *tap.Info) (context.Context, error) {
	if !indexedMethod(info.FullMethodName) {
		return ctx, nil
	}
	if ctx.Err() != nil {
		return ctx, status.FromContextError(ctx.Err()).Err()
	}
	bound, cancel := context.WithTimeout(ctx, CallTimeout)
	lease := &rpcLease{service: s, cancel: cancel}
	select {
	case s.calls <- struct{}{}:
		lease.admitted = true
	default:
		select {
		case s.waiting <- struct{}{}:
			lease.queued = true
		default:
			cancel()
			return ctx, budgetError()
		}
	}
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

func (lifecycleStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	lease, _ := ctx.Value(rpcLeaseKey{}).(*rpcLease)
	if lease == nil {
		return ctx
	}
	lease.mu.Lock()
	if lease.ended {
		lease.mu.Unlock()
		return ctx
	}
	// Once dispatch starts, even an expired queued request keeps its header
	// reservation until RPC End and the pre-decode registration guard rejects it.
	lease.begun = true
	if lease.admitted {
		lease.mu.Unlock()
		return ctx
	}
	lease.mu.Unlock()
	// TagRPC runs in the per-RPC dispatch goroutine before request decoding.
	// Tap must stay nonblocking because it runs in the connection I/O goroutine.
	select {
	case lease.service.calls <- struct{}{}:
		lease.mu.Lock()
		if lease.ended || ctx.Err() != nil {
			<-lease.service.calls
		} else {
			lease.admitted = true
			<-lease.service.waiting
			lease.queued = false
		}
		lease.mu.Unlock()
	case <-ctx.Done():
	}
	return ctx
}
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

// Register installs the fixed generated methods with a pre-decode admission
// check. TagRPC cannot return an error, and a canceled transport may still have
// an entire buffered request available, so context cancellation alone is not a
// sufficient guarantee that the generated decoder will not run.
func (s *Governor) Register(registrar grpc.ServiceRegistrar, implementation pb.IndexerServiceServer) {
	desc := pb.IndexerService_ServiceDesc
	desc.Methods = append([]grpc.MethodDesc(nil), desc.Methods...)
	for i := range desc.Methods {
		original := desc.Methods[i].Handler
		desc.Methods[i].Handler = func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			if err := ctx.Err(); err != nil {
				return nil, status.FromContextError(err).Err()
			}
			lease, _ := ctx.Value(rpcLeaseKey{}).(*rpcLease)
			if lease == nil || lease.service != s {
				return nil, status.Error(codes.Internal, "indexed transport unavailable")
			}
			lease.mu.Lock()
			admitted := lease.admitted && !lease.ended
			lease.mu.Unlock()
			if !admitted {
				return nil, status.Error(codes.Canceled, "indexed admission ended")
			}
			return original(server, ctx, decode, interceptor)
		}
	}
	registrar.RegisterService(&desc, implementation)
}

func (s *Governor) wrapReply(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	response, err := handler(ctx, request)
	if err != nil || !indexedMethod(info.FullMethod) {
		return response, err
	}
	lease, _ := ctx.Value(rpcLeaseKey{}).(*rpcLease)
	message, ok := response.(proto.Message)
	if lease == nil || !ok {
		return nil, status.Error(codes.Internal, "indexed transport unavailable")
	}
	// gRPC converts codec errors to INTERNAL and drops their details. Reject
	// here so a caller observes the fixed resource status before serialization.
	if proto.Size(message) > MaxSendBytes {
		return nil, budgetError()
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
		return nil, budgetError()
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
		return nil, budgetError()
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

// ServerOptions and Register must be used together. V1 methods retain their
// original contexts/admission and default protobuf codec.
func (s *Governor) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.InTapHandle(s.transportTap), grpc.StatsHandler(lifecycleStats{}), grpc.ChainUnaryInterceptor(s.wrapReply), grpc.ForceServerCodecV2(boundedCodec{encoding.GetCodecV2("proto")})}
}
