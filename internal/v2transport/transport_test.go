package v2transport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	legacy "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type wireOnlyFixture struct {
	pb.UnimplementedIndexerServiceServer
	opened         atomic.Int32
	oversize       atomic.Bool
	legacyDeadline atomic.Bool
	readinessGate  <-chan struct{}
	readinessCalls atomic.Int32
	readinessLive  atomic.Int32
	readinessMax   atomic.Int32
}

func (f *wireOnlyFixture) ListTransactions(ctx context.Context, _ *pb.ListTransactionsRequest) (*pb.ListTransactionsResponse, error) {
	f.opened.Add(1)
	if f.oversize.Load() {
		return &pb.ListTransactionsResponse{Transactions: []*pb.Transaction{{InputData: make([]byte, MaxSendBytes+1)}}}, nil
	}
	// Large response intentionally exceeds HTTP/2 stream flow-control window.
	return &pb.ListTransactionsResponse{Transactions: []*pb.Transaction{{InputData: make([]byte, 3<<20)}}}, nil
}
func (f *wireOnlyFixture) GetChainView(ctx context.Context, _ *pb.GetChainViewRequest) (*pb.GetChainViewResponse, error) {
	f.readinessCalls.Add(1)
	live := f.readinessLive.Add(1)
	defer f.readinessLive.Add(-1)
	for prior := f.readinessMax.Load(); live > prior; prior = f.readinessMax.Load() {
		if f.readinessMax.CompareAndSwap(prior, live) {
			break
		}
	}
	if f.readinessGate != nil {
		select {
		case <-f.readinessGate:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return &pb.GetChainViewResponse{ChainId: "31337"}, nil
}

type legacyFixture struct {
	legacy.UnimplementedIndexerServiceServer
	owner *wireOnlyFixture
}

func (f *legacyFixture) GetLatestBlockNumber(ctx context.Context, _ *legacy.Empty) (*legacy.LatestBlockNumber, error) {
	_, ok := ctx.Deadline()
	f.owner.legacyDeadline.Store(ok)
	return &legacy.LatestBlockNumber{Number: 7}, nil
}
func transportFixture(t *testing.T) (*Governor, *wireOnlyFixture, string) {
	t.Helper()
	s := New()
	fixture := &wireOnlyFixture{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	options := append(s.ServerOptions(), grpc.MaxRecvMsgSize(MaxRecvBytes), grpc.MaxSendMsgSize(MaxSendBytes))
	server := grpc.NewServer(options...)
	s.Register(server, fixture)
	legacy.RegisterIndexerServiceServer(server, &legacyFixture{owner: fixture})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return s, fixture, listener.Addr().String()
}
func wireClient(t *testing.T, target string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithInitialWindowSize(64<<10), grpc.WithInitialConnWindowSize(16<<20), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxSendBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
func awaitCallCount(t *testing.T, s *Governor, want int) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		if len(s.calls) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("RPC reservations got %d want %d", len(s.calls), want)
}
func awaitWaitingCount(t *testing.T, s *Governor, want int) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		if len(s.waiting) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiting RPC reservations got %d want %d", len(s.waiting), want)
}
func TestIndexedTransportEightReadinessRequestsQueueBeforeDecode(t *testing.T) {
	s, fixture, target := transportFixture(t)
	gate := make(chan struct{})
	fixture.readinessGate = gate
	conn := wireClient(t, target)
	client := pb.NewIndexerServiceClient(conn)
	var ready, done sync.WaitGroup
	ready.Add(8)
	done.Add(8)
	start := make(chan struct{})
	results := make(chan error, 8)
	for range 8 {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			ctx, cancel := context.WithTimeout(t.Context(), CallTimeout)
			defer cancel()
			source, err := client.GetChainView(ctx, &pb.GetChainViewRequest{})
			if err == nil && source.ChainId != "31337" {
				t.Error("readiness response not ready")
			}
			results <- err
		}()
	}
	ready.Wait()
	close(start)
	awaitCallCount(t, s, 4)
	awaitWaitingCount(t, s, 4)
	legacy, err := legacy.NewIndexerServiceClient(conn).GetLatestBlockNumber(t.Context(), &legacy.Empty{})
	if err != nil || legacy.Number != 7 || fixture.legacyDeadline.Load() {
		t.Fatal("queued indexed requests blocked or changed legacy RPC", err)
	}
	close(gate)
	done.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal("finite eight-request readiness burst failed", err)
		}
	}
	awaitCallCount(t, s, 0)
	awaitWaitingCount(t, s, 0)
	if fixture.readinessCalls.Load() != 8 || fixture.readinessMax.Load() > 4 {
		t.Fatal("readiness burst changed active-call limit", fixture.readinessCalls.Load(), fixture.readinessMax.Load())
	}
}
func TestIndexedTransportQueueCapacityCancellationAndDeadline(t *testing.T) {
	s, fixture, target := transportFixture(t)
	gate := make(chan struct{})
	fixture.readinessGate = gate
	conn := wireClient(t, target)
	client := pb.NewIndexerServiceClient(conn)
	activeResults := make(chan error, 4)
	for range 4 {
		go func() { _, err := client.GetChainView(t.Context(), &pb.GetChainViewRequest{}); activeResults <- err }()
	}
	awaitCallCount(t, s, 4)
	short, stopShort := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer stopShort()
	if _, err := client.GetChainView(short, &pb.GetChainViewRequest{}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("queued client deadline was not preserved", err)
	}
	awaitWaitingCount(t, s, 0)
	queued, cancelQueued := context.WithCancel(t.Context())
	defer cancelQueued()
	queuedResults := make(chan error, MaxWaitingCalls)
	for range MaxWaitingCalls {
		go func() { _, err := client.GetChainView(queued, &pb.GetChainViewRequest{}); queuedResults <- err }()
	}
	awaitWaitingCount(t, s, MaxWaitingCalls)
	if _, err := client.GetChainView(t.Context(), &pb.GetChainViewRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded admission wait queue", err)
	}
	legacy, err := legacy.NewIndexerServiceClient(conn).GetLatestBlockNumber(t.Context(), &legacy.Empty{})
	if err != nil || legacy.Number != 7 || fixture.legacyDeadline.Load() {
		t.Fatal("full indexed queue blocked or changed legacy RPC", err)
	}
	cancelQueued()
	for range MaxWaitingCalls {
		if err := <-queuedResults; status.Code(err) != codes.Canceled {
			t.Fatal("queued cancellation did not retire request", err)
		}
	}
	awaitWaitingCount(t, s, 0)
	if fixture.readinessCalls.Load() != 4 {
		t.Fatal("unadmitted canceled/expired request reached handler", fixture.readinessCalls.Load())
	}
	close(gate)
	for range 4 {
		if err := <-activeResults; err != nil {
			t.Fatal("active request damaged by queued cancellation", err)
		}
	}
	awaitCallCount(t, s, 0)
	if _, err := client.GetChainView(t.Context(), &pb.GetChainViewRequest{}); err != nil {
		t.Fatal("queue cancellation leaked admission", err)
	}
}

type capturedService struct{ desc *grpc.ServiceDesc }

func (c *capturedService) RegisterService(desc *grpc.ServiceDesc, _ any) { c.desc = desc }
func TestIndexedTransportCanceledBufferedRequestRejectedBeforeDecode(t *testing.T) {
	s := New()
	fixture := &wireOnlyFixture{}
	registered := &capturedService{}
	s.Register(registered, fixture)
	for _, method := range registered.desc.Methods {
		if method.MethodName != "GetChainView" {
			continue
		}
		for _, canceled := range []bool{false, true} {
			ctx, cancel := context.WithCancel(t.Context())
			lease := &rpcLease{service: s, begun: true, queued: true}
			ctx = context.WithValue(ctx, rpcLeaseKey{}, lease)
			if canceled {
				cancel()
			}
			decoded := false
			_, err := method.Handler(fixture, ctx, func(any) error { decoded = true; return nil }, nil)
			cancel()
			if decoded || status.Code(err) != codes.Canceled || fixture.readinessCalls.Load() != 0 {
				t.Fatal("unadmitted buffered request reached decoder/handler", canceled, decoded, err)
			}
		}
		return
	}
	t.Fatal("fixed GetChainView method missing")
}
func TestIndexedTransportQueueAdmissionKeepsOriginalDeadline(t *testing.T) {
	s := New()
	for range 4 {
		s.calls <- struct{}{}
	}
	parent, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ctx, err := s.transportTap(parent, &tap.Info{FullMethodName: pb.IndexerService_GetChainView_FullMethodName})
	if err != nil {
		t.Fatal(err)
	}
	original, _ := ctx.Deadline()
	dispatched := make(chan context.Context, 1)
	go func() { dispatched <- (lifecycleStats{}).TagRPC(ctx, &stats.RPCTagInfo{}) }()
	awaitWaitingCount(t, s, 1)
	<-s.calls
	admitted := <-dispatched
	deadline, _ := admitted.Deadline()
	parentDeadline, _ := parent.Deadline()
	if deadline != original || deadline != parentDeadline {
		t.Fatal("queue admission reset original deadline")
	}
	lease := admitted.Value(rpcLeaseKey{}).(*rpcLease)
	lease.end()
	awaitWaitingCount(t, s, 0)
	if len(s.calls) != 3 {
		t.Fatal("queue conversion leaked or doubled active reservation")
	}
}
func TestIndexedTransportRetainsStalledSendAfterHandlerEndAndTTL(t *testing.T) {
	s, fixture, target := transportFixture(t)
	conn := wireClient(t, target)
	var streams []grpc.ClientStream
	for range 4 {
		stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{}, pb.IndexerService_ListTransactions_FullMethodName)
		if err != nil {
			t.Fatal(err)
		}
		if err = stream.SendMsg(&pb.ListTransactionsRequest{}); err != nil {
			t.Fatal(err)
		}
		if err = stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, stream)
	}
	awaitCallCount(t, s, 4)
	until := time.Now().Add(2 * time.Second)
	for fixture.opened.Load() != 4 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if fixture.opened.Load() != 4 {
		t.Fatal("not all transport fixture handlers completed")
	}
	time.Sleep(30 * time.Millisecond)
	waitingResult := make(chan error, 1)
	go func() {
		_, err := pb.NewIndexerServiceClient(conn).GetChainView(context.Background(), &pb.GetChainViewRequest{})
		waitingResult <- err
	}()
	awaitWaitingCount(t, s, 1)
	// Existing V1 RPC keeps its own admission, deadline and default protobuf codec.
	legacy, err := legacy.NewIndexerServiceClient(conn).GetLatestBlockNumber(context.Background(), &legacy.Empty{})
	if err != nil || legacy.Number != 7 || fixture.legacyDeadline.Load() {
		t.Fatal("V1 changed by indexed transport guard", err)
	}
	if err := <-waitingResult; status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("queued response reservations released at handler end or queue deadline reset", err)
	}
	if len(s.calls) != 4 {
		t.Fatal("transport buffers became unreserved at purpose TTL")
	}
	conn.Close()
	awaitCallCount(t, s, 0)
	fresh := wireClient(t, target)
	if _, err = pb.NewIndexerServiceClient(fresh).GetChainView(context.Background(), &pb.GetChainViewRequest{}); err != nil {
		t.Fatal("released wire reservation not reusable", err)
	}
}
func TestIndexedTransportDeadlineCoversRequestDecodeWithoutClientDeadline(t *testing.T) {
	s, _, target := transportFixture(t)
	conn := wireClient(t, target)
	stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{}, pb.IndexerService_GetChainView_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	// Force headers onto the transport without providing an entire request body.
	// A partial gRPC message stalls the actual stream decoder, not our handler.
	headerSent := make(chan struct{})
	go func() { _, _ = stream.Header(); close(headerSent) }()
	// SendMsg alone sends one complete message, so use a zero-message stream with
	// the gRPC client header flush initiated by Header() above.
	awaitCallCount(t, s, 1)
	started := time.Now()
	var response pb.GetChainViewResponse
	err = stream.RecvMsg(&response)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("server deadline did not cancel actual request decoder", err)
	}
	if elapsed := time.Since(started); elapsed > CallTimeout+2*time.Second {
		t.Fatal("unbounded server stream deadline", elapsed)
	}
	awaitCallCount(t, s, 0)
	<-headerSent
}

func TestIndexedTransportSmallReplyAndMarshalFailureRelease(t *testing.T) {
	s := New()
	makeLease := func() *rpcLease {
		s.calls <- struct{}{}
		return &rpcLease{service: s, begun: true, admitted: true, cancel: func() {}}
	}
	codec := boundedCodec{base: encoding.GetCodecV2("proto")}
	lease := makeLease()
	encoded, err := codec.Marshal(&leasedReply{message: &pb.GetChainViewRequest{}, lease: lease})
	if err != nil {
		t.Fatal(err)
	}
	lease.end()
	if len(s.calls) != 1 {
		t.Fatal("small reply bypassed transport buffer reservation")
	}
	encoded.Free()
	if len(s.calls) != 0 {
		t.Fatal("small refcounted buffer callback did not release")
	}
	lease = makeLease()
	_, err = codec.Marshal(&leasedReply{message: &pb.GetChainViewResponse{ChainId: string([]byte{255})}, lease: lease})
	if err == nil {
		t.Fatal("invalid UTF8 unexpectedly marshaled")
	}
	lease.end()
	if len(s.calls) != 0 {
		t.Fatal("marshal failure leaked/double-reserved admission")
	}
}

func TestEveryV2MethodIsGuardedBeforeDecode(t *testing.T) {
	s := New()
	registered := &capturedService{}
	s.Register(registered, &wireOnlyFixture{})
	if len(registered.desc.Methods) != 20 {
		t.Fatalf("unexpected V2 surface: %d methods", len(registered.desc.Methods))
	}
	for _, method := range registered.desc.Methods {
		name := "/" + registered.desc.ServiceName + "/" + method.MethodName
		if !indexedMethod(name) {
			t.Fatalf("unguarded method %s", name)
		}
		decoded := false
		_, err := method.Handler(&wireOnlyFixture{}, t.Context(), func(any) error { decoded = true; return nil }, nil)
		if decoded || status.Code(err) != codes.Internal {
			t.Fatalf("method %s decoded without admission", name)
		}
	}
	if indexedMethod("/chain_indexer.v1.IndexerService/ListTransactions") || indexedMethod("/chain_indexer.v2.IndexerService/Unknown") {
		t.Fatal("admission matched an unregistered V2 method or a V1 method")
	}
}

func TestOversizeReplyHasFixedReasonAndReleasesAdmission(t *testing.T) {
	s := New()
	s.calls <- struct{}{}
	lease := &rpcLease{service: s, begun: true, admitted: true, cancel: func() {}}
	codec := boundedCodec{base: encoding.GetCodecV2("proto")}
	_, err := codec.Marshal(&leasedReply{message: &pb.ListTransactionsResponse{Transactions: []*pb.Transaction{{InputData: make([]byte, MaxSendBytes+1)}}}, lease: lease})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversize encoded response did not fail", err)
	}
	var reason bool
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "indexer.ops" && info.Reason == "READ_BUDGET_EXCEEDED" {
			reason = true
		}
	}
	if !reason {
		t.Fatal("missing fixed budget error reason")
	}
	lease.end()
	if len(s.calls) != 0 || lease.buffers != 0 {
		t.Fatal("oversize rejection leaked an admission lease")
	}
}

func TestOversizeReplyPreservesFixedErrorOverActualTCP(t *testing.T) {
	s, fixture, target := transportFixture(t)
	fixture.oversize.Store(true)
	client := pb.NewIndexerServiceClient(wireClient(t, target))
	_, err := client.ListTransactions(t.Context(), &pb.ListTransactionsRequest{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversize response changed its status on the wire", err)
	}
	var reason bool
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "indexer.ops" && info.Reason == "READ_BUDGET_EXCEEDED" {
			reason = true
		}
	}
	if !reason {
		t.Fatal("oversize wire response lost its fixed reason")
	}
	awaitCallCount(t, s, 0)
	if _, err := client.GetChainView(t.Context(), &pb.GetChainViewRequest{}); err != nil {
		t.Fatal("oversize rejection damaged subsequent admission", err)
	}
}
