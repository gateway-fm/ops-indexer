package indexedread

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

type wireOnlyFixture struct {
	pb.UnimplementedIndexedReadServiceServer
	pb.UnimplementedIndexerServiceServer
	opened         atomic.Int32
	legacyDeadline atomic.Bool
}

func (f *wireOnlyFixture) OpenCohort(ctx context.Context, _ *pb.OpenCohortRequest) (*pb.OpenCohortResponse, error) {
	f.opened.Add(1)
	// Large response intentionally exceeds HTTP/2 stream flow-control window.
	return &pb.OpenCohortResponse{Data: &pb.IndexedCohort{Rows: []*pb.IndexedRecord{{Json: make([]byte, 3<<20)}}}}, nil
}
func (f *wireOnlyFixture) GetSource(context.Context, *pb.Empty) (*pb.IndexedSource, error) {
	return &pb.IndexedSource{Ready: true}, nil
}
func (f *wireOnlyFixture) GetLatestBlockNumber(ctx context.Context, _ *pb.Empty) (*pb.LatestBlockNumber, error) {
	_, ok := ctx.Deadline()
	f.legacyDeadline.Store(ok)
	return &pb.LatestBlockNumber{Number: 7}, nil
}
func transportFixture(t *testing.T) (*Service, *wireOnlyFixture, string) {
	t.Helper()
	s := registryFixture()
	fixture := &wireOnlyFixture{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	options := append(s.ServerOptions(), grpc.MaxRecvMsgSize(MaxRecvBytes), grpc.MaxSendMsgSize(MaxSendBytes))
	server := grpc.NewServer(options...)
	pb.RegisterIndexedReadServiceServer(server, fixture)
	pb.RegisterIndexerServiceServer(server, fixture)
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
func awaitCallCount(t *testing.T, s *Service, want int) {
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
func TestIndexedTransportRetainsStalledSendAfterHandlerEndAndTTL(t *testing.T) {
	s, fixture, target := transportFixture(t)
	conn := wireClient(t, target)
	var streams []grpc.ClientStream
	for range 4 {
		stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{}, pb.IndexedReadService_OpenCohort_FullMethodName)
		if err != nil {
			t.Fatal(err)
		}
		if err = stream.SendMsg(&pb.OpenCohortRequest{}); err != nil {
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
	if _, err := pb.NewIndexedReadServiceClient(conn).GetSource(context.Background(), &pb.Empty{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("queued response reservations released at handler end", err)
	}
	// Existing V1 RPC keeps its own admission, deadline and default protobuf codec.
	legacy, err := pb.NewIndexerServiceClient(conn).GetLatestBlockNumber(context.Background(), &pb.Empty{})
	if err != nil || legacy.Number != 7 || fixture.legacyDeadline.Load() {
		t.Fatal("V1 changed by indexed transport guard", err)
	}
	time.Sleep(DefaultLimits().Timeout + 50*time.Millisecond)
	if len(s.calls) != 4 {
		t.Fatal("transport buffers became unreserved at purpose TTL")
	}
	conn.Close()
	awaitCallCount(t, s, 0)
	fresh := wireClient(t, target)
	if _, err = pb.NewIndexedReadServiceClient(fresh).GetSource(context.Background(), &pb.Empty{}); err != nil {
		t.Fatal("released wire reservation not reusable", err)
	}
}
func TestIndexedTransportDeadlineCoversRequestDecodeWithoutClientDeadline(t *testing.T) {
	s, _, target := transportFixture(t)
	conn := wireClient(t, target)
	stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{}, pb.IndexedReadService_GetSource_FullMethodName)
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
	var response pb.IndexedSource
	err = stream.RecvMsg(&response)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("server deadline did not cancel actual request decoder", err)
	}
	if elapsed := time.Since(started); elapsed > DefaultLimits().Timeout+2*time.Second {
		t.Fatal("unbounded server stream deadline", elapsed)
	}
	awaitCallCount(t, s, 0)
	<-headerSent
}

func TestIndexedTransportSmallReplyAndMarshalFailureRelease(t *testing.T) {
	s := registryFixture()
	makeLease := func() *rpcLease { s.calls <- struct{}{}; return &rpcLease{service: s, begun: true, cancel: func() {}} }
	codec := boundedCodec{base: encoding.GetCodecV2("proto")}
	lease := makeLease()
	encoded, err := codec.Marshal(&leasedReply{message: &pb.Empty{}, lease: lease})
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
	_, err = codec.Marshal(&leasedReply{message: &pb.IndexedSource{Binding: &pb.IndexedExpectedSource{SourceReference: string([]byte{255})}}, lease: lease})
	if err == nil {
		t.Fatal("invalid UTF8 unexpectedly marshaled")
	}
	lease.end()
	if len(s.calls) != 0 {
		t.Fatal("marshal failure leaked/double-reserved admission")
	}
}
