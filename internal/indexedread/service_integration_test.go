package indexedread

import (
	"context"
	"encoding/json"
	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"net"
	"strings"
	"testing"
	"time"
)

func sourceRPCFixture(t *testing.T) (*pgFixture, *Service, pb.IndexedReadServiceClient) {
	t.Helper()
	f := indexFixture(t)
	s, e := NewService(t.Context(), ServiceConfig{DSN: f.dsn, SourceReference: "sha256:" + strings.Repeat("1", 64), Epoch: f.binding.ConfiguredEpoch, ChainID: "1", GenesisHash: f.binding.Network.GenesisHash})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.CloseService)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	options := append(s.ServerOptions(), grpc.MaxRecvMsgSize(MaxRecvBytes), grpc.MaxSendMsgSize(MaxSendBytes))
	server := grpc.NewServer(options...)
	pb.RegisterIndexedReadServiceServer(server, s)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, e := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxSendBytes)))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { conn.Close() })
	return f, s, pb.NewIndexedReadServiceClient(conn)
}
func TestActualIndexedRPCCohortRecheckContextAndLosslessValue(t *testing.T) {
	f, s, c := sourceRPCFixture(t)
	hash := func(v string) string { return "0x" + strings.Repeat(v, 64) }
	address := "0x" + strings.Repeat("2", 40)
	f.exec(t, `INSERT INTO blocks(number,hash,parent_hash,timestamp,gas_used,gas_limit,transaction_count)VALUES(1,$1,$1,1,0,30000000,1)`, hash("1"))
	f.exec(t, `INSERT INTO transactions(hash,block_number,tx_index,from_address,to_address,value,gas_used,gas_price,status,input_data)VALUES($1,1,0,$2,$2,340282366920938463463374607431768211456,21000,1,1,'0x')`, hash("3"), address)
	source, e := c.GetSource(t.Context(), &pb.Empty{})
	if e != nil || !source.Ready {
		t.Fatal(e)
	}
	opened, e := c.OpenCohort(t.Context(), &pb.OpenCohortRequest{Expected: source.Binding, Query: &pb.IndexedQuery{Family: "transactions", RawLimit: 1}})
	if e != nil {
		t.Fatal(e)
	}
	if len(opened.Data.Rows) != 1 || len(opened.Data.Context) == 0 {
		t.Fatal("missing row/context")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(opened.Data.Rows[0].Json, &fields) != nil || string(fields["value"]) != `"340282366920938463463374607431768211456"` {
		t.Fatal("lossless value changed")
	}
	if _, e = c.RecheckCohort(t.Context(), &pb.IndexedHandleRequest{Handle: opened.Handle}); e != nil {
		t.Fatal(e)
	}
	f.exec(t, `UPDATE transactions SET value=value+1 WHERE hash=$1`, hash("3"))
	if _, e = c.RecheckCohort(t.Context(), &pb.IndexedHandleRequest{Handle: opened.Handle}); status.Code(e) != codes.Aborted {
		t.Fatal("changed cohort accepted", e)
	}
	if _, e = c.RecheckCohort(t.Context(), &pb.IndexedHandleRequest{Handle: opened.Handle}); status.Code(e) != codes.Unavailable {
		t.Fatal("failed purpose rebound", e)
	}
	waitDiscarded(t, s)
}
func TestActualIndexedRPCProtectedCompoundLookaheadRecheck(t *testing.T) {
	f, _, c := sourceRPCFixture(t)
	a, contract := protectedTestAddress(1), protectedTestAddress(9)
	for n := 1; n <= 2; n++ {
		f.exec(t, `INSERT INTO blocks(number,hash,parent_hash,timestamp,gas_used,gas_limit,transaction_count)VALUES($1,$2,$2,$1,0,30000000,1)`, n, protectedHash(protectedTestHash(byte(n))))
		f.exec(t, `INSERT INTO transactions(hash,block_number,tx_index,from_address,to_address,value,gas_used,gas_price,status)VALUES($1,$2,0,$3,$4,0,21000,1,1)`, protectedHash(protectedTestHash(byte(n+10))), n, protectedAddress(a), protectedAddress(contract))
	}
	source, e := c.GetSource(t.Context(), &pb.Empty{})
	if e != nil {
		t.Fatal(e)
	}
	handle, e := c.OpenProtected(t.Context(), &pb.OpenProtectedRequest{Expected: source.Binding, Selection: &pb.IndexedProtectedSelection{Targets: [][]byte{a[:]}, FromNumber: 0, ThroughNumber: 2, FromTime: 0, ThroughTime: 10}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ReadDestinations(t.Context(), &pb.IndexedHandleRequest{Handle: handle}); e != nil {
		t.Fatal(e)
	}
	pred := &pb.IndexedPredicate{Intervals: []*pb.IndexedInterval{{Address: contract[:], FromNumber: 0, BeforeNumber: 3}}}
	if _, e = c.ReadWitnesses(t.Context(), &pb.IndexedWitnessRequest{Handle: handle, Predicate: pred}); e != nil {
		t.Fatal(e)
	}
	rows, e := c.ReadTransactions(t.Context(), &pb.IndexedTransactionsRequest{Handle: handle, Predicate: pred, Query: &pb.IndexedTransactionQuery{Limit: 1}})
	if e != nil || rows.Exhausted || len(rows.Rows) != 1 {
		t.Fatal("lookahead missing", e)
	}
	if _, e = c.RecheckProtected(t.Context(), &pb.IndexedHandleRequest{Handle: handle}); e != nil {
		t.Fatal(e)
	}
	f.exec(t, `UPDATE transactions SET tx_index=1 WHERE hash=$1`, protectedHash(protectedTestHash(12)))
	if _, e = c.RecheckProtected(t.Context(), &pb.IndexedHandleRequest{Handle: handle}); status.Code(e) != codes.Aborted {
		t.Fatal("hidden lookahead change accepted", e)
	}
}
func TestActualIndexedRPCOriginalDeadlineDoesNotRenew(t *testing.T) {
	_, _, c := sourceRPCFixture(t)
	source, e := c.GetSource(t.Context(), &pb.Empty{})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	handle, e := c.OpenProtected(ctx, &pb.OpenProtectedRequest{Expected: source.Binding, Selection: &pb.IndexedProtectedSelection{ThroughNumber: 1, ThroughTime: 1}})
	if e != nil {
		t.Fatal(e)
	}
	<-ctx.Done()
	if _, e = c.ReadDestinations(context.Background(), &pb.IndexedHandleRequest{Handle: handle}); status.Code(e) != codes.Unavailable {
		t.Fatal("original deadline renewed", e)
	}
}
