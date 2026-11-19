// This fixture runs the real bounded gRPC source without ingestion workers.
package main

import (
	"context"
	"fmt"
	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/indexedread"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	host, _, e := net.SplitHostPort(os.Getenv("INDEXED_READ_LISTEN_ADDR"))
	if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		fmt.Fprintln(os.Stderr, "invalid test source listen address")
		os.Exit(1)
	}
	source, e := indexedread.NewService(ctx, indexedread.ServiceConfig{DSN: os.Getenv("INDEXED_READ_DSN"), SourceReference: os.Getenv("INDEXED_READ_SOURCE_REFERENCE"), Epoch: os.Getenv("INDEXED_READ_EPOCH"), ChainID: os.Getenv("INDEXED_READ_CHAIN_ID"), GenesisHash: os.Getenv("INDEXED_READ_GENESIS_HASH")})
	if e != nil {
		fmt.Fprintln(os.Stderr, "indexed test source unavailable")
		os.Exit(1)
	}
	defer source.CloseService()
	listener, e := net.Listen("tcp", os.Getenv("INDEXED_READ_LISTEN_ADDR"))
	if e != nil {
		fmt.Fprintln(os.Stderr, "indexed test source listen failed")
		os.Exit(1)
	}
	options := append(source.ServerOptions(), grpc.MaxRecvMsgSize(indexedread.MaxRecvBytes), grpc.MaxSendMsgSize(indexedread.MaxSendBytes))
	server := grpc.NewServer(options...)
	pb.RegisterIndexedReadServiceServer(server, source)
	go func() { <-ctx.Done(); server.Stop() }()
	fmt.Println("ready " + listener.Addr().String())
	if e = server.Serve(listener); e != nil && ctx.Err() == nil {
		os.Exit(1)
	}
}
