// Command contractfixture runs the real migrated Indexer API for consumer
// integration tests. It is not part of the production image.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/grpcserver"
	"github.com/gateway-fm/chain-indexer/internal/nodelookup"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

func main() {
	migrate := flag.Bool("migrate", false, "apply real Indexer migrations and exit")
	flag.Parse()
	if flag.NArg() != 0 {
		os.Exit(2)
	}
	dsn := os.Getenv("INDEXER_FIXTURE_DSN")
	if dsn == "" {
		os.Exit(2)
	}
	database, err := db.New(dsn)
	if err != nil {
		os.Exit(1)
	}
	defer database.Close()
	if *migrate {
		if err := database.Migrate(); err != nil {
			os.Exit(1)
		}
		return
	}
	listen := os.Getenv("INDEXER_FIXTURE_LISTEN_ADDR")
	host, _, err := net.SplitHostPort(listen)
	if err != nil || host != "127.0.0.1" {
		os.Exit(2)
	}
	var lookup *nodelookup.Lookup
	if endpoint := os.Getenv("INDEXER_FIXTURE_NODE_RPC_URL"); endpoint != "" {
		// Explicit trust is portable across Darwin/Linux. SSL_CERT_FILE alone is
		// ignored by Darwin's system-root verifier; verification is never disabled.
		caFile := os.Getenv("INDEXER_FIXTURE_NODE_CA_FILE")
		if caFile == "" {
			os.Exit(2)
		}
		file, err := os.Open(caFile)
		if err != nil {
			os.Exit(1)
		}
		cert, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		_ = file.Close()
		if err != nil || len(cert) == 0 || len(cert) > 1<<20 {
			os.Exit(1)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(cert) {
			os.Exit(1)
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		http.DefaultTransport = transport
		client, err := rpc.NewWithTimeout(endpoint, 2*time.Second)
		if err != nil {
			os.Exit(1)
		}
		lookup = nodelookup.New(nodelookup.NewNodeSource(client), nodelookup.Config{MaxBlocks: 4, MaxBytes: 8 << 20, Timeout: 2 * time.Second, Concurrency: 2})
	}
	server, err := grpcserver.New(grpcserver.Config{ListenAddr: listen, NodeLookup: lookup}, database)
	if err != nil {
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Serve(ctx); err != nil {
		os.Exit(1)
	}
}
