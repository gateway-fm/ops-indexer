// Package grpcserver implements the chain-indexer gRPC read API.
//
// The server wraps the indexer's postgres store (internal/db) and exposes it
// via IndexerService defined in proto/chain_indexer/v1/.
//
// Trust model: the server does not authenticate callers, does not filter by
// viewer identity, and does not redact. Consumers (privacy-proxy for privacy
// mode, block-explorer api in standalone mode) handle access control at their
// own layer. See docs/API.md and RD-855 for the rationale.
package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/gateway-fm/chain-indexer/internal/db"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

// Config controls runtime behavior of the gRPC server.
type Config struct {
	// ListenAddr is the address the gRPC server binds to, e.g. ":50051".
	// Required.
	ListenAddr string

	// MaxRecvMsgBytes caps inbound message size. Default 4 MiB if zero.
	MaxRecvMsgBytes int

	// MaxSendMsgBytes caps outbound message size. Default 16 MiB if zero.
	MaxSendMsgBytes int

	// OPStackEnabled gates the OP-Stack RPCs. When false, GetOPDeposit and
	// ListOPDeposits return Unavailable.
	OPStackEnabled bool

	// DefaultPageSize is the page_size used when a request sends 0. 25.
	DefaultPageSize int32

	// MaxPageSize caps the server-side page size. Requests above this are
	// clamped, not rejected. Default 100.
	MaxPageSize int32
}

func (c Config) withDefaults() Config {
	if c.MaxRecvMsgBytes == 0 {
		c.MaxRecvMsgBytes = 4 * 1024 * 1024
	}
	if c.MaxSendMsgBytes == 0 {
		c.MaxSendMsgBytes = 16 * 1024 * 1024
	}
	if c.DefaultPageSize == 0 {
		c.DefaultPageSize = 25
	}
	if c.MaxPageSize == 0 {
		c.MaxPageSize = 100
	}
	return c
}

// Server implements IndexerServiceServer. It holds the database and
// configuration; each handler translates typed requests into db-package calls
// and maps results into proto messages.
type Server struct {
	indexerv1.UnimplementedIndexerServiceServer

	cfg Config
	db  *db.DB
}

// New constructs a Server. The db must already be connected and migrated.
func New(cfg Config, database *db.DB) (*Server, error) {
	if cfg.ListenAddr == "" {
		return nil, errors.New("grpcserver: ListenAddr is required")
	}
	if database == nil {
		return nil, errors.New("grpcserver: database is required")
	}
	return &Server{
		cfg: cfg.withDefaults(),
		db:  database,
	}, nil
}

// Serve starts the gRPC server and blocks until the context is cancelled or an
// error occurs. Callers typically run it in a goroutine.
func (s *Server) Serve(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("grpcserver: listen on %s: %w", s.cfg.ListenAddr, err)
	}

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(s.cfg.MaxRecvMsgBytes),
		grpc.MaxSendMsgSize(s.cfg.MaxSendMsgBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	indexerv1.RegisterIndexerServiceServer(grpcSrv, s)

	slog.Info("gRPC server listening", "addr", s.cfg.ListenAddr)

	errCh := make(chan error, 1)
	go func() { errCh <- grpcSrv.Serve(listener) }()

	select {
	case <-ctx.Done():
		slog.Info("gRPC server shutting down gracefully")
		grpcSrv.GracefulStop()
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("grpcserver: serve: %w", err)
		}
		return nil
	}
}

// clampPageSize applies server-side caps and defaults.
func (s *Server) clampPageSize(requested int32) int32 {
	if requested <= 0 {
		return s.cfg.DefaultPageSize
	}
	if requested > s.cfg.MaxPageSize {
		return s.cfg.MaxPageSize
	}
	return requested
}
