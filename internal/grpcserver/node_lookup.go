package grpcserver

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/gateway-fm/chain-indexer/internal/nodelookup"
)

// SourceHeader is set to "node" on a response served from the node because its block is pruned.
const SourceHeader = "x-indexer-source"

func servedFromNode(ctx context.Context) {
	_ = grpc.SetHeader(ctx, metadata.Pairs(SourceHeader, "node"))
}

func nodeLookupErr(err error, op string) error {
	if errors.Is(err, nodelookup.ErrBusy) {
		return status.Error(codes.Unavailable, "pruned history: the node lookup is busy, retry later")
	}
	slog.Error("node lookup", "op", op, "error", err)
	return status.Errorf(codes.Unavailable, "pruned history: the node did not answer (%s)", op)
}

// prunedThroughForNode is the watermark when pruned history is served from the node; ok is false with the lookup
// off or nothing pruned.
func (s *Server) prunedThroughForNode(ctx context.Context) (through uint64, ok bool, err error) {
	if s.cfg.NodeLookup == nil {
		return 0, false, nil
	}
	through, _, err = s.prunedThrough(ctx)
	return through, err == nil && through > 0, err
}

// prunedBlockByNumber is the node's block for a number at or below the watermark; nil (and no error) when the
// number is retained or the lookup is off, so the caller reads the database.
func (s *Server) prunedBlockByNumber(ctx context.Context, number uint64) (*nodelookup.Block, error) {
	through, ok, err := s.prunedThroughForNode(ctx)
	if err != nil || !ok || number > through {
		return nil, err
	}
	b, err := s.cfg.NodeLookup.BlockByNumber(ctx, number)
	if err != nil {
		return nil, nodeLookupErr(err, "block by number")
	}
	if b == nil {
		return nil, notFound("block", "")
	}
	servedFromNode(ctx)
	return b, nil
}

// prunedBlockByHash is the node's block for a hash the database does not have, if that block is pruned.
func (s *Server) prunedBlockByHash(ctx context.Context, hash string) (*nodelookup.Block, error) {
	through, ok, err := s.prunedThroughForNode(ctx)
	if err != nil || !ok {
		return nil, err
	}
	b, err := s.cfg.NodeLookup.BlockByHash(ctx, hash, through)
	if err != nil {
		return nil, nodeLookupErr(err, "block by hash")
	}
	if b != nil {
		servedFromNode(ctx)
	}
	return b, nil
}
