package grpcserver

import (
	"context"
	"log/slog"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
)

func (s *Server) GetSyncStatus(ctx context.Context, _ *indexerv1.Empty) (*indexerv1.SyncStatus, error) {
	st, err := s.db.GetSyncStatus(ctx)
	if err != nil {
		slog.Error("GetSyncStatus", "error", err)
		return nil, internalErr(err, "GetSyncStatus")
	}
	resp := mapSyncStatus(st)
	if resp == nil {
		return &indexerv1.SyncStatus{}, nil
	}
	return resp, nil
}
