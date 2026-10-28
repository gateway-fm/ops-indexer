package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const watermarkTTL = 5 * time.Second

// watermark caches the prune watermark; handlers refresh it once when an explicit target comes back missing. It
// follows the stored value both ways, since FORCE_REINDEX's wipe sets it back to 0. Of concurrent refreshes the one
// that started last wins: it read the watermark at least as late as any other, while a read that started earlier
// and finished later could be from before a prune.
type watermark struct {
	mu        sync.Mutex
	through   uint64
	timestamp int64
	readAt    time.Time
	started   uint64
	applied   uint64
}

func (s *Server) prunedThrough(ctx context.Context) (uint64, int64, error) {
	s.watermark.mu.Lock()
	through, ts, fresh := s.watermark.through, s.watermark.timestamp, time.Since(s.watermark.readAt) < watermarkTTL
	s.watermark.mu.Unlock()
	if fresh {
		return through, ts, nil
	}
	return s.refreshPrunedThrough(ctx)
}

func (s *Server) refreshPrunedThrough(ctx context.Context) (uint64, int64, error) {
	s.watermark.mu.Lock()
	s.watermark.started++
	read := s.watermark.started
	s.watermark.mu.Unlock()
	through, ts, err := s.db.PrunedThrough(ctx)
	if err != nil {
		slog.Error("read prune watermark", "error", err)
		return 0, 0, internalErr(err, "PrunedThrough")
	}
	s.watermark.mu.Lock()
	defer s.watermark.mu.Unlock()
	if read > s.watermark.applied {
		s.watermark.through, s.watermark.timestamp, s.watermark.applied = through, ts, read
		s.watermark.readAt = time.Now()
	}
	return s.watermark.through, s.watermark.timestamp, nil
}

// checkRetained answers OUT_OF_RANGE for an explicit block target at or below the (cached) watermark.
func (s *Server) checkRetained(ctx context.Context, block uint64) error {
	through, _, err := s.prunedThrough(ctx)
	if err != nil {
		return err
	}
	return outOfRetention(block, through)
}

// recheckRetained is checkRetained against a fresh watermark, for a target that came back missing: the block
// may have been pruned since the cached read.
func (s *Server) recheckRetained(ctx context.Context, block uint64) error {
	through, _, err := s.refreshPrunedThrough(ctx)
	if err != nil {
		return err
	}
	return outOfRetention(block, through)
}

// lastBlockBelow is the newest block an exclusive upper bound admits, for checking a range against the watermark.
func lastBlockBelow(exclusiveUpper uint64) uint64 {
	if exclusiveUpper == 0 {
		return 0
	}
	return exclusiveUpper - 1
}

func outOfRetention(block, through uint64) error {
	if through > 0 && block <= through {
		return pruned(block, through)
	}
	return nil
}

// notFoundByHash is notFound for lookups that cannot tell a pruned entity from one that never existed.
func (s *Server) notFoundByHash(ctx context.Context, kind, hash string) error {
	through, _, err := s.prunedThrough(ctx)
	if err != nil || through == 0 {
		return notFound(kind, hash)
	}
	return notFound(kind, fmt.Sprintf("%s (history at or below block %d is pruned)", hash, through))
}
