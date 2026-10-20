package indexer

import (
	"context"
	"errors"

	"github.com/gateway-fm/chain-indexer/internal/log"
)

// errReceiptsUnavailable marks a block whose header and transactions were fetched but whose
// receipts were not, so a loop can tell it from a node or database outage.
var errReceiptsUnavailable = errors.New("receipts unavailable")

// maxReceiptAttempts is how many consecutive times a head loop retries a block whose receipts
// cannot be fetched before recording it in missing_block_ranges and moving past it.
const maxReceiptAttempts = 10

// receiptSkip lets a head loop move past a block whose receipts the node never returns, instead
// of stopping ingest at that block. The block is skipped only once it is recorded in
// missing_block_ranges, where catch-up keeps retrying it; any other error still stops the loop.
type receiptSkip struct {
	block    uint64
	attempts int
}

func (s *receiptSkip) skip(ctx context.Context, d Database, blockNum uint64, err error) bool {
	if !errors.Is(err, errReceiptsUnavailable) {
		return false
	}
	if s.block != blockNum {
		s.block, s.attempts = blockNum, 0
	}
	s.attempts++
	if s.attempts < maxReceiptAttempts {
		return false
	}
	if requeueErr := d.RequeueMissingBlock(ctx, blockNum); requeueErr != nil {
		log.Error("failed to record block with unavailable receipts as missing; retrying it", "block", blockNum, "error", requeueErr)
		return false
	}
	log.Error("receipts still unavailable; block recorded as missing and skipped, catch-up retries it",
		"block", blockNum, "attempts", s.attempts, "error", err)
	s.attempts = 0
	return true
}
