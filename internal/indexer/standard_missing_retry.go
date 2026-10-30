package indexer

import (
	"context"
	"strings"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
)

// missingRetryBlocks bounds the blocks one retry pass writes, so the head is never held up for long.
const missingRetryBlocks = 20

// retryMissing indexes, at most once per missingRetryInterval, the blocks recorded in missing_block_ranges below the
// cursor: one interval after the loop starts (records a start left, with catch-up off), and then while the loop has
// recorded blocks it has not seen filled. In standard mode nothing else reads that table while the process runs: the
// blocks the head loop skipped for their receipts would wait for the next start, and the ledger, and with it
// retention, would stop at the first of them.
//
// A pass reads the lowest recorded ranges that are not paused (GetLowestMissingRanges), lowest first, since the
// lowest is what holds the ledger, and works through them in block order, writing at most missingRetryBlocks blocks.
// A block is written only when it extends the stored block below it and the stored block above extends it. A block
// the node still cannot serve, or that does not fit, ends its range for this pass and puts the range on a doubling
// pause (missingBackoff). A paused range is left out of the read, so ranges that keep failing never fill a pass: the
// next pass reaches the ranges above them, up to the newest.
func (i *Indexer) retryMissing(ctx context.Context, lastIndexed uint64, receipts *receiptSkip) {
	if !receipts.recorded || time.Now().Before(i.missingRetryAt) {
		return
	}
	i.missingRetryAt = time.Now().Add(i.missingRetryInterval)
	paused := i.pausedMissing()
	ranges, err := i.db.GetLowestMissingRanges(ctx, missingRetryBlocks, paused)
	if err != nil {
		log.Error("failed to read the blocks recorded as missing; retrying them later", "error", err)
		return
	}
	if len(ranges) == 0 && len(paused) == 0 {
		receipts.recorded = false
		clear(i.missingBackoff)
		return
	}
	budget := missingRetryBlocks
	for _, r := range ranges {
		if r.FromNumber > lastIndexed || budget == 0 {
			return
		}
		next, failed := r.FromNumber, false
		for ; next <= r.ToNumber && next <= lastIndexed && budget > 0; next++ {
			budget--
			if !i.retryMissingBlock(ctx, next) {
				failed = true
				break
			}
		}
		if failed {
			i.backOffMissing(r.FromNumber, next)
		} else {
			delete(i.missingBackoff, r.FromNumber)
		}
		if next == r.FromNumber {
			continue
		}
		if err := i.db.AdvanceMissingRange(ctx, r, next); err != nil {
			log.Error("failed to record the indexed start of a missing range; it stays recorded whole",
				"from", r.FromNumber, "indexed_to", next-1, "to", r.ToNumber, "error", err)
			continue
		}
		if next > r.ToNumber {
			log.Info("indexed blocks recorded as missing", "from", r.FromNumber, "to", r.ToNumber)
		}
	}
}

// missingAttempt is a recorded range's pause after a failed retry, kept in memory by its first unfilled block.
type missingAttempt struct {
	failures int
	nextTry  time.Time
}

// pausedMissing lists the first blocks of the ranges whose pause has not run out.
func (i *Indexer) pausedMissing() []uint64 {
	now := time.Now()
	var paused []uint64
	for from, a := range i.missingBackoff {
		if now.Before(a.nextTry) {
			paused = append(paused, from)
		}
	}
	return paused
}

// backOffMissing pauses the range that now starts at block after a failure there: missingRetryInterval, doubling
// with each consecutive failure, up to 64 intervals.
func (i *Indexer) backOffMissing(from, block uint64) {
	failures := i.missingBackoff[from].failures + 1
	delete(i.missingBackoff, from)
	if i.missingBackoff == nil {
		i.missingBackoff = map[uint64]missingAttempt{}
	}
	i.missingBackoff[block] = missingAttempt{failures: failures,
		nextTry: time.Now().Add(i.missingRetryInterval << min(failures-1, 6))}
}

// retryMissingBlock writes one recorded block and reports whether the pass goes on to the next.
func (i *Indexer) retryMissingBlock(ctx context.Context, number uint64) bool {
	stored, err := i.db.HasBlock(ctx, number)
	if err != nil {
		log.Error("failed to check a block recorded as missing", "block", number, "error", err)
		return false
	}
	if stored {
		return true
	}
	f := i.fetchBlockAndReceipts(ctx, number)
	if f.err != nil {
		log.Warn("a block recorded as missing is still unavailable; retrying it later", "block", number, "error", f.err)
		return false
	}
	if number > 0 {
		below, err := i.db.GetBlock(ctx, number-1)
		if err != nil {
			log.Error("failed to read the block below one recorded as missing", "block", number, "error", err)
			return false
		}
		if below != nil && !strings.EqualFold(below.Hash, f.raw.ParentHash.Hex()) {
			log.Warn("a block recorded as missing does not extend the stored block below it; retrying it later",
				"block", number, "parent", f.raw.ParentHash.Hex(), "stored", below.Hash)
			return false
		}
	}
	above, err := i.db.GetBlock(ctx, number+1)
	if err != nil {
		log.Error("failed to read the block above one recorded as missing", "block", number, "error", err)
		return false
	}
	if above != nil && !strings.EqualFold(above.ParentHash, f.raw.Hash.Hex()) {
		log.Warn("the stored block above one recorded as missing does not extend it; retrying it later",
			"block", number, "hash", f.raw.Hash.Hex(), "stored_parent", above.ParentHash)
		return false
	}
	if len(f.raw.Transactions) == 0 {
		err = i.insertEmptyBlock(ctx, f.raw)
	} else {
		err = i.processBlockWithReceipts(ctx, f.raw, f.receipts)
	}
	if err != nil {
		log.Error("failed to index a block recorded as missing; retrying it later", "block", number, "error", err)
		return false
	}
	return true
}
