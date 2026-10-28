package indexer

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
)

// missingRetryBlocks bounds the blocks one retry pass writes, so the head is never held up for long.
const missingRetryBlocks = 20

// retryMissing indexes, at most once per missingRetryInterval, the blocks recorded in missing_block_ranges below the
// cursor, in block order: one interval after the loop starts (records a start left, with catch-up off), and then
// while the loop has recorded blocks it has not seen filled. In standard mode nothing else reads that table while the process runs: the blocks the head
// loop skipped for their receipts would wait for the next start, and the ledger, and with it retention, would stop
// at the first of them. A block is written only when it extends the stored block below it and the stored block above
// extends it; a block the node still cannot serve, or that does not fit, stays recorded and ends the pass.
func (i *Indexer) retryMissing(ctx context.Context, lastIndexed uint64, receipts *receiptSkip) {
	if !receipts.recorded || time.Now().Before(i.missingRetryAt) {
		return
	}
	i.missingRetryAt = time.Now().Add(i.missingRetryInterval)
	ranges, err := i.db.GetMissingRangesBatch(ctx, missingRetryBlocks)
	if err != nil {
		log.Error("failed to read the blocks recorded as missing; retrying them later", "error", err)
		return
	}
	if len(ranges) == 0 {
		receipts.recorded = false
		return
	}
	sort.Slice(ranges, func(a, b int) bool { return ranges[a].FromNumber < ranges[b].FromNumber })
	budget := missingRetryBlocks
	for _, r := range ranges {
		if r.FromNumber > lastIndexed || budget == 0 {
			return
		}
		next := r.FromNumber
		for ; next <= r.ToNumber && next <= lastIndexed && budget > 0; next++ {
			budget--
			if !i.retryMissingBlock(ctx, next) {
				break
			}
		}
		if next == r.FromNumber {
			return
		}
		if err := i.db.DeleteMissingRangeByBlock(ctx, r.FromNumber); err != nil {
			log.Error("failed to remove indexed blocks from the missing ranges", "from", r.FromNumber, "to", next-1, "error", err)
			return
		}
		if next <= r.ToNumber {
			if err := i.db.SaveMissingRanges(ctx, []db.BlockRange{{FromNumber: next, ToNumber: r.ToNumber}}); err != nil {
				log.Error("failed to record the rest of a missing range", "from", next, "to", r.ToNumber, "error", err)
			}
			return
		}
		log.Info("indexed blocks recorded as missing", "from", r.FromNumber, "to", r.ToNumber)
	}
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
