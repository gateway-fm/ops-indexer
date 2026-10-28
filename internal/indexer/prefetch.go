package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// blockPrefetchDepth is how many blocks ahead of the committer the standard loop fetches.
//
// Each block used to be fetched, then its receipts, then committed, strictly one after another:
// on a 6.8M-transaction benchmark database that was ~70 ms of block fetch + receipts + ~94 ms of
// commit per block, with Postgres and the indexer each ~0.7 cores busy. The two chain reads do not
// depend on the database, so they run ahead while the previous block commits. Commits stay
// sequential and in block order; only the reads overlap.
const blockPrefetchDepth = 8

type fetchedBlock struct {
	number   uint64
	raw      *rpc.RawBlock
	receipts map[common.Hash]*rpclient.Receipt
	err      error
}

func (i *Indexer) fetchBlockAndReceipts(ctx context.Context, number uint64) fetchedBlock {
	raw, err := i.rpc.RawBlockByNumber(ctx, number)
	if err != nil {
		return fetchedBlock{number: number, err: err}
	}
	if len(raw.Transactions) == 0 {
		return fetchedBlock{number: number, raw: raw}
	}
	receipts, err := i.rpc.FetchReceiptsBatch(ctx, i.receiptTxHashes(raw), i.config.RPCWorkers, i.config.RPCRateLimit, number)
	if err != nil {
		err = fmt.Errorf("%w: %w", errReceiptsUnavailable, err)
	}
	return fetchedBlock{number: number, raw: raw, receipts: receipts, err: err}
}

// prefetchBlocks fetches blocks from..to with at most depth reads in flight, and hands them back
// in block order: one slot per block, sent in order, each filled when its read finishes.
func (i *Indexer) prefetchBlocks(ctx context.Context, from, to uint64, depth int) <-chan chan fetchedBlock {
	slots := make(chan chan fetchedBlock, depth)
	go func() {
		defer close(slots)
		for n := from; n <= to; n++ {
			slot := make(chan fetchedBlock, 1)
			select {
			case slots <- slot:
			case <-ctx.Done():
				return
			}
			go func(n uint64) { slot <- i.fetchBlockAndReceipts(ctx, n) }(n)
		}
	}()
	return slots
}

// anchor is the block a tick starts after, and the hash its first block must extend. That is lastIndexed when it is
// stored, or a block receipts skipped (not stored) at lastIndexed. When lastIndexed is neither, as after a revert whose
// delete committed but reported an error, the tick starts after the highest stored block below it, so the deleted
// blocks are fetched again and the check is never skipped. Block 0, or nothing stored at or below lastIndexed, has
// no hash to check against. A remembered hash the cursor has moved below is dropped. remembered reports that hash
// is the remembered one (see recheckSkipped). ok is false when the store cannot be read; the tick then ends.
func (i *Indexer) anchor(ctx context.Context, lastIndexed uint64, receipts *receiptSkip) (from uint64, hash common.Hash, remembered, ok bool) {
	if receipts.skipped > lastIndexed {
		receipts.skipped, receipts.skippedHash = 0, common.Hash{}
	}
	if lastIndexed == 0 {
		return 0, common.Hash{}, false, true
	}
	b, err := i.db.GetBlock(ctx, lastIndexed)
	if err != nil {
		log.Error("failed to read the block before this tick, retrying on the next tick", "block", lastIndexed, "error", err)
		return lastIndexed, common.Hash{}, false, false
	}
	if b != nil {
		return lastIndexed, common.HexToHash(b.Hash), false, true
	}
	if receipts.skipped == lastIndexed && receipts.skippedHash != (common.Hash{}) {
		return lastIndexed, receipts.skippedHash, true, true
	}
	below, err := i.db.HighestBlockAtOrBelow(ctx, lastIndexed)
	if err != nil {
		log.Error("failed to read the highest stored block, retrying on the next tick", "below", lastIndexed, "error", err)
		return lastIndexed, common.Hash{}, false, false
	}
	if below == nil {
		return lastIndexed, common.Hash{}, false, true
	}
	log.Warn("the block before this tick is not stored; indexing again from the highest stored block below it",
		"block", lastIndexed, "from", below.Number)
	return below.Number, common.HexToHash(below.Hash), false, true
}

// recheckSkipped is called when the tick's first block does not extend the remembered hash of the block skipped for
// its receipts. That hash is what the chain had when the block was skipped; the block is not stored, so the reorg
// check never compares it. When the node now has a different block there, or none, the hash is dropped and the next
// tick starts again from the highest stored block below it. When the node still has it, the mismatch is real.
func (i *Indexer) recheckSkipped(ctx context.Context, receipts *receiptSkip) {
	hash, err := i.rpc.RawBlockHash(ctx, receipts.skipped)
	if err == nil && common.HexToHash(hash) == receipts.skippedHash {
		return
	}
	log.Warn("the block skipped for its receipts is no longer on the chain; checking again from the stored blocks",
		"block", receipts.skipped, "error", err)
	receipts.skipped, receipts.skippedHash = 0, common.Hash{}
}

// indexPrefetched indexes lastIndexed+1..latest in order and returns the new last indexed block.
// It stops at the first failure, as the sequential loop did, and leaves the rest to the next tick.
// A prefetched block whose parent is not the block just committed was read before a reorg; it is
// discarded unwritten, and the next tick's detectReorg sees the chain as it is then.
// A block whose receipts never come is moved past once receipts records it as missing (receipt_skip.go), and only
// if it extends the block before it.
func (i *Indexer) indexPrefetched(ctx context.Context, lastIndexed, latest uint64, receipts *receiptSkip) uint64 {
	lastIndexed, prevHash, remembered, ok := i.anchor(ctx, lastIndexed, receipts)
	if !ok || latest <= lastIndexed {
		return lastIndexed
	}
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for slot := range i.prefetchBlocks(fetchCtx, lastIndexed+1, latest, blockPrefetchDepth) {
		select {
		case req := <-i.indexRequests:
			req.done <- i.processBlock(ctx, req.blockNumber)
		default:
		}

		b := <-slot
		if b.raw != nil && prevHash != (common.Hash{}) && b.raw.ParentHash != prevHash {
			log.Warn("prefetched block does not extend the block just indexed, refetching on the next tick",
				"block", b.number, "parent", b.raw.ParentHash.Hex(), "previous", prevHash.Hex())
			if remembered {
				i.recheckSkipped(ctx, receipts)
			}
			return lastIndexed
		}
		remembered = false
		if b.err != nil {
			log.Error("failed to process block", "block", b.number, "error", b.err)
			if b.raw == nil || !receipts.skip(ctx, i.db, b.number, b.err) {
				return lastIndexed
			}
			lastIndexed, prevHash = b.number, b.raw.Hash
			receipts.skipped, receipts.skippedHash = b.number, b.raw.Hash
			continue
		}

		var err error
		if len(b.raw.Transactions) == 0 {
			err = i.insertEmptyBlock(ctx, b.raw)
		} else {
			err = i.processBlockWithReceipts(ctx, b.raw, b.receipts)
		}
		if err != nil {
			log.Error("failed to process block", "block", b.number, "error", err)
			return lastIndexed
		}
		lastIndexed = b.number
		prevHash = b.raw.Hash

		if lastIndexed%10 == 0 {
			i.db.UpdateSyncStatus(ctx, lastIndexed, true)
		}
	}
	return lastIndexed
}

// inFlight is a block whose own rows are being written in its own transaction while earlier
// blocks finish.
type inFlight struct {
	built *builtBlock
	begun chan beginResult
}

type beginResult struct {
	w   *db.BlockWrite
	err error
}

// indexPipelined is indexPrefetched with up to depth block transactions open at once (the
// pipelined commit). Each block's own rows (transactions, logs, token_transfers, ...) are written
// concurrently; the shared rows and the COMMIT run in block order, one block after the other, so
// the committed blocks stay a prefix of the chain. On the first failure every later open
// transaction is rolled back and the loop returns the last block it committed.
func (i *Indexer) indexPipelined(ctx context.Context, lastIndexed, latest uint64, depth int, receipts *receiptSkip) uint64 {
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var queue []*inFlight
	abort := func() {
		for _, f := range queue {
			if r := <-f.begun; r.err == nil {
				r.w.Rollback(ctx)
			}
		}
		queue = nil
	}
	finishOldest := func() bool {
		f := queue[0]
		queue = queue[1:]
		r := <-f.begun
		if r.err != nil {
			log.Error("failed to process block", "block", f.built.blockNumber, "error", r.err)
			return false
		}
		finishStart := time.Now()
		if err := r.w.Finish(ctx); err != nil {
			log.Error("failed to process block", "block", f.built.blockNumber, "error", err)
			return false
		}
		metrics.ObserveStage(metrics.StageDBFinish, time.Since(finishStart))
		metrics.ObserveStage(metrics.StageBlockTotal, time.Since(f.built.start))
		i.afterCommit(ctx, f.built)
		lastIndexed = f.built.blockNumber
		if lastIndexed%10 == 0 {
			i.db.UpdateSyncStatus(ctx, lastIndexed, true)
		}
		return true
	}
	drain := func() bool {
		for len(queue) > 0 {
			if !finishOldest() {
				abort()
				return false
			}
		}
		return true
	}

	from, prevHash, remembered, ok := i.anchor(ctx, lastIndexed, receipts)
	if !ok || latest <= from {
		return from
	}
	lastIndexed = from
	for slot := range i.prefetchBlocks(fetchCtx, lastIndexed+1, latest, blockPrefetchDepth+depth) {
		// Serve a block request (as indexPrefetched does) between blocks. Finish what is in flight first, so the
		// requested block never runs beside a pipelined one and committed blocks stay a prefix.
		select {
		case req := <-i.indexRequests:
			if !drain() {
				req.done <- fmt.Errorf("block %d not processed: the commit pipeline aborted", req.blockNumber)
				return lastIndexed
			}
			req.done <- i.processBlock(ctx, req.blockNumber)
		default:
		}

		b := <-slot
		if b.raw != nil && prevHash != (common.Hash{}) && b.raw.ParentHash != prevHash {
			log.Warn("prefetched block does not extend the block before it, refetching on the next tick",
				"block", b.number, "parent", b.raw.ParentHash.Hex(), "previous", prevHash.Hex())
			if remembered {
				i.recheckSkipped(ctx, receipts)
			}
			break
		}
		remembered = false
		if b.err != nil {
			log.Error("failed to process block", "block", b.number, "error", b.err)
			// A block whose receipts never come is moved past once recorded as missing (receipt_skip.go), after
			// the blocks before it are committed.
			if b.raw == nil || !drain() || !receipts.skip(ctx, i.db, b.number, b.err) {
				break
			}
			lastIndexed, prevHash = b.number, b.raw.Hash
			receipts.skipped, receipts.skippedHash = b.number, b.raw.Hash
			continue
		}
		prevHash = b.raw.Hash

		if len(b.raw.Transactions) == 0 {
			if !drain() {
				return lastIndexed
			}
			if err := i.insertEmptyBlock(ctx, b.raw); err != nil {
				log.Error("failed to process block", "block", b.number, "error", err)
				return lastIndexed
			}
			lastIndexed = b.number
			continue
		}

		built := i.buildBlock(ctx, b.raw, b.receipts)
		f := &inFlight{built: built, begun: make(chan beginResult, 1)}
		go func() {
			beginStart := time.Now()
			w, err := i.db.BeginBlockData(ctx, built.data)
			metrics.ObserveStage(metrics.StageDBBegin, time.Since(beginStart))
			f.begun <- beginResult{w: w, err: err}
		}()
		queue = append(queue, f)
		if len(queue) >= depth && !finishOldest() {
			abort()
			return lastIndexed
		}
	}
	drain()
	return lastIndexed
}
