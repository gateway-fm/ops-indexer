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

// indexPrefetched indexes lastIndexed+1..latest in order and returns the new last indexed block.
// It stops at the first failure, as the sequential loop did, and leaves the rest to the next tick.
// A prefetched block whose parent is not the block just committed was read before a reorg; it is
// discarded unwritten, and the next tick's detectReorg sees the chain as it is then.
// A block whose receipts never come is moved past once receipts records it as missing (receipt_skip.go).
func (i *Indexer) indexPrefetched(ctx context.Context, lastIndexed, latest uint64, receipts *receiptSkip) uint64 {
	if latest <= lastIndexed {
		return lastIndexed
	}
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var prevHash common.Hash
	for slot := range i.prefetchBlocks(fetchCtx, lastIndexed+1, latest, blockPrefetchDepth) {
		select {
		case req := <-i.indexRequests:
			req.done <- i.processBlock(ctx, req.blockNumber)
		default:
		}

		b := <-slot
		if b.err != nil {
			log.Error("failed to process block", "block", b.number, "error", b.err)
			if b.raw == nil || !receipts.skip(ctx, i.db, b.number, b.err) {
				return lastIndexed
			}
			lastIndexed, prevHash = b.number, b.raw.Hash
			continue
		}
		if prevHash != (common.Hash{}) && b.raw.ParentHash != prevHash {
			log.Warn("prefetched block does not extend the block just indexed, refetching on the next tick",
				"block", b.number, "parent", b.raw.ParentHash.Hex(), "previous", prevHash.Hex())
			return lastIndexed
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
	if latest <= lastIndexed {
		return lastIndexed
	}
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

	var prevHash common.Hash
	for slot := range i.prefetchBlocks(fetchCtx, lastIndexed+1, latest, blockPrefetchDepth+depth) {
		b := <-slot
		if b.err != nil {
			log.Error("failed to process block", "block", b.number, "error", b.err)
			// A block whose receipts never come is moved past once recorded as missing (receipt_skip.go), after
			// the blocks before it are committed.
			if b.raw == nil || !drain() || !receipts.skip(ctx, i.db, b.number, b.err) {
				break
			}
			lastIndexed, prevHash = b.number, b.raw.Hash
			continue
		}
		if prevHash != (common.Hash{}) && b.raw.ParentHash != prevHash {
			log.Warn("prefetched block does not extend the block before it, refetching on the next tick",
				"block", b.number, "parent", b.raw.ParentHash.Hex(), "previous", prevHash.Hex())
			break
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
