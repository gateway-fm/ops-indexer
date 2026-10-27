package indexer

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

type BalanceWork struct {
	Address      common.Address
	TokenAddress common.Address
	BlockNumber  uint64
}

type BalanceWorkerPool struct {
	db         Database
	rpc        RPCClient
	workChan   chan BalanceWork
	numWorkers int
	rateLimit  int
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc

	// sendMu guards workChan's close against concurrent sends; closed is set under its write lock by Stop.
	sendMu   sync.RWMutex
	closed   bool
	stopOnce sync.Once

	// dedupWindow > 0 holds each (address, token) key for that long before one fetch at latest.
	// Items for a held key fold into it; fetchChan feeds the workers, held counts keys not yet forwarded.
	dedupWindow time.Duration
	fetchChan   chan BalanceWork
	held        atomic.Int64

	// readerBatch > 1 makes a worker drain up to that many items and read them with one BalanceReader eth_call
	// per token, when the RPC client supports it.
	readerBatch    int
	readerSkipL1   bool
	balancesReader balancesReader
}

func NewBalanceWorkerPool(database Database, rpcClient RPCClient, numWorkers, rateLimit int) *BalanceWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &BalanceWorkerPool{
		db:         database,
		rpc:        rpcClient,
		workChan:   make(chan BalanceWork, 10000), // Buffer for 10k balance requests
		numWorkers: numWorkers,
		rateLimit:  rateLimit,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// balancesReader is implemented by the RPC client that can read many balances of one token in one eth_call.
type balancesReader interface {
	BalancesOf(ctx context.Context, token common.Address, holders []common.Address, skipL1Charging bool) ([]*big.Int, []bool, error)
}

// SetReaderBatch enables batched balance reads. Must be called before Start.
func (p *BalanceWorkerPool) SetReaderBatch(n int, skipL1Charging bool) {
	r, ok := p.rpc.(balancesReader)
	if n <= 1 || !ok {
		return
	}
	p.readerBatch, p.readerSkipL1, p.balancesReader = n, skipL1Charging, r
}

// SetDedupWindow enables the dedup window. Must be called before Start.
func (p *BalanceWorkerPool) SetDedupWindow(d time.Duration) {
	p.dedupWindow = d
}

func (p *BalanceWorkerPool) Start() {
	log.Info("starting balance workers", "count", p.numWorkers, "dedup_window", p.dedupWindow, "reader_batch", p.readerBatch, "skip_l1", p.readerSkipL1)
	if p.dedupWindow > 0 {
		p.fetchChan = make(chan BalanceWork, cap(p.workChan))
		p.wg.Add(1)
		go p.coalesce()
	}
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// balanceDrainTimeout bounds how long Stop lets the workers finish queued work, and balanceEnqueueGrace how long
// a committed block's producer keeps waiting for queue room once shutdown has begun. Together they stay below
// Kubernetes' default 30 s termination grace period.
var (
	balanceDrainTimeout = 20 * time.Second
	balanceEnqueueGrace = 5 * time.Second
)

// Stop drains instead of discarding: it closes the queue, so the coalescer forwards every key held by the dedup
// window, and lets the workers fetch and flush everything queued and held, under a live context. Only after balanceDrainTimeout does it cancel and drop what is left (logged with the
// count). A Queue call after Stop is refused (counted as dropped) instead of panicking on the closed channel.
func (p *BalanceWorkerPool) Stop() {
	p.stopOnce.Do(func() {
		left := p.QueueSize()
		log.Info("stopping balance workers: draining", "items", left)
		p.sendMu.Lock()
		p.closed = true
		close(p.workChan)
		p.sendMu.Unlock()

		done := make(chan struct{})
		go func() { p.wg.Wait(); close(done) }()
		select {
		case <-done:
			log.Info("balance workers stopped: drained", "items", left)
		case <-time.After(balanceDrainTimeout):
			log.Warn("balance drain timed out; remaining balance work dropped", "left", p.QueueSize())
			p.cancel()
			<-done
		}
		p.cancel()
	})
}

// QueueWork returns false if the queue is full (non-blocking).
func (p *BalanceWorkerPool) QueueWork(work BalanceWork) bool {
	p.sendMu.RLock()
	defer p.sendMu.RUnlock()
	if p.closed {
		return false
	}
	select {
	case p.workChan <- work:
		return true
	default:
		return false
	}
}

func (p *BalanceWorkerPool) QueueWorkBatch(works []BalanceWork) int {
	queued := 0
	for _, work := range works {
		if p.QueueWork(work) {
			queued++
		}
	}
	return queued
}

// QueueWorkBatchWait queues every item, waiting for room instead of dropping (spike: backpressure). A full
// queue then slows the committer to what the balance workers absorb, instead of losing balance updates.
func (p *BalanceWorkerPool) QueueWorkBatchWait(ctx context.Context, works []BalanceWork) int {
	p.sendMu.RLock()
	defer p.sendMu.RUnlock()
	if p.closed {
		return 0
	}
	queued := 0
	for _, work := range works {
		select {
		case p.workChan <- work:
			queued++
		case <-ctx.Done():
			return queued
		}
	}
	return queued
}

// QueueCommittedWork queues the balance work of a block that is already committed. It keeps waiting for room
// after ctx is cancelled, so shutdown does not drop it, but only for balanceEnqueueGrace: with the node hung the
// queue never drains, and an unbounded wait would hold shutdown until the pod is killed.
func (p *BalanceWorkerPool) QueueCommittedWork(ctx context.Context, works []BalanceWork) int {
	qctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(balanceEnqueueGrace, cancel) })
	defer stop()
	return p.QueueWorkBatchWait(qctx, works)
}

func (p *BalanceWorkerPool) QueueSize() int {
	if p.fetchChan == nil {
		return len(p.workChan)
	}
	return len(p.workChan) + int(p.held.Load()) + len(p.fetchChan)
}

type balanceKey struct {
	address common.Address
	token   common.Address
}

type heldKey struct {
	key balanceKey
	due time.Time
}

// maxHeldKeys bounds the window's memory; past it the oldest key is forwarded early.
const maxHeldKeys = 100000

// coalesce holds each key for dedupWindow, folding repeats into it at the highest block, then forwards it once.
// The fetch reads at latest after the window, so it sees every transfer folded into the key. A key that changes
// again after it was forwarded opens a new window. Forwarding blocks while the workers are busy, which stops
// reads from workChan and so reaches the committer's backpressure.
func (p *BalanceWorkerPool) coalesce() {
	defer p.wg.Done()
	defer close(p.fetchChan)

	held := make(map[balanceKey]*BalanceWork)
	var order []heldKey
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	forwardOldest := func() bool {
		h := order[0]
		order = order[1:]
		w := held[h.key]
		delete(held, h.key)
		select {
		case p.fetchChan <- *w:
			p.held.Add(-1)
			return true
		case <-p.ctx.Done():
			return false
		}
	}

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for len(order) > 0 && !order[0].due.After(now) {
				if !forwardOldest() {
					return
				}
			}
		case work, ok := <-p.workChan:
			if !ok {
				for len(order) > 0 {
					if !forwardOldest() {
						return
					}
				}
				return
			}
			if work.Address == (common.Address{}) {
				continue
			}
			k := balanceKey{address: work.Address, token: work.TokenAddress}
			if h, ok := held[k]; ok {
				if work.BlockNumber > h.BlockNumber {
					h.BlockNumber = work.BlockNumber
				}
				metrics.BalanceDeduped(1)
				continue
			}
			if len(held) >= maxHeldKeys && !forwardOldest() {
				return
			}
			w := work
			held[k] = &w
			order = append(order, heldKey{key: k, due: time.Now().Add(p.dedupWindow)})
			p.held.Add(1)
		}
	}
}

func (p *BalanceWorkerPool) worker(id int) {
	defer p.wg.Done()

	batch := make([]*types.Balance, 0, 100)

	// Periodic flush so low-throughput chains don't strand fetched balances in
	// memory waiting for the size threshold. Size-based flush at 100 still
	// applies for high-throughput periods.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 {
			p.flushBatch(batch)
			batch = batch[:0]
		}
	}

	in := p.workChan
	if p.fetchChan != nil {
		in = p.fetchChan
	}

	for {
		select {
		case <-p.ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case work, ok := <-in:
			if !ok {
				flush()
				return
			}

			if work.Address == (common.Address{}) {
				continue
			}

			if p.readerBatch > 1 {
				batch = append(batch, p.fetchReader(work, in)...)
				if len(batch) >= 100 {
					flush()
				}
				continue
			}

			balance, err := p.fetchBalance(work)
			if err != nil || balance == nil {
				continue // Skip on error, or a token whose balanceOf returned no uint256
			}

			batch = append(batch, balance)

			if len(batch) >= 100 {
				flush()
			}
		}
	}
}

func (p *BalanceWorkerPool) fetchBalance(work BalanceWork) (*types.Balance, error) {
	// balanceOf(address) = 0x70a08231
	addrPadded := common.LeftPadBytes(work.Address.Bytes(), 32)
	callData := append(common.FromHex("0x70a08231"), addrPadded...)

	balanceData, err := p.rpc.CallContract(p.ctx, work.TokenAddress, callData)
	if err != nil {
		return nil, err
	}

	if len(balanceData) < 32 {
		return nil, nil
	}

	balance := new(big.Int).SetBytes(balanceData)

	return &types.Balance{
		Address:      work.Address.Hex(),
		TokenAddress: work.TokenAddress.Hex(),
		BlockNumber:  work.BlockNumber,
		Balance:      types.JSONString(balance.String()),
	}, nil
}

func (p *BalanceWorkerPool) flushBatch(batch []*types.Balance) {
	if len(batch) == 0 {
		return
	}

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := p.db.InsertBalancesBatch(p.ctx, batch)
		if err == nil {
			return
		}

		// Check if it's a retryable error (deadlock or serialization failure)
		errStr := err.Error()
		isRetryable := strings.Contains(errStr, "40P01") || // deadlock
			strings.Contains(errStr, "40001") // serialization failure

		if !isRetryable || attempt == maxRetries-1 {
			log.Error("balance worker: failed to insert batch", "count", len(batch), "error", err)
			return
		}

		// Exponential backoff: 10ms, 20ms, 40ms
		time.Sleep(time.Duration(10<<attempt) * time.Millisecond)
	}
}

// fetchReader drains up to readerBatch-1 more items after first without waiting, keeps one per (address, token) at
// its highest block (reads are at latest), and reads each token's holders with one BalanceReader call. Items whose
// balanceOf failed, or whose call failed, are counted and not written.
func (p *BalanceWorkerPool) fetchReader(first BalanceWork, in <-chan BalanceWork) []*types.Balance {
	works := map[balanceKey]BalanceWork{{first.Address, first.TokenAddress}: first}
	var tokens []common.Address
	holders := map[common.Address][]common.Address{first.TokenAddress: {first.Address}}
	tokens = append(tokens, first.TokenAddress)
	taken := 1
drain:
	for taken < p.readerBatch {
		select {
		case w, ok := <-in:
			if !ok {
				break drain
			}
			taken++
			if w.Address == (common.Address{}) {
				continue
			}
			k := balanceKey{w.Address, w.TokenAddress}
			if prev, seen := works[k]; seen {
				if w.BlockNumber > prev.BlockNumber {
					works[k] = w
				}
				metrics.BalanceDeduped(1)
				continue
			}
			works[k] = w
			if _, ok := holders[w.TokenAddress]; !ok {
				tokens = append(tokens, w.TokenAddress)
			}
			holders[w.TokenAddress] = append(holders[w.TokenAddress], w.Address)
		default:
			break drain
		}
	}
	out := make([]*types.Balance, 0, len(works))
	failed := 0
	for _, token := range tokens {
		hs := holders[token]
		values, ok, err := p.balancesReader.BalancesOf(p.ctx, token, hs, p.readerSkipL1)
		if err != nil {
			log.Warn("balance reader: batched balance call failed; these balances stay stale until their next transfer",
				"token", token.Hex(), "holders", len(hs), "error", err)
			failed += len(hs)
			continue
		}
		for i, h := range hs {
			if !ok[i] {
				failed++
				continue
			}
			w := works[balanceKey{h, token}]
			out = append(out, &types.Balance{
				Address:      w.Address.Hex(),
				TokenAddress: w.TokenAddress.Hex(),
				BlockNumber:  w.BlockNumber,
				Balance:      types.JSONString(values[i].String()),
			})
		}
	}
	metrics.BalanceReads(len(works))
	metrics.BalanceFetchFailed(failed)
	return out
}
