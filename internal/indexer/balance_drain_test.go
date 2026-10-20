package indexer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// Stop must write the balance work still queued, not drop it: a dropped key keeps a stale current balance until
// it moves again, because the startup reconcile rebuilds from stored history, not from the chain.
func TestStopDrainsQueuedBalanceWork(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	mockRPC.On("CallContract", mock.Anything, mock.Anything, mock.Anything).After(2*time.Millisecond).Return(make([]byte, 32), nil) // slow enough that work is still queued at Stop
	var mu sync.Mutex
	written := map[string]bool{}
	mockDB.On("InsertBalancesBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		mu.Lock()
		defer mu.Unlock()
		for _, b := range args.Get(1).([]*types.Balance) {
			written[b.Address] = true
		}
	}).Return(nil)

	p := NewBalanceWorkerPool(mockDB, mockRPC, 2, 0)
	p.Start()
	const n = 50
	works := make([]BalanceWork, n)
	for i := range works {
		works[i] = BalanceWork{Address: common.HexToAddress(fmt.Sprintf("0x%040x", i+1)), TokenAddress: common.HexToAddress("0x01"), BlockNumber: 1}
	}
	if got := p.QueueWorkBatchWait(t.Context(), works); got != n {
		t.Fatalf("queued %d of %d", got, n)
	}

	p.Stop()

	mu.Lock()
	defer mu.Unlock()
	for _, w := range works {
		if !written[w.Address.Hex()] {
			t.Fatalf("balance of %s dropped at Stop (%d of %d written)", w.Address.Hex(), len(written), n)
		}
	}
	if p.QueueWork(BalanceWork{Address: common.HexToAddress("0xff"), BlockNumber: 2}) {
		t.Fatal("QueueWork after Stop must be refused, not sent on the closed queue")
	}
}

// A committed block's producer keeps waiting for queue room after shutdown begins, but only for the grace period:
// with the node hung the queue never drains, and an unbounded wait would hold shutdown until the pod is killed.
func TestQueueCommittedWorkWaitsOnlyAGraceAfterShutdown(t *testing.T) {
	defer func(g time.Duration) { balanceEnqueueGrace = g }(balanceEnqueueGrace)
	balanceEnqueueGrace = 100 * time.Millisecond

	p := NewBalanceWorkerPool(nil, nil, 0, 0)
	p.workChan = make(chan BalanceWork, 1)
	p.workChan <- BalanceWork{BlockNumber: 1}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- p.QueueCommittedWork(ctx, []BalanceWork{{BlockNumber: 2}}) }()
	select {
	case n := <-done:
		t.Fatalf("returned %d with a live context and a full queue; want it to wait", n)
	case <-time.After(50 * time.Millisecond):
	}

	cancelled := time.Now()
	cancel()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("queued %d into a full queue", n)
		}
		if waited := time.Since(cancelled); waited < balanceEnqueueGrace {
			t.Fatalf("gave up %v after shutdown began; want it to wait the %v grace", waited, balanceEnqueueGrace)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("still waiting long after the grace period; shutdown would hang")
	}
}

// With a dedup window every key is held in memory when the pod is told to stop; Stop must forward and write the
// held keys too, not only what is still in the queue.
func TestStopDrainsHeldBalanceWork(t *testing.T) {
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	mockRPC.On("CallContract", mock.Anything, mock.Anything, mock.Anything).Return(make([]byte, 32), nil)
	var mu sync.Mutex
	written := map[string]bool{}
	mockDB.On("InsertBalancesBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		mu.Lock()
		defer mu.Unlock()
		for _, b := range args.Get(1).([]*types.Balance) {
			written[b.Address] = true
		}
	}).Return(nil)

	p := NewBalanceWorkerPool(mockDB, mockRPC, 2, 0)
	p.SetDedupWindow(10 * time.Second) // longer than the test: every key is still held at Stop
	p.Start()
	const n = 50
	works := make([]BalanceWork, n)
	for i := range works {
		works[i] = BalanceWork{Address: common.HexToAddress(fmt.Sprintf("0x%040x", i+1)), TokenAddress: common.HexToAddress("0x01"), BlockNumber: 1}
	}
	if got := p.QueueWorkBatchWait(t.Context(), works); got != n {
		t.Fatalf("queued %d of %d", got, n)
	}

	p.Stop()

	mu.Lock()
	defer mu.Unlock()
	for _, w := range works {
		if !written[w.Address.Hex()] {
			t.Fatalf("held balance of %s dropped at Stop (%d of %d written)", w.Address.Hex(), len(written), n)
		}
	}
}
