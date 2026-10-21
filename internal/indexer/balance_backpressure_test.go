package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// A full balance queue makes the committer wait for room; nothing is dropped.
func TestQueueWorkBatchWaitBlocksForRoomInsteadOfDropping(t *testing.T) {
	p := NewBalanceWorkerPool(nil, nil, 0, 0)
	p.workChan = make(chan BalanceWork, 2)
	works := []BalanceWork{
		{Address: common.HexToAddress("0x1"), BlockNumber: 1},
		{Address: common.HexToAddress("0x2"), BlockNumber: 1},
		{Address: common.HexToAddress("0x3"), BlockNumber: 1},
	}

	done := make(chan int, 1)
	go func() { done <- p.QueueWorkBatchWait(context.Background(), works) }()

	select {
	case n := <-done:
		t.Fatalf("returned %d with a full queue; want it to wait for room", n)
	case <-time.After(100 * time.Millisecond):
	}
	<-p.workChan // a worker takes one item
	select {
	case n := <-done:
		if n != 3 {
			t.Fatalf("queued %d, want 3", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("still blocked after room was made")
	}
}

// Only a cancelled context ends the wait early (shutdown); the return value says how many were queued.
func TestQueueWorkBatchWaitStopsOnContextCancel(t *testing.T) {
	p := NewBalanceWorkerPool(nil, nil, 0, 0)
	p.workChan = make(chan BalanceWork, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	n := p.QueueWorkBatchWait(ctx, []BalanceWork{{BlockNumber: 1}, {BlockNumber: 2}})
	if n != 1 {
		t.Fatalf("queued %d before the cancel, want 1", n)
	}
}
