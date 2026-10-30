package indexer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRealtimeReportsHowFarBehindItIs(t *testing.T) {
	rt := realtimeAt(new(MockDatabase), 100, 0)
	assert.Zero(t, rt.Behind(), "before it has seen the node's head")
	assert.Equal(t, uint64(100), rt.jumpIfFarBehind(context.Background(), 150))
	assert.Equal(t, uint64(50), rt.Behind())
	rt.SetLastProcessedBlock(150)
	assert.Zero(t, rt.Behind(), "caught up")
}

// After a stall, catch-up's workers took every pool connection and realtime fell behind again (load test: 1.6 b/s
// against catch-up's 45, three more jumps). Catch-up now waits while realtime is behind, until it is nearly level.
func TestCatchupYieldsWhileRealtimeIsBehind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var behind atomic.Uint64
	c := &CatchupIndexer{ctx: ctx}
	c.SetRealtimeBehind(behind.Load)

	behind.Store(catchupYieldAbove)
	start := time.Now()
	c.yieldToRealtime(0)
	assert.Less(t, time.Since(start), 50*time.Millisecond, "at the threshold catch-up goes on")

	behind.Store(500)
	done := make(chan struct{})
	go func() { c.yieldToRealtime(0); close(done) }()
	time.Sleep(300 * time.Millisecond)
	behind.Store(catchupResumeBelow + 10)
	time.Sleep(500 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("catch-up resumed before realtime was nearly level")
	default:
	}
	behind.Store(catchupResumeBelow)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("catch-up did not resume once realtime caught up")
	}
}
