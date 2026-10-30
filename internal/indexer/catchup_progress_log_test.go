package indexer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Every worker reaches the progress line; one of them logs it per 5 s, however many reach it at once, and the
// throttle is safe under the race detector.
func TestCatchupProgressLogIsClaimedByOneWorkerPerInterval(t *testing.T) {
	c := &CatchupIndexer{}
	start := time.Now()
	c.lastLogNanos.Store(start.Add(-time.Minute).UnixNano())

	var claimed atomic.Int32
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.claimProgressLog(start) {
				claimed.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := claimed.Load(); n != 1 {
		t.Fatalf("%d workers logged the progress line at once, want 1", n)
	}
	if c.claimProgressLog(start.Add(4 * time.Second)) {
		t.Fatal("logged again within 5 s")
	}
	if !c.claimProgressLog(start.Add(6 * time.Second)) {
		t.Fatal("not logged again after 5 s")
	}
}
