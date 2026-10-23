package main

import (
	"context"
	"errors"
	"testing"
)

// A worker that fails (the indexer refusing to start on a stale fork) cancels the rest and makes the
// process exit 1; a shutdown by signal exits 0.
func TestWorkerFailureExitsNonZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := &workerStop{cancel: cancel}
	if code := stop.exitCode(); code != 0 {
		t.Fatalf("exit %d with no failure, want 0", code)
	}
	stop.fail("indexer worker error", errors.New("the stored head 112 is on a stale fork and reverting it failed"))
	if ctx.Err() == nil {
		t.Fatal("a failure does not cancel the other workers")
	}
	if code := stop.exitCode(); code != 1 {
		t.Fatalf("exit %d after a worker failed, want 1", code)
	}
}
