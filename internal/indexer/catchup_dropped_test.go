package indexer

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// runWorkerOn hands one block to a catch-up worker and waits until it is processed or the deadline passes.
func runWorkerOn(t *testing.T, catchup *CatchupIndexer, blockNum uint64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	catchup.ctx, catchup.cancel = ctx, cancel
	catchup.wg.Add(1)
	go catchup.worker(0)
	catchup.workQueue <- blockNum
	require.Eventually(t, func() bool { return atomic.LoadInt64(&catchup.processedBlocks) == 1 }, 3*time.Second, 5*time.Millisecond,
		"block %d was dequeued and never processed", blockNum)
	cancel()
	catchup.wg.Wait()
}

func withShortCatchupRetry(t *testing.T) {
	t.Helper()
	before := catchupRetryWait
	catchupRetryWait = time.Millisecond
	t.Cleanup(func() { catchupRetryWait = before })
}

// A dequeued block whose existence check fails (a dropped connection) is checked again and processed, not dropped:
// its range row is already gone, so nothing else would bring it back while the process runs.
func TestCatchupWorkerRetriesABlockWhoseExistenceCheckFails(t *testing.T) {
	withShortCatchupRetry(t)
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	catchup := newTestCatchupIndexer(mockDB, mockRPC)
	mockDB.On("HasBlock", mock.Anything, uint64(5)).Return(false, fmt.Errorf("connection reset")).Once()
	mockDB.On("HasBlock", mock.Anything, uint64(5)).Return(false, nil)
	mockRPC.On("RawBlockByNumber", mock.Anything, uint64(5)).Return(makeRawBlock(5), nil)
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("DeleteMissingRangeByBlock", mock.Anything, uint64(5)).Return(nil)

	runWorkerOn(t, catchup, 5)
}

// A block whose write fails and whose requeue fails too is kept and tried again until it is stored (or durably
// recorded as missing), not dropped.
func TestCatchupWorkerKeepsABlockWhoseWriteAndRequeueFail(t *testing.T) {
	withShortCatchupRetry(t)
	mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
	catchup := newTestCatchupIndexer(mockDB, mockRPC)
	mockDB.On("HasBlock", mock.Anything, uint64(5)).Return(false, nil)
	mockRPC.On("RawBlockByNumber", mock.Anything, uint64(5)).Return(nil, fmt.Errorf("connection refused")).Once()
	mockRPC.On("RawBlockByNumber", mock.Anything, uint64(5)).Return(makeRawBlock(5), nil)
	mockDB.On("RequeueMissingBlock", mock.Anything, uint64(5)).Return(fmt.Errorf("connection reset")).Once()
	mockDB.On("InsertBlock", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("DeleteMissingRangeByBlock", mock.Anything, uint64(5)).Return(nil)

	runWorkerOn(t, catchup, 5)
}
