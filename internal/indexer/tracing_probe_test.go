package indexer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// errNoHistoricalState is a node without old state answering a trace of an old block.
var errNoHistoricalState error = &rpclient.RPCError{Code: -32000, Message: "historical state is not available"}

// probedChain answers the start's tracing check with probe (an error, or whether the node traces), and traces no old
// block: it keeps no historical state.
type probedChain struct {
	*fakeChain
	supported bool
	probeErr  error
	probes    int
	// traceBlockErr is the node's answer to a trace of a whole old block.
	traceBlockErr error
}

func (p *probedChain) TraceOldBlock(context.Context, uint64) (map[string]rpc.TraceBlockResult, error) {
	return nil, p.traceBlockErr
}

func (p *probedChain) CheckTracingSupport(context.Context) (bool, error) {
	p.probes++
	return p.supported, p.probeErr
}

func (p *probedChain) FetchTracesBatch(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, errNoHistoricalState
}

func (p *probedChain) FetchTracesBatchStrict(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, errNoHistoricalState
}

func (p *probedChain) FetchTracesBatchSkippingRefusals(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, nil
}

// A tracing check that fails is retried, and one that keeps failing is no answer: the node recount and the stats
// backfill still fetch traces, and fail a block they cannot trace rather than count it without its internal calls. A
// node that answers that it does not trace is counted without traces, as ingest counts it.
func TestAFailedTracingCheckNeverCountsAnOldBlockWithoutTraces(t *testing.T) {
	retry := tracingProbeRetry
	tracingProbeRetry = time.Millisecond
	t.Cleanup(func() { tracingProbeRetry = retry })
	for _, c := range []struct {
		name      string
		chain     *probedChain
		untraced  bool
		failCount bool
	}{
		{"the check fails", &probedChain{probeErr: errors.New("tracing support check: i/o timeout")}, true, true},
		{"the node does not trace", &probedChain{}, true, false},
		{"the node traces", &probedChain{supported: true}, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.chain.fakeChain = &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20}
			idx := newTestIndexer(new(MockDatabase), c.chain)
			idx.config.EnableTracing = true
			idx.probeTracing(context.Background())
			require.Equal(t, c.untraced, !idx.tracingSupported, "ingest traces only on a node that answered it does")
			if c.chain.probeErr != nil {
				require.Equal(t, tracingProbes, c.chain.probes, "the check is retried")
			}

			_, err := idx.newSpanCounter(1_000_000).block(context.Background(), 5)
			_, _, berr := idx.newBackfiller(nil, testBackfill).batch(context.Background(), 5, 6, 1_000_000, nil)
			if c.failCount {
				require.ErrorIs(t, err, errNoHistoricalState, "the recount fails the block")
				require.ErrorIs(t, berr, errNoHistoricalState, "the backfill fails the block")
				return
			}
			require.NoError(t, err)
			require.NoError(t, berr)
		})
	}
}

// initStore is a database with the stats backfill state; it records whether one was prepared.
type initStore struct {
	state    db.BackfillState
	prepared bool
}

func (s *initStore) StatsBackfillState(context.Context) (db.BackfillState, error) { return s.state, nil }
func (s *initStore) InitStatsBackfill(context.Context, uint64, uint64) error {
	s.prepared = true
	return nil
}

// An old block a node without historical state cannot trace fails on every try. The node recount puts its span at the
// back of the queue; the stats backfill refuses to start on such a node, and counts a single block that keeps failing
// without what failed after strictBlockTries tries. Neither retries a block forever, so neither holds retention.
func TestAnOldBlockTheNodeCannotTraceIsNeverRetriedForever(t *testing.T) {
	ctx := context.Background()
	newChain := func() *probedChain {
		return &probedChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20_000, busy: 20},
			supported: true, traceBlockErr: errNoHistoricalState}
	}

	t.Run("the node recount defers the span", func(t *testing.T) {
		idx := newTestIndexer(new(MockDatabase), newChain())
		idx.tracingSupported = true
		store := &deferStore{}
		counter := idx.newSpanCounter(1_000_000)
		counter.retry = 0
		for range nodeRecountFailuresBeforeDefer {
			_, _, _ = idx.RecountPrunedSpan(ctx, store, counter, true)
		}
		require.Equal(t, []int64{0}, store.deferred)
		require.Zero(t, store.settled)
	})

	t.Run("the stats backfill fails its prepare", func(t *testing.T) {
		store := &initStore{}
		err := PrepareStatsBackfill(ctx, store, newChain(), 5_000, true)
		require.ErrorIs(t, err, errNoHistoricalState)
		require.ErrorContains(t, err, "archive node")
		require.False(t, store.prepared)

		absent := newChain()
		absent.traceBlockErr = errors.New("the method debug_traceBlockByNumber does not exist/is not available")
		require.NoError(t, PrepareStatsBackfill(ctx, store, absent, 5_000, true), "a node that does not trace")
		require.True(t, store.prepared)
		require.NoError(t, PrepareStatsBackfill(ctx, &initStore{}, newChain(), 5_000, false), "tracing off")
	})

	t.Run("the stats backfill counts a block that keeps failing without what failed", func(t *testing.T) {
		tries := strictBlockTries
		strictBlockTries = 3
		t.Cleanup(func() { strictBlockTries = tries })
		idx := newTestIndexer(new(MockDatabase), newChain())
		idx.tracingSupported = true
		b := idx.newBackfiller(nil, testBackfill)
		for range strictBlockTries - 1 {
			_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
			require.ErrorIs(t, err, errNoHistoricalState)
		}
		out, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
		require.NoError(t, err)
		require.NotEmpty(t, out.Spans, "the block is counted")
	})
}

// A backfill prepared on an archive node can be resumed on a node without old state: every start checks the node's
// history again while blocks remain to count, and fails as the prepare would.
func TestEveryStartRechecksTheNodesHistoryWhileTheBackfillHasBlocksToCount(t *testing.T) {
	ctx := context.Background()
	node := &probedChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20_000, busy: 20},
		supported: true, traceBlockErr: errNoHistoricalState}
	counting := &initStore{state: db.BackfillState{Exists: true, Target: 4_999, Through: 1_000}}
	err := PrepareStatsBackfill(ctx, counting, node, 5_000, true)
	require.ErrorIs(t, err, errNoHistoricalState)
	require.ErrorContains(t, err, "archive node")
	require.False(t, counting.prepared, "the backfill is resumed, not prepared again")

	for _, done := range []db.BackfillState{
		{Exists: true, Target: 4_999, Through: 4_999},
		{Exists: true, Complete: true, Target: 4_999, Through: 4_999},
	} {
		require.NoError(t, PrepareStatsBackfill(ctx, &initStore{state: done}, node, 5_000, true),
			"no block left to trace")
	}
	require.NoError(t, PrepareStatsBackfill(ctx, counting, node, 5_000, false), "tracing off")
}

// countingChain counts the blocks fetched; its traces fail until failUntil tries of block 7 have failed.
type countingChain struct {
	*probedChain
	mu       sync.Mutex
	fetched  map[uint64]int
	failures int
	failFor  int
}

func (c *countingChain) RawBlockByNumber(ctx context.Context, n uint64) (*rpc.RawBlock, error) {
	c.mu.Lock()
	c.fetched[n]++
	c.mu.Unlock()
	return c.probedChain.RawBlockByNumber(ctx, n)
}

func (c *countingChain) FetchTracesBatchStrict(_ context.Context, _ []common.Hash, block uint64, _ uint64, _ int, _ int) ([]*types.InternalTransaction, error) {
	if block == 7 && c.failures < c.failFor {
		c.failures++
		return nil, errNoHistoricalState
	}
	return nil, nil
}

// A batch retried after one of its blocks failed fetches none of its blocks again, and builds only from the block
// that failed.
func TestABackfillBatchRetryFetchesNoBlockAgain(t *testing.T) {
	ctx := context.Background()
	chain := &countingChain{probedChain: &probedChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20,
		busy: 20}, supported: true}, fetched: map[uint64]int{}, failFor: 2}
	idx := newTestIndexer(new(MockDatabase), chain)
	idx.tracingSupported = true
	b := idx.newBackfiller(nil, testBackfill)
	for range chain.failFor {
		_, _, err := b.batch(ctx, 5, 10, 1_000_000, nil)
		require.ErrorIs(t, err, errNoHistoricalState)
	}
	out, _, err := b.batch(ctx, 5, 10, 1_000_000, nil)
	require.NoError(t, err)
	require.NotEmpty(t, out.Spans)
	for n := uint64(5); n < 10; n++ {
		require.Equal(t, 1, chain.fetched[n], "block %d fetched once", n)
	}
}
