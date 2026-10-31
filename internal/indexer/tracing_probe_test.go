package indexer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// errNoHistoricalState is a node without old state answering a trace of an old block.
var errNoHistoricalState = errors.New("historical state is not available")

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

func (p *probedChain) TraceBlock(context.Context, uint64) (map[string]rpc.TraceBlockResult, error) {
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

// initStore is a database with no stats backfill yet; it records whether one was prepared.
type initStore struct{ prepared bool }

func (s *initStore) StatsBackfillState(context.Context) (db.BackfillState, error) { return db.BackfillState{}, nil }
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
