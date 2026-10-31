package indexer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
