package indexer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// mendingChain fails to trace block 5 until mended, as a node does until its tracer timeout is raised.
type mendingChain struct {
	*fakeChain
	mended bool
}

func (c *mendingChain) FetchTracesBatchStrict(_ context.Context, _ []common.Hash, n, _ uint64, _, _ int) ([]*types.InternalTransaction, error) {
	c.count()
	if n == 5 && !c.mended {
		return nil, errors.New("debug_traceTransaction failed: execution timeout")
	}
	return nil, nil
}

// A span left unchecked is counted as such, by reason, until a later recount settles it.
func TestTheRecountCountsTheSpansItLeftUncheckedUntilOneSettles(t *testing.T) {
	chain := &mendingChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 30, busy: 30}}
	idx := newTestIndexer(new(MockDatabase), chain)
	idx.tracingSupported = true
	store := &twoSpanStore{checked: map[int64]int{}, settled: map[int64]int{}}
	counter := idx.newSpanCounter(1_000_000)
	counter.retry = 0
	for range 2 {
		_, _, _ = idx.RecountPrunedSpan(context.Background(), store, counter, true)
	}
	require.Equal(t, map[int64]string{0: "failing"}, counter.unchecked)

	chain.mended = true
	for store.settled[0] == 0 {
		_, _, err := idx.RecountPrunedSpan(context.Background(), store, counter, true)
		require.NoError(t, err)
	}
	require.Empty(t, counter.unchecked, "the span settled, so it is checked again")
}
