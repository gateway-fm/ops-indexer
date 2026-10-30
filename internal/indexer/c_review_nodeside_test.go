package indexer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// failedCreationChain is creationChain with block 5's creation reverted: its receipt has status 0 and, as geth sets
// it for every creation, the address the contract would have had.
type failedCreationChain struct {
	*creationChain
}

func (c *failedCreationChain) FetchReceiptsBatch(ctx context.Context, hashes []common.Hash, w, r int, blockNumber ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	out, err := c.creationChain.FetchReceiptsBatch(ctx, hashes, w, r, blockNumber...)
	if h := c.txHash(5); out[h] != nil {
		out[h].Status = 0
	}
	return out, err
}

// A reverted creation deployed nothing: ingest found no code for it, and the receipt alone says so. On a node without
// old state (no code at the block, none at the head) the recount must still count the block, not call it undecided
// and leave every span holding a failed deployment unchecked.
func TestARecountCountsABlockWithAFailedCreationOnANodeWithoutOldState(t *testing.T) {
	chain := &failedCreationChain{creationChain: &creationChain{
		fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20}}}
	idx := newTestIndexer(new(MockDatabase), chain)
	data, err := idx.newSpanCounter(1_000_000).block(context.Background(), 5)
	require.NoError(t, err, "a reverted creation is no contract, whatever the node keeps")
	require.Empty(t, data.Contracts)
}

// traceFailChain cannot trace block 5, on every try: a transaction whose trace always exceeds the node's tracer
// timeout, or a block whose state the node no longer keeps.
type traceFailChain struct {
	*fakeChain
}

func (c *traceFailChain) FetchTracesBatchStrict(_ context.Context, _ []common.Hash, n, _ uint64, _, _ int) ([]*types.InternalTransaction, error) {
	c.count()
	if n == 5 {
		return nil, errors.New("debug_traceTransaction failed: execution timeout")
	}
	return nil, nil
}

// twoSpanStore holds the pruned spans [0, 10) and [10, 20), handing out the one checked longest ago as
// NextPrunedSpan does.
type twoSpanStore struct {
	checked map[int64]int
	settled map[int64]int
}

func (s *twoSpanStore) NextPrunedSpan(context.Context) (int64, int64, bool, error) {
	if s.checked[0] <= s.checked[10] {
		return 0, 10, true, nil
	}
	return 10, 20, true, nil
}
func (s *twoSpanStore) PrunedSpanFrom(_ context.Context, lo, _ int64) (int64, error) { return lo, nil }
func (s *twoSpanStore) SettleNodeSpan(_ context.Context, lo, hi int64, _ *db.SpanTotals, _ bool) (db.SpanCheck, error) {
	s.checked[lo]++
	s.settled[lo]++
	return db.SpanCheck{Lo: lo, Hi: hi, State: "ok"}, nil
}
func (s *twoSpanStore) DeferPrunedSpan(_ context.Context, lo int64) error {
	s.checked[lo]++
	return nil
}

// A span with a block the node fails to trace on every try fails its recount every time, and stays the span checked
// longest ago: the recount must still reach the other pruned spans, as it does for a creation it cannot judge.
func TestARecountReachesOtherSpansPastABlockThatAlwaysFails(t *testing.T) {
	chain := &traceFailChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 30, busy: 30}}
	idx := newTestIndexer(new(MockDatabase), chain)
	idx.tracingSupported = true
	store := &twoSpanStore{checked: map[int64]int{}, settled: map[int64]int{}}
	counter := idx.newSpanCounter(1_000_000)
	counter.retry = 0
	for range 3 {
		_, _, _ = idx.RecountPrunedSpan(context.Background(), store, counter, true)
	}
	require.NotZero(t, store.settled[10], "[10, 20) is never recounted while [0, 10) fails")
}
