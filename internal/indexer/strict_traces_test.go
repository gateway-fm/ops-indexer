package indexer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// partialTraceChain answers a block's traces leniently with what it has, and strictly with an error: one of the
// block's transactions could not be traced.
type partialTraceChain struct{ *fakeChain }

func (p *partialTraceChain) FetchTracesBatch(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, nil
}

func (p *partialTraceChain) FetchTracesBatchStrict(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, errors.New("trace of transaction 0x02: trace failed")
}

// Counts that replace or extend the ledger, the pruned-span recount and the stats backfill, fail a block whose traces
// are incomplete; ingest keeps the lenient fetch and leaves the missing internal calls out.
func TestTheRecountAndTheBackfillFailABlockWithIncompleteTraces(t *testing.T) {
	chain := &partialTraceChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20}}
	idx := newTestIndexer(new(MockDatabase), chain)
	idx.tracingSupported = true

	_, err := idx.newSpanCounter(1_000_000).block(context.Background(), 5)
	require.ErrorContains(t, err, "trace failed", "the recount fails the block")

	require.True(t, idx.newBackfiller(nil, StatsBackfillConfig{RPS: 100, Workers: 1}).idx.config.StrictTraces,
		"the backfill fetches traces strictly")

	raw, err := chain.RawBlockByNumber(context.Background(), 5)
	require.NoError(t, err)
	receipts, err := chain.FetchReceiptsBatch(context.Background(), idx.receiptTxHashes(raw), 1, 1, 5)
	require.NoError(t, err)
	b := idx.buildBlock(context.Background(), raw, receipts)
	require.NoError(t, b.tracesErr, "ingest leaves the untraced calls out")
}
