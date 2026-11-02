package indexer

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// untracedArchive keeps old state, so it serves a creation's code at its block, but traces no old block.
type untracedArchive struct {
	*archiveChain
}

func (u *untracedArchive) FetchTracesBatch(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, errNoHistoricalState
}

func (u *untracedArchive) FetchTracesBatchStrict(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, errNoHistoricalState
}

// untracedCreation keeps no old state: it traces no old block and serves no code at one.
type untracedCreation struct {
	*creationChain
}

func (u *untracedCreation) FetchTracesBatch(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, errNoHistoricalState
}

func (u *untracedCreation) FetchTracesBatchStrict(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, errNoHistoricalState
}

// A block counted without its traces, after the node refused them strictBlockTries times, still judges its creations
// by their code at the block: a contract destroyed since is counted. A creation the node cannot judge (no state at the
// block, no code at the head) is counted as no contract, as ingest would have, rather than fail the block again.
func TestABlockCountedWithoutItsTracesJudgesAContractByItsCodeAtTheBlock(t *testing.T) {
	ctx := context.Background()
	tries := strictBlockTries
	strictBlockTries = 3
	t.Cleanup(func() { strictBlockTries = tries })
	for _, c := range []struct {
		name     string
		rpc      func(*fakeChain) RPCClient
		contract bool
	}{
		{"destroyed since, code at the block", func(f *fakeChain) RPCClient {
			return &untracedArchive{archiveChain: &archiveChain{creationChain: &creationChain{fakeChain: f}, at: []byte{0x60, 0x80}}}
		}, true},
		{"destroyed since, no old state", func(f *fakeChain) RPCClient {
			return &untracedCreation{creationChain: &creationChain{fakeChain: f}}
		}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			idx := newTestIndexer(new(MockDatabase), c.rpc(&fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20}))
			idx.tracingSupported = true
			b := idx.newBackfiller(nil, testBackfill)
			for range strictBlockTries - 1 {
				_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
				require.ErrorIs(t, err, errNoHistoricalState)
			}
			out, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
			require.NoError(t, err)
			counted := false
			for _, k := range out.Contracts {
				counted = counted || strings.EqualFold(k.Address, recountCreated.Hex())
			}
			require.Equal(t, c.contract, counted)
		})
	}
}
