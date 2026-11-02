package indexer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
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

func (u *untracedArchive) FetchTracesBatchSkippingRefusals(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, nil
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

func (u *untracedCreation) FetchTracesBatchSkippingRefusals(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	return nil, nil
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

// errNodeDown is a node that does not answer at all.
var errNodeDown = errors.New("dial tcp 192.0.2.1:8545: connect: connection refused")

var tracedCallee = "0x00000000000000000000000000000000000000c3"

// scriptedTraces answers its trace calls in turn from answers: an error the node answers with, errNodeDown the node
// down, nil (and every call past the list) the block's one internal call, from its recipient to tracedCallee.
type scriptedTraces struct {
	*fakeChain
	mu      sync.Mutex
	answers []error
}

func (s *scriptedTraces) next() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.answers) == 0 {
		return nil
	}
	err := s.answers[0]
	s.answers = s.answers[1:]
	return err
}

func (s *scriptedTraces) call(block uint64) []*types.InternalTransaction {
	to := tracedCallee
	return []*types.InternalTransaction{{BlockNumber: block, From: strings.ToLower(chainRecipient.Hex()), To: &to, CallType: "call"}}
}

func (s *scriptedTraces) FetchTracesBatch(_ context.Context, _ []common.Hash, block uint64, _ uint64, _ int, _ int) ([]*types.InternalTransaction, error) {
	if s.next() != nil {
		return nil, nil
	}
	return s.call(block), nil
}

func (s *scriptedTraces) FetchTracesBatchStrict(_ context.Context, _ []common.Hash, block uint64, _ uint64, _ int, _ int) ([]*types.InternalTransaction, error) {
	if err := s.next(); err != nil {
		return nil, err
	}
	return s.call(block), nil
}

func (s *scriptedTraces) FetchTracesBatchSkippingRefusals(_ context.Context, _ []common.Hash, block uint64, _ uint64, _ int, _ int) ([]*types.InternalTransaction, error) {
	switch err := s.next(); {
	case err == nil:
		return s.call(block), nil
	case rpc.NodeRefused(err):
		return nil, nil
	default:
		return nil, err
	}
}

// downArchive keeps old state but does not answer its first downFor reads of code at a block.
type downArchive struct {
	*creationChain
	downFor int
	reads   int
}

func (d *downArchive) GetCodeAt(context.Context, common.Address, uint64) ([]byte, error) {
	d.reads++
	if d.reads <= d.downFor {
		return nil, errNodeDown
	}
	return []byte{0x60, 0x80}, nil
}

// Only the node answering that it cannot serve a block's traces or a creation's code counts toward strictBlockTries. A
// node down for longer than those tries fails the block on every try, and the block is counted whole once the node
// is back. A block counted without its traces is counted only while the node answers: a node down meanwhile fails it.
func TestOnlyTheNodeRefusingABlockCountsTowardCountingItWithoutWhatFailed(t *testing.T) {
	ctx := context.Background()
	tries := strictBlockTries
	strictBlockTries = 3
	t.Cleanup(func() { strictBlockTries = tries })
	internalCalls := func(out *db.BackfillBatch) int {
		if d := out.Shared.AddressStats[tracedCallee]; d != nil {
			return d.InternalTxCountDelta
		}
		return 0
	}
	newBackfiller := func(r RPCClient, traces bool) *backfiller {
		idx := newTestIndexer(new(MockDatabase), r)
		idx.tracingSupported = traces
		return idx.newBackfiller(nil, testBackfill)
	}

	t.Run("traces, the node down for longer than the tries", func(t *testing.T) {
		down := make([]error, strictBlockTries+2)
		for i := range down {
			down[i] = errNodeDown
		}
		b := newBackfiller(&scriptedTraces{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20},
			answers: down}, true)
		for range len(down) {
			_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
			require.ErrorIs(t, err, errNodeDown)
		}
		out, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
		require.NoError(t, err)
		require.Equal(t, 1, internalCalls(out), "the block is counted with its internal call")
	})

	t.Run("code at the block, the node down for longer than the tries", func(t *testing.T) {
		chain := &downArchive{creationChain: &creationChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20,
			busy: 20}}, downFor: strictBlockTries + 2}
		b := newBackfiller(chain, false)
		for range chain.downFor {
			_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
			require.ErrorIs(t, err, errNodeDown)
		}
		out, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
		require.NoError(t, err)
		counted := false
		for _, k := range out.Contracts {
			counted = counted || strings.EqualFold(k.Address, recountCreated.Hex())
		}
		require.True(t, counted, "the contract is counted")
	})

	t.Run("the node down while the block is counted without its traces", func(t *testing.T) {
		answers := make([]error, strictBlockTries)
		for i := range answers {
			answers[i] = errNoHistoricalState
		}
		answers = append(answers, errNodeDown, errNoHistoricalState, errNoHistoricalState)
		b := newBackfiller(&scriptedTraces{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20},
			answers: answers}, true)
		for range strictBlockTries - 1 {
			_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
			require.ErrorIs(t, err, errNoHistoricalState)
		}
		_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
		require.ErrorIs(t, err, errNodeDown, "not counted while the node is down")
		out, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
		require.NoError(t, err)
		require.Zero(t, internalCalls(out), "counted without its traces once the node answers again")
	})
}

// errOutOfRange is a node refusing an old block in its own words.
var errOutOfRange error = &rpclient.RPCError{Code: -32602, Message: "BlockOutOfRangeError: block height is 60000 but requested was 5"}

// outOfRangeChain keeps no old state and refuses the code at an old block in its own words.
type outOfRangeChain struct {
	*creationChain
}

func (o *outOfRangeChain) GetCodeAt(context.Context, common.Address, uint64) ([]byte, error) {
	return nil, errOutOfRange
}

// The node refusing a block's code or traces counts toward strictBlockTries whatever words it refuses in, so the block
// is counted without what failed. A timeout or a rate limit does not count: the block fails on every try until the
// node serves it.
func TestANodeRefusingInItsOwnWordsCountsTowardCountingABlockWithoutWhatFailed(t *testing.T) {
	ctx := context.Background()
	tries := strictBlockTries
	strictBlockTries = 3
	t.Cleanup(func() { strictBlockTries = tries })
	internalCalls := func(out *db.BackfillBatch) int {
		if d := out.Shared.AddressStats[tracedCallee]; d != nil {
			return d.InternalTxCountDelta
		}
		return 0
	}
	newBackfiller := func(r RPCClient, traces bool) *backfiller {
		idx := newTestIndexer(new(MockDatabase), r)
		idx.tracingSupported = traces
		return idx.newBackfiller(nil, testBackfill)
	}
	answering := func(err error, n int) []error {
		answers := make([]error, n)
		for i := range answers {
			answers[i] = err
		}
		return answers
	}

	t.Run("code at the block", func(t *testing.T) {
		b := newBackfiller(&outOfRangeChain{creationChain: &creationChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient),
			head: 20, busy: 20}}}, false)
		for range strictBlockTries - 1 {
			_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
			require.Error(t, err)
		}
		out, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
		require.NoError(t, err)
		for _, k := range out.Contracts {
			require.False(t, strings.EqualFold(k.Address, recountCreated.Hex()), "a creation the node cannot judge is no contract")
		}
	})

	t.Run("traces", func(t *testing.T) {
		b := newBackfiller(&scriptedTraces{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20},
			answers: answering(errOutOfRange, strictBlockTries+1)}, true)
		for range strictBlockTries - 1 {
			_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
			require.ErrorIs(t, err, errOutOfRange)
		}
		out, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
		require.NoError(t, err)
		require.Zero(t, internalCalls(out), "counted without its traces")
	})

	for _, notRefused := range []error{
		&rpclient.RPCError{Code: -32000, Message: "execution timeout"},
		&rpclient.RPCError{Code: -32005, Message: "limit exceeded"},
	} {
		t.Run(notRefused.Error(), func(t *testing.T) {
			answers := answering(notRefused, strictBlockTries+2)
			b := newBackfiller(&scriptedTraces{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20},
				answers: answers}, true)
			for range len(answers) {
				_, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
				require.ErrorIs(t, err, notRefused)
			}
			out, _, err := b.batch(ctx, 5, 6, 1_000_000, nil)
			require.NoError(t, err)
			require.Equal(t, 1, internalCalls(out), "the block is counted with its internal call")
		})
	}
}
