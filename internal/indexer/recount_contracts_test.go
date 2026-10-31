package indexer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

var recountCreated = common.HexToAddress("0x00000000000000000000000000000000000c0de5")

// creationChain is fakeChain with block 5's transaction a contract creation; latest and latestErr are the created
// contract's code at the head.
type creationChain struct {
	*fakeChain
	latest    []byte
	latestErr error
}

func (c *creationChain) RawBlockByNumber(ctx context.Context, n uint64) (*rpc.RawBlock, error) {
	b, err := c.fakeChain.RawBlockByNumber(ctx, n)
	if err == nil && n == 5 {
		b.Transactions[0].To = nil
	}
	return b, err
}

func (c *creationChain) FetchReceiptsBatch(ctx context.Context, hashes []common.Hash, w, r int, blockNumber ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	out, err := c.fakeChain.FetchReceiptsBatch(ctx, hashes, w, r, blockNumber...)
	if h := c.txHash(5); out[h] != nil {
		out[h].ContractAddress = recountCreated
	}
	return out, err
}

func (c *creationChain) GetCode(context.Context, common.Address) ([]byte, error) {
	return c.latest, c.latestErr
}

// archiveChain also serves code at a block, as a node keeping old state does.
type archiveChain struct {
	*creationChain
	at []byte
}

func (a *archiveChain) GetCodeAt(context.Context, common.Address, uint64) ([]byte, error) {
	return a.at, nil
}

// A pruned-span recount judges a creation as ingest judged it after the block: the code after that block where the
// node keeps it, else the code at the head when there is some. A failed read, or no code at the head with no older
// state to ask, fails the block instead of counting it without the contract.
func TestARecountJudgesAContractByItsCodeAtTheBlock(t *testing.T) {
	code := []byte{0x60, 0x80}
	for _, c := range []struct {
		name        string
		rpc         RPCClient
		contract    bool
		undecidable bool
	}{
		{"code at the head", &creationChain{latest: code}, true, false},
		{"destroyed since, code at the block", &archiveChain{creationChain: &creationChain{}, at: code}, true, false},
		{"never had code, at the block", &archiveChain{creationChain: &creationChain{latest: code}, at: nil}, false, false},
		{"destroyed since, no old state", &creationChain{}, false, true},
		{"the code read fails", &creationChain{latestErr: errors.New("429 too many requests")}, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20}
			switch r := c.rpc.(type) {
			case *creationChain:
				r.fakeChain = chain
			case *archiveChain:
				r.fakeChain = chain
			}
			idx := newTestIndexer(new(MockDatabase), c.rpc)
			data, err := idx.newSpanCounter(1_000_000).block(context.Background(), 5)
			if c.undecidable {
				require.Error(t, err, "the block fails rather than count without the contract")
				return
			}
			require.NoError(t, err)
			created := strings.ToLower(recountCreated.Hex())
			counted := false
			for _, k := range data.Contracts {
				counted = counted || strings.EqualFold(k.Address, created)
			}
			require.Equal(t, c.contract, counted)
		})
	}
}

// The stats backfill counts the oldest blocks of the chain and is the only writer of their contracts: it judges a
// creation as the recount does, and fails the block where the recount would.
func TestTheBackfillJudgesAContractAsTheRecountDoes(t *testing.T) {
	code := []byte{0x60, 0x80}
	for _, c := range []struct {
		name        string
		rpc         RPCClient
		contract    bool
		undecidable bool
	}{
		{"code at the head", &creationChain{latest: code}, true, false},
		{"destroyed since, code at the block", &archiveChain{creationChain: &creationChain{}, at: code}, true, false},
		{"never had code, at the block", &archiveChain{creationChain: &creationChain{latest: code}, at: nil}, false, false},
		{"destroyed since, no old state", &creationChain{}, false, true},
		{"the code read fails", &creationChain{latestErr: errors.New("429 too many requests")}, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 20}
			switch r := c.rpc.(type) {
			case *creationChain:
				r.fakeChain = chain
			case *archiveChain:
				r.fakeChain = chain
			}
			idx := newTestIndexer(new(MockDatabase), c.rpc)
			out, _, err := idx.newBackfiller(nil, testBackfill).batch(context.Background(), 5, 6, 1_000_000, nil)
			if c.undecidable {
				require.Error(t, err, "the block fails rather than count without the contract")
				return
			}
			require.NoError(t, err)
			counted := false
			for _, k := range out.Contracts {
				counted = counted || strings.EqualFold(k.Address, recountCreated.Hex())
			}
			require.Equal(t, c.contract, counted)
		})
	}
}

// deferStore is one pruned span [0, 10) to recount; it records what the recount did with it.
type deferStore struct {
	deferred []int64
	settled  int
}

func (s *deferStore) NextPrunedSpan(context.Context) (int64, int64, bool, error) {
	return 0, 10, true, nil
}
func (s *deferStore) PrunedSpanFrom(_ context.Context, lo, _ int64) (int64, error) { return lo, nil }
func (s *deferStore) SettleNodeSpan(context.Context, int64, int64, *db.SpanTotals, bool) (db.SpanCheck, error) {
	s.settled++
	return db.SpanCheck{}, nil
}
func (s *deferStore) DeferPrunedSpan(_ context.Context, lo int64) error {
	s.deferred = append(s.deferred, lo)
	return nil
}

// A span whose creation this node cannot judge (no old state, no code at the head) would fail the same way on every
// try; it is put at the back of the queue unsettled, so the recount goes on to the other pruned spans.
func TestARecountDefersASpanItCannotJudge(t *testing.T) {
	chain := &creationChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 20, busy: 21}}
	idx := NewWithConfig(nil, chain, 0, 0, &Config{RPCWorkers: 1, RPCRateLimit: 1000})
	store := &deferStore{}
	counter := idx.newSpanCounter(1000)
	counter.retry = 0
	c, ok, err := idx.RecountPrunedSpan(context.Background(), store, counter, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "undecided", c.State)
	require.Equal(t, []int64{0}, store.deferred)
	require.Zero(t, store.settled, "nothing settled from a guess")
}
