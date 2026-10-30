package indexer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// creatingChain makes every block's transaction a contract creation, and records the per-call rate each receipts
// batch was given.
type creatingChain struct {
	*fakeChain
	mu    sync.Mutex
	rates []int
}

func (c *creatingChain) RawBlockByNumber(ctx context.Context, n uint64) (*rpc.RawBlock, error) {
	b, err := c.fakeChain.RawBlockByNumber(ctx, n)
	if err == nil && len(b.Transactions) > 0 {
		b.Transactions[0].To = nil
	}
	return b, err
}

func (c *creatingChain) FetchReceiptsBatch(ctx context.Context, hashes []common.Hash, w, r int, blockNumber ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	c.mu.Lock()
	c.rates = append(c.rates, r)
	c.mu.Unlock()
	out, err := c.fakeChain.FetchReceiptsBatch(ctx, hashes, w, r, blockNumber...)
	for h, rc := range out {
		rc.ContractAddress = common.BytesToAddress(h.Bytes())
	}
	return out, err
}

func (c *creatingChain) GetCode(context.Context, common.Address) ([]byte, error) {
	c.fakeChain.count()
	return []byte{0x60, 0x80}, nil
}

// The recount's and the backfill's RPS budget holds every call their block builder makes, the contract code reads
// included, and a batch call runs any per-transaction fallback at the budget divided among the job's callers.
func TestTheBackgroundJobsHoldEveryNodeCallToTheirBudget(t *testing.T) {
	chain := &creatingChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 40, busy: 40}}
	idx := newTestIndexer(new(MockDatabase), chain)

	start := time.Now()
	_, err := idx.newSpanCounter(100).count(context.Background(), 0, 30)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 850*time.Millisecond,
		"30 blocks, 30 receipts batches and 30 code reads at 100/s")

	b := idx.newBackfiller(nil, StatsBackfillConfig{RPS: 100, Workers: 4})
	_, err = b.idx.rpc.FetchReceiptsBatch(context.Background(), []common.Hash{chain.txHash(1)}, 1, 100, 1)
	require.NoError(t, err)
	require.Equal(t, 25, chain.rates[len(chain.rates)-1], "a receipts fallback runs at 100/s shared by 4 workers")
}
