package indexer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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
