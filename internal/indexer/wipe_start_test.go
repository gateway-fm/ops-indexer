package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// prunedMockDB is a MockDatabase whose history is pruned through `through` until WipeAllData resets it.
type prunedMockDB struct {
	*MockDatabase
	through uint64
}

func (m *prunedMockDB) PrunedThrough(ctx context.Context) (uint64, int64, error) {
	return m.through, 0, nil
}

func (m *prunedMockDB) WipeAllData(ctx context.Context) error {
	m.through = 0
	return m.MockDatabase.WipeAllData(ctx)
}

// FORCE_REINDEX after a chain reset on a pruned database: the start block was moved above the old chain's
// watermark before the wipe, and goes back to the configured start block with it, so the new chain is indexed from
// there and not from the old watermark.
func TestForceReindexStartsTheNewChainAtTheConfiguredBlock(t *testing.T) {
	base := new(MockDatabase)
	database := &prunedMockDB{MockDatabase: base, through: 5_000}
	node := new(MockRPCClient)
	base.On("GetLatestBlockNumber", mock.Anything).Return(uint64(10_000), nil)
	base.On("GetBlockCount", mock.Anything).Return(int64(5_000), nil)
	base.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
	base.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()
	base.On("WipeAllData", mock.Anything).Return(nil)
	base.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	node.On("BlockNumber", mock.Anything).Return(uint64(50), nil)
	node.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
	idx := NewWithConfig(database, node, time.Second, 0, &Config{RPCWorkers: 4, RPCRateLimit: 100, DBBatchSize: 100})
	t.Setenv("FORCE_REINDEX", "true")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, idx.Start(ctx), context.DeadlineExceeded)
	base.AssertCalled(t, "WipeAllData", mock.Anything)
	require.Equal(t, uint64(0), idx.startBlock, "the new chain starts at the configured block, not above the old watermark")
}
