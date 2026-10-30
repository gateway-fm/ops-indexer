package indexer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
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

// The ledger, the checks and retention start once Start has passed its FORCE_REINDEX wipe: started at the startup
// reconcile, they would close spans, recount and prune the rows the wipe is truncating.
func TestTheWorkersAfterTheStartWaitOutItsWipe(t *testing.T) {
	base := new(MockDatabase)
	database := &prunedMockDB{MockDatabase: base}
	node := new(MockRPCClient)
	idx := NewWithConfig(database, node, time.Second, 0, &Config{RPCWorkers: 4, RPCRateLimit: 100, DBBatchSize: 100})
	pastDuringWipe := true
	base.On("GetLatestBlockNumber", mock.Anything).Return(uint64(10_000), nil)
	base.On("GetBlockCount", mock.Anything).Return(int64(10_000), nil)
	base.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
	base.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()
	base.On("WipeAllData", mock.Anything).Run(func(mock.Arguments) {
		select {
		case <-idx.PastWipe():
		default:
			pastDuringWipe = false
		}
	}).Return(nil)
	base.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	node.On("BlockNumber", mock.Anything).Return(uint64(50), nil)
	node.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
	t.Setenv("FORCE_REINDEX", "true")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, idx.Start(ctx), context.DeadlineExceeded)
	base.AssertCalled(t, "WipeAllData", mock.Anything)
	require.False(t, pastDuringWipe, "the workers were released while the wipe ran")
	select {
	case <-idx.PastWipe():
	default:
		t.Fatal("the workers were never released after the wipe")
	}
}

// rereadMockDB is a prunedMockDB with balance reread requests: it records whether any was claimed while the wipe ran.
type rereadMockDB struct {
	*prunedMockDB
	wiping, claimedDuringWipe atomic.Bool
}

func (m *rereadMockDB) WipeAllData(ctx context.Context) error {
	m.wiping.Store(true)
	defer m.wiping.Store(false)
	time.Sleep(100 * time.Millisecond)
	return m.prunedMockDB.WipeAllData(ctx)
}

func (m *rereadMockDB) ClaimBalanceRereads(context.Context, int) (claimed, dropped []db.BalanceReread, err error) {
	if m.wiping.Load() {
		m.claimedDuringWipe.Store(true)
	}
	return nil, nil, nil
}

func (m *rereadMockDB) CurrentBalanceEpoch(context.Context) (int64, error) { return 0, nil }

// The balance rereads start after the start's wipe: a request claimed before the truncate would write the old
// chain's balance after it, stamped with the old chain's head, and the new chain's reads of that key would be refused.
func TestTheBalanceRereadsStartAfterTheStartsWipe(t *testing.T) {
	interval := balanceRereadInterval
	balanceRereadInterval = 5 * time.Millisecond
	t.Cleanup(func() { balanceRereadInterval = interval })
	base := new(MockDatabase)
	database := &rereadMockDB{prunedMockDB: &prunedMockDB{MockDatabase: base}}
	node := new(MockRPCClient)
	base.On("GetLatestBlockNumber", mock.Anything).Return(uint64(10_000), nil)
	base.On("GetBlockCount", mock.Anything).Return(int64(10_000), nil)
	base.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
	base.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()
	base.On("WipeAllData", mock.Anything).Return(nil)
	base.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	node.On("BlockNumber", mock.Anything).Return(uint64(50), nil)
	node.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
	idx := NewWithConfig(database, node, time.Second, 0, &Config{RPCWorkers: 4, RPCRateLimit: 100, DBBatchSize: 100})
	t.Setenv("FORCE_REINDEX", "true")

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, idx.Start(ctx), context.DeadlineExceeded)
	base.AssertCalled(t, "WipeAllData", mock.Anything)
	require.False(t, database.claimedDuringWipe.Load(), "a reread request was claimed while the wipe ran")
}
