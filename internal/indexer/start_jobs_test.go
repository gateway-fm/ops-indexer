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

// slowRereadMockDB is a prunedMockDB whose reread claim takes a while to return, as a query does after its context
// is cancelled; it counts the claims in flight.
type slowRereadMockDB struct {
	*prunedMockDB
	inFlight atomic.Int32
	claimed  atomic.Bool
}

func (m *slowRereadMockDB) ClaimBalanceRereads(context.Context, int) (claimed, dropped []db.BalanceReread, err error) {
	m.inFlight.Add(1)
	defer m.inFlight.Add(-1)
	m.claimed.Store(true)
	time.Sleep(300 * time.Millisecond)
	return nil, nil, nil
}

func (m *slowRereadMockDB) CurrentBalanceEpoch(context.Context) (int64, error) { return 0, nil }

// Start returns only once the background jobs it started have stopped, so the caller can close the pool behind it.
func TestStartReturnsOnlyAfterTheJobsItStartedStop(t *testing.T) {
	interval := balanceRereadInterval
	balanceRereadInterval = 5 * time.Millisecond
	t.Cleanup(func() { balanceRereadInterval = interval })
	base := new(MockDatabase)
	database := &slowRereadMockDB{prunedMockDB: &prunedMockDB{MockDatabase: base}}
	node := new(MockRPCClient)
	base.On("GetLatestBlockNumber", mock.Anything).Return(uint64(0), nil)
	base.On("GetBlockCount", mock.Anything).Return(int64(0), nil)
	base.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()
	base.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()
	base.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	node.On("BlockNumber", mock.Anything).Return(uint64(50), nil)
	node.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
	idx := NewWithConfig(database, node, time.Second, 0, &Config{RPCWorkers: 4, RPCRateLimit: 100, DBBatchSize: 100})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		require.Eventually(t, database.claimed.Load, 5*time.Second, time.Millisecond)
		cancel()
	}()
	idx.Start(ctx)
	require.True(t, database.claimed.Load())
	require.Zero(t, database.inFlight.Load(), "a job was still running when Start returned")
}
