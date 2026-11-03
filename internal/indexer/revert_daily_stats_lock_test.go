package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
)

// A reorg revert that waits for the daily-stats lock (4670: daily_stats_advance, a day recount, recountPendingDays hold
// it for as long as they run) must not stop the balance work of blocks other writers commit meanwhile. Catch-up and
// the standard loop queue every committed block's balance work (QueueCommittedWork), so work that cannot be queued
// holds ingest.
func TestBalanceWorkIsQueuedWhileARevertWaitsForTheDailyStatsLock(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := sql.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		VALUES (9, '0xb9', '0xb8', 1700000540, 0, 1, 0), (10, '0xb10', '0xb9', 1700000600, 0, 1, 0)`)
	require.NoError(t, err)

	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 10}
	idx := NewWithConfig(store, chain, time.Second, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
	idx.balanceWorkers = NewBalanceWorkerPool(store, chain, 1, 1000)
	require.NoError(t, loadBalanceEpoch(ctx, store, idx.balanceWorkers))

	daily, err := sql.Begin(ctx)
	require.NoError(t, err)
	defer daily.Rollback(context.Background())
	_, err = daily.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`)
	require.NoError(t, err, "the daily-stats advance holds its lock")

	reverted := make(chan error, 1)
	go func() { reverted <- idx.handleReorg(ctx, 10) }()
	require.Eventually(t, func() bool {
		var waiting bool
		_ = sql.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
			AND classid = 4670 AND objid = 1)`).Scan(&waiting)
		return waiting
	}, 20*time.Second, 20*time.Millisecond, "the revert waits for the daily-stats lock")

	queued := make(chan bool, 1)
	go func() {
		queued <- idx.balanceWorkers.QueueWork(BalanceWork{
			Address:      common.HexToAddress("0x00000000000000000000000000000000000000a1"),
			TokenAddress: common.HexToAddress("0x00000000000000000000000000000000000070c1"),
			BlockNumber:  5,
		})
	}()
	select {
	case ok := <-queued:
		require.True(t, ok)
	case <-time.After(3 * time.Second):
		_ = daily.Rollback(context.Background())
		<-queued
		require.NoError(t, <-reverted)
		t.Fatal("a committed block's balance work waited for a revert that waited for the daily-stats lock")
	}
	require.NoError(t, daily.Rollback(ctx))
	require.NoError(t, <-reverted)
}
