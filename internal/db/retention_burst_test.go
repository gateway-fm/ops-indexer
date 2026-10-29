package db

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/stretchr/testify/require"
)

// On a chain that makes more blocks a day than the window, ranges become prunable once a day, when the floor block
// crosses midnight: one pass then detaches and drops a day of them. Ingest keeps committing through it, with the
// ledger closer running as in production.
func TestIngestCommitsThroughTheDailyPruneStep(t *testing.T) {
	const midnight = 1700092800
	const firstOfDay = 100_000
	epoch0 := int64(midnight - firstOfDay)
	cfg := RetentionConfig{RetainBlocks: 12_800, PartitionBlocks: 1_000}
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	seedHistoryAt(t, d, 0, 40_001, 1, epoch0, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	for made := 1; made > 0; {
		n, err := d.EnsureHistoryPartitions(ctx, 140_000, cfg.PartitionBlocks)
		require.NoError(t, err)
		made = n
	}
	head := int64(firstOfDay + PruneFloorGap - 1)
	seedHistoryAt(t, d, 40_002, head, 1, epoch0, 1)
	_, err := d.pool.Exec(ctx, `DELETE FROM daily_stats_watermark`)
	require.NoError(t, err)
	advanceDailyStats(t, d, head)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	require.Zero(t, prunedThrough(t, d), "the floor block 99,999 is on the day before: nothing is prunable yet")

	write := func(n int64) error {
		to := "0xb"
		return d.InsertBlockDataBatch(ctx, &BlockData{
			Block: &types.Block{Number: uint64(n), Hash: fmt.Sprintf("blk%d", n), ParentHash: fmt.Sprintf("blk%d", n-1),
				Timestamp: uint64(epoch0 + n), TransactionCount: 1},
			Transactions: []*types.Transaction{{Hash: fmt.Sprintf("tx%d", n), BlockNumber: uint64(n), From: "0xa", To: &to,
				Value: "0", InputData: "0x", Status: 1}},
		})
	}
	head++
	require.NoError(t, write(head), "the floor block is now the day's first: a day of ranges is prunable")

	var attachedBefore int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM history_partitions WHERE state = 'attached'`).Scan(&attachedBefore))

	passCtx, stopCloser := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for passCtx.Err() == nil {
			_, _ = d.CloseLedgerSpans(passCtx, testLedger, 1_000_000)
			time.Sleep(200 * time.Millisecond)
		}
	}()
	pass := make(chan error, 1)
	start := time.Now()
	go func() { pass <- d.retentionPass(ctx, cfg) }()

	var latencies []time.Duration
	var longestGap time.Duration
	last := time.Now()
	var passErr error
	for done := false; !done; {
		select {
		case passErr = <-pass:
			done = true
		default:
		}
		head++
		t0 := time.Now()
		require.NoError(t, write(head))
		now := time.Now()
		latencies = append(latencies, now.Sub(t0))
		longestGap = max(longestGap, now.Sub(last))
		last = now
		require.Less(t, time.Since(start), 5*time.Minute, "the prune step did not finish")
	}
	passTook := time.Since(start)
	stopCloser()
	wg.Wait()
	require.NoError(t, passErr)

	var attachedAfter int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM history_partitions WHERE state = 'attached'`).Scan(&attachedAfter))
	ranges := attachedBefore - attachedAfter
	slices.Sort(latencies)
	pct := func(p float64) time.Duration { return latencies[int(p*float64(len(latencies)-1))] }
	t.Logf("prune step: %d ranges pruned through %d in %s; ingest: %d commits, p50 %s, p99 %s, max %s, longest gap between commits %s",
		ranges, prunedThrough(t, d), passTook.Round(time.Millisecond), len(latencies),
		pct(0.5), pct(0.99), latencies[len(latencies)-1], longestGap)

	require.Equal(t, uint64(firstOfDay-1_000), prunedThrough(t, d), "every range before the floor block's day; [99001, 100001) ends on it")
	require.GreaterOrEqual(t, ranges, int64(50))
	require.Less(t, latencies[len(latencies)-1], time.Second, "a commit waited behind the prune step")
	require.Less(t, longestGap, time.Second)
}
