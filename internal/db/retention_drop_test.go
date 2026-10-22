package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// legacyPastTheWatermark leaves the legacy range past the watermark ("detaching"), still attached, the head at 80,001.
func legacyPastTheWatermark(t *testing.T, d *DB) RetentionConfig {
	t.Helper()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 100_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	_, err := d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
	require.NoError(t, err)
	return cfg
}

// openSpan holds the gate as the closer does for a span, and reads every history partition.
func openSpan(t *testing.T, d *DB) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	span, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = span.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, historyDDLGate)
	require.NoError(t, err)
	for _, tbl := range HistoryTables {
		var n int64
		require.NoError(t, span.QueryRow(ctx, `SELECT count(*) FROM `+tbl).Scan(&n))
	}
	return span
}

// legacyBlocksPartition reports whether blocks_legacy is attached, and whether a detach of it is pending.
func legacyBlocksPartition(t *testing.T, d *DB) (attached, pending bool) {
	t.Helper()
	err := d.pool.QueryRow(context.Background(), `
		SELECT true, inhdetachpending FROM pg_inherits WHERE inhrelid = to_regclass('blocks_legacy')`).Scan(&attached, &pending)
	if err == pgx.ErrNoRows {
		return false, false
	}
	require.NoError(t, err)
	return attached, pending
}

// A range past the watermark is detached and dropped only between ledger spans: retention waits on the gate, not
// inside DETACH CONCURRENTLY, so the closer starts no span meanwhile and ingest goes on.
func TestRangeDropWaitsForTheLedgerSpanWithoutStallingIngest(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := legacyPastTheWatermark(t, d)
	cfg.DropGateWait = 30 * time.Second

	span := openSpan(t, d)
	type result struct {
		finished bool
		err      error
	}
	drop := make(chan result, 1)
	go func() {
		finished, err := d.finishDetached(ctx, cfg)
		drop <- result{finished, err}
	}()
	time.Sleep(500 * time.Millisecond)
	nextSpan := make(chan error, 1)
	go func() {
		nextSpan <- pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, historyDDLGate)
			return err
		})
	}()

	for b := uint64(80_002); b < 80_012; b++ {
		start := time.Now()
		require.NoError(t, insertCutTestBlock(d, b))
		require.Less(t, time.Since(start), 500*time.Millisecond, "ingest goes on (block %d)", b)
		_, err := d.GetLatestBlockNumber(ctx)
		require.NoError(t, err)
		require.Less(t, time.Since(start), 500*time.Millisecond, "ingest's head read goes on (block %d)", b)
		time.Sleep(200 * time.Millisecond)
	}
	select {
	case r := <-drop:
		t.Fatalf("the range was finished while a span read the history tables: %+v", r)
	case err := <-nextSpan:
		t.Fatalf("the closer started another span while the drop waited: %v", err)
	default:
	}
	attached, pending := legacyBlocksPartition(t, d)
	require.True(t, attached)
	require.False(t, pending, "no detach is in progress while the drop waits for the span")

	require.NoError(t, span.Rollback(ctx))
	select {
	case r := <-drop:
		require.NoError(t, r.err)
		require.True(t, r.finished)
	case <-time.After(30 * time.Second):
		t.Fatal("the drop did not finish after the span ended")
	}
	require.NoError(t, <-nextSpan, "the closer resumes after the drop")
	attached, _ = legacyBlocksPartition(t, d)
	require.False(t, attached)
	require.NotContains(t, partitionRanges(t, d), "legacy:44001:detaching")
}

// A drop that cannot get the gate within its wait leaves the range whole, attached and past the watermark, and the
// next pass finishes it.
func TestRangeDropSkipsThePassWhenTheGateStaysBusy(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := legacyPastTheWatermark(t, d)
	cfg.DropGateWait = 300 * time.Millisecond

	span := openSpan(t, d)
	start := time.Now()
	finished, err := d.finishDetached(ctx, cfg)
	require.NoError(t, err, "a busy gate is not a failure")
	require.False(t, finished)
	require.Less(t, time.Since(start), 5*time.Second, "the wait is bounded")
	attached, pending := legacyBlocksPartition(t, d)
	require.True(t, attached)
	require.False(t, pending, "no half-detached partition")
	require.Contains(t, partitionRanges(t, d), "legacy:44001:detaching")
	require.NoError(t, span.Rollback(ctx))

	finished, err = d.finishDetached(ctx, cfg)
	require.NoError(t, err)
	require.True(t, finished)
	attached, _ = legacyBlocksPartition(t, d)
	require.False(t, attached)
}
