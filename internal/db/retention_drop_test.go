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

// A reader that keeps a history table open holds up a detach of a range past the watermark only for the detach's
// wait: the pass gives up, leaves the detach pending with the pool's connections returned, and a later pass, once
// the reader is gone, finishes it.
func TestRangeDetachGivesUpOnALongReaderAndFinishesLater(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := legacyPastTheWatermark(t, d)
	cfg.DetachWait = time.Second

	reader, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = reader.Rollback(ctx) }()
	var n int64
	require.NoError(t, reader.QueryRow(ctx, `SELECT count(*) FROM logs`).Scan(&n))

	type result struct {
		finished bool
		err      error
	}
	pass := make(chan result, 1)
	pctx, stop := context.WithCancel(ctx)
	defer stop()
	start := time.Now()
	go func() {
		finished, err := d.finishDetached(pctx, cfg)
		pass <- result{finished, err}
	}()
	select {
	case r := <-pass:
		require.NoError(t, r.err, "a reader holding the range is not a failure")
		require.False(t, r.finished)
	case <-time.After(20 * time.Second):
		stop()
		t.Fatal("the detach waited on the reader past its wait")
	}
	require.Less(t, time.Since(start), 10*time.Second)
	require.Equal(t, int32(1), d.pool.Stat().AcquiredConns(), "only the reader's connection is still held")
	require.Contains(t, partitionRanges(t, d), "legacy:44001:detaching")

	require.NoError(t, reader.Rollback(ctx))
	finished, err := d.finishDetached(ctx, cfg)
	require.NoError(t, err)
	require.True(t, finished)
	for _, tbl := range HistoryTables {
		var exists bool
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl+"_legacy").Scan(&exists))
		require.False(t, exists, "%s_legacy dropped", tbl)
	}
}

// A view or a foreign key outside the indexer that depends on a range's tables (one created on a history table
// before it was partitioned stays on <table>_legacy) keeps that range on disk, whole and detached, until it is gone;
// the ranges after it are still pruned and dropped.
func TestRangeWithDependentObjectsIsKeptAndLaterRangesGo(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `
		CREATE VIEW consumer_txs AS SELECT hash, block_number FROM transactions_legacy;
		CREATE TABLE consumer_notes (tx TEXT REFERENCES transactions_legacy (hash), note TEXT)`)
	require.NoError(t, err)
	cfg := legacyPastTheWatermark(t, d)

	finished, err := d.finishDetached(ctx, cfg)
	require.NoError(t, err, "dependent objects are not a failure of the pass")
	require.True(t, finished, "a range kept for its dependents does not hold up the ranges after it")
	for _, tbl := range HistoryTables {
		var exists bool
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl+"_legacy").Scan(&exists))
		require.True(t, exists, "%s_legacy is kept whole", tbl)
	}
	require.Contains(t, partitionRanges(t, d), "legacy:44001:detached")

	seedHistory(t, d, 80_002, 100_001, 1)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(80_000), wm, "pruning goes on past the kept range")
	var exists bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass('transactions_p44001') IS NOT NULL`).Scan(&exists))
	require.False(t, exists, "a later range is dropped")

	_, err = d.pool.Exec(ctx, `DROP VIEW consumer_txs; DROP TABLE consumer_notes`)
	require.NoError(t, err)
	finished, err = d.finishDetached(ctx, cfg)
	require.NoError(t, err)
	require.True(t, finished)
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass('transactions_legacy') IS NOT NULL`).Scan(&exists))
	require.False(t, exists, "the range goes once nothing depends on it")
}
