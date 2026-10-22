package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

func historyCutChecks(t *testing.T, d *DB) int {
	t.Helper()
	var n int
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pg_constraint WHERE conname = 'history_cut' AND conrelid::regclass::text LIKE '%\_legacy'`).Scan(&n))
	return n
}

func insertCutTestBlock(d *DB, number uint64) error {
	return d.InsertBlockDataBatch(context.Background(), &BlockData{
		Block: &types.Block{Number: number, Hash: fmt.Sprintf("cut%d", number), Timestamp: 1_700_000_000 + number},
	})
}

// A cut abandoned after its checks were validated (the head came near it, a failure, a restart) leaves "below the
// cut" on every legacy partition while the history is still uncut. With retention then switched off nothing would
// ever drop it, and ingest stops at the cut: the next start drops it before ingest.
func TestAbandonedCutIsReleasedBeforeIngest(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	_, err := d.pool.Exec(ctx, `SELECT history_cut_constrain(44001)`)
	require.NoError(t, err)
	for _, tbl := range HistoryTables {
		_, err := d.pool.Exec(ctx, `ALTER TABLE `+tbl+`_legacy VALIDATE CONSTRAINT history_cut`)
		require.NoError(t, err)
	}
	require.Error(t, insertCutTestBlock(d, 44_001), "the hazard: ingest is refused at the abandoned cut")

	d.RunRetention(ctx, RetentionConfig{RetainBlocks: 0})
	require.Equal(t, len(HistoryTables), historyCutChecks(t, d), "retention off returns at once and keeps the check")

	require.NoError(t, d.Migrate(), "a restart: the reconcile runs before ingest")
	require.Zero(t, historyCutChecks(t, d))
	require.NoError(t, insertCutTestBlock(d, 44_001), "ingest continues past the old cut")
	cut, err := d.HistoryCut(ctx)
	require.NoError(t, err)
	require.False(t, cut)
}

// A cut abandoned mid-validation drops its checks on the way out: here the logs partition holds a row above the cut,
// so validating it fails after the earlier tables validated.
func TestAbandonedCutDropsItsChecks(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	_, err := d.pool.Exec(ctx, `
		INSERT INTO logs (tx_hash, log_index, address, topic0, data, block_number, timestamp)
		VALUES ('above', 0, '0xa', '0x0', '0x', 50000, 1700050000)`)
	require.NoError(t, err)

	err = d.cutHistory(ctx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000})
	require.ErrorContains(t, err, "validate logs_legacy", "the cut at 44,001 is abandoned at the logs partition")
	require.Zero(t, historyCutChecks(t, d), "the abandoned cut's checks are dropped")
	require.NoError(t, insertCutTestBlock(d, 44_001))
}

// A cut the head caught up with retries further out: at least twice the head's advance during the attempt, at
// least twice the last margin, so the margin outgrows the validation time.
func TestCutMarginGrowsAfterTheHeadCatchesUp(t *testing.T) {
	require.Equal(t, uint64(6_000), nextCutMargin(0, 3_000))
	require.Equal(t, uint64(12_000), nextCutMargin(6_000, 3_000), "doubles even when the head moved no faster")
	require.Equal(t, uint64(20_000), nextCutMargin(6_000, 10_000), "follows a faster head")

	d, cleanup := setupTestDB(t)
	defer cleanup()
	seedHistory(t, d, 0, 40_001, 1)
	d.cutMargin = 6_000
	require.NoError(t, d.cutHistory(context.Background(), RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}))
	require.Equal(t, []string{"legacy:50001:attached", "p50001:52001:attached"}, partitionRanges(t, d),
		"cut at head 40,001 + 2 partitions + 6,000")
}

// The ledger closer reads the history tables in long transactions; the cut's DDL needs ACCESS EXCLUSIVE on them.
// Retention waits for the span on the history-DDL gate, not in the tables' lock queue, so ingest is never held
// behind a queued DDL lock, the closer starts no new span meanwhile, and the cut finishes on its first attempt.
func TestCutWaitsForTheLedgerSpanWithoutStallingIngest(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)

	span, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = span.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, historyDDLGate)
	require.NoError(t, err)
	var n int64
	require.NoError(t, span.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&n), "a span read holds ACCESS SHARE until it ends")

	cut := make(chan error, 1)
	go func() { cut <- d.cutHistory(ctx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}) }()
	time.Sleep(500 * time.Millisecond)

	nextSpan := make(chan error, 1)
	go func() {
		nextSpan <- pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, historyDDLGate)
			return err
		})
	}()

	for b := uint64(40_002); b < 40_012; b++ {
		start := time.Now()
		require.NoError(t, insertCutTestBlock(d, b))
		require.Less(t, time.Since(start), 500*time.Millisecond, "ingest is not held behind the cut's DDL (block %d)", b)
		time.Sleep(300 * time.Millisecond)
	}
	select {
	case err := <-nextSpan:
		t.Fatalf("the closer started another span while the cut waited: %v", err)
	case err := <-cut:
		t.Fatalf("the cut ran while a span read the history tables: %v", err)
	default:
	}

	require.NoError(t, span.Rollback(ctx), "the span ends")
	select {
	case err := <-cut:
		require.NoError(t, err, "the cut finishes on its first attempt")
	case <-time.After(30 * time.Second):
		t.Fatal("the cut did not finish after the span ended")
	}
	require.NoError(t, <-nextSpan, "the closer resumes after the cut")
	cutDone, err := d.HistoryCut(ctx)
	require.NoError(t, err)
	require.True(t, cutDone)
}

// A cut started while ingest is behind (a restart) is placed above the node's head, keeping its full margin.
func TestCutIsPlacedAboveTheNodeHead(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	seedHistory(t, d, 0, 40_001, 1)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000,
		NodeHead: func(context.Context) (uint64, error) { return 45_001, nil }}
	require.NoError(t, d.cutHistory(context.Background(), cfg))
	require.Equal(t, []string{"legacy:49001:attached", "p49001:51001:attached"}, partitionRanges(t, d),
		"node head 45,001 + 2 partitions, not stored head 40,001 + 2")
}
