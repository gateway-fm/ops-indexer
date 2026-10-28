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

	require.Zero(t, historyCutChecks(t, d), "no check is in force while the cut waits")
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
	require.Equal(t, "legacy:44011:attached", partitionRanges(t, d)[0],
		"the cut is placed from the head after the wait (40,011 + 2 partitions), with no check in force while it waited")
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

// Retention switched off after the cut, with PARTITION_BLOCKS unset (it defaults to PRUNE_RETAIN_BLOCKS / 10 = 0):
// partitions must still be kept ahead of the head, at the newest partition's size, or ingest stops at the last one.
func TestRetentionOffAfterTheCutKeepsPartitionsAhead(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}))
	require.Equal(t, []string{"legacy:44001:attached", "p44001:46001:attached"}, partitionRanges(t, d))

	off := RetentionConfig{RetainBlocks: 0, PartitionBlocks: 0}
	for from := int64(40_002); from <= 60_000; from += 1_000 {
		require.NoError(t, d.retentionPass(ctx, off))
		seedHistory(t, d, from, from+999, 1)
	}
	var head int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT max(number) FROM blocks`).Scan(&head))
	require.Equal(t, int64(60_001), head, "ingest went on past the original partitions")
	ranges := partitionRanges(t, d)
	require.Equal(t, "p62001:64001:attached", ranges[len(ranges)-1], "still two 2,000-block partitions ahead")
}

// Partitions are kept ahead on their own short ticker, not only in the retention pass: at the catch-up rate the
// head crosses 2 partitions in minutes, far inside a PRUNE_INTERVAL of 10 min, and every insert above them fails.
func TestPartitionsAreKeptAheadBetweenRetentionPasses(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seedHistory(t, d, 0, 40_001, 1)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000, Interval: time.Hour, EnsureInterval: 100 * time.Millisecond}
	require.NoError(t, d.retentionPass(ctx, cfg))
	go d.RunRetention(ctx, cfg)

	for from := int64(40_002); from <= 60_000; from += 1_000 {
		require.Eventually(t, func() bool {
			ranges := partitionRanges(t, d)
			var top int64
			_, err := fmt.Sscanf(ranges[len(ranges)-1], "p%d:", &top)
			return err == nil && top+2_000 >= from+2*2_000
		}, 5*time.Second, 50*time.Millisecond, "two partitions ahead of %d", from)
		seedHistory(t, d, from, from+999, 1)
	}
}

// holdValidateThenRead starts the cut, holds the VALIDATE of balances_legacy until a reader has opened logs, and
// returns the cut's result channel and the reader, whose transaction stays open.
func holdValidateThenRead(t *testing.T, d *DB, cfg RetentionConfig) (<-chan error, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	connString := d.pool.Config().ConnConfig.ConnString()
	blocker, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	t.Cleanup(func() { blocker.Close(context.Background()) })
	reader, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close(context.Background()) })

	cut := make(chan error, 1)
	cctx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	go func() { cut <- d.cutHistory(cctx, cfg) }()
	deadline := time.Now().Add(30 * time.Second)
	for historyCutChecks(t, d) < len(HistoryTables) {
		require.True(t, time.Now().Before(deadline), "the cut never added its checks")
		time.Sleep(2 * time.Millisecond)
	}
	_, err = blocker.Exec(ctx, `BEGIN; LOCK TABLE balances_legacy IN SHARE UPDATE EXCLUSIVE MODE`)
	require.NoError(t, err)
	for validating := false; !validating; time.Sleep(10 * time.Millisecond) {
		require.True(t, time.Now().Before(deadline), "the VALIDATE of balances_legacy never waited")
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE query LIKE 'ALTER TABLE "balances_legacy" VALIDATE%' AND wait_event_type = 'Lock')`).Scan(&validating))
	}
	_, err = reader.Exec(ctx, `BEGIN; SELECT count(*) FROM logs`)
	require.NoError(t, err)
	time.Sleep(1500 * time.Millisecond)
	_, err = blocker.Exec(ctx, `COMMIT`)
	require.NoError(t, err)
	return cut, reader
}

// A consumer's query that starts while the cut validates and outlasts the validation delays the cut's finish for as
// long as the head stays more than a partition below the cut; the cut finishes once the query ends.
func TestCutWaitsOutALongReaderWhileTheHeadIsFarBelowIt(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	cut, reader := holdValidateThenRead(t, d, cfg)

	select {
	case err := <-cut:
		t.Fatalf("the cut gave up while the head was far below it: %v", err)
	case <-time.After(45 * time.Second):
	}
	_, err := reader.Exec(ctx, `COMMIT`)
	require.NoError(t, err)
	select {
	case err := <-cut:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the cut did not finish after the reader ended")
	}
	done, err := d.HistoryCut(ctx)
	require.NoError(t, err)
	require.True(t, done)
	require.NoError(t, insertCutTestBlock(d, 44_001), "the first partition above the cut takes the block")
}

// A cut the head comes within a partition of while a reader holds it off is abandoned, and its check goes as soon as
// the reader ends, without a restart; a restart meanwhile does not fail over the check.
func TestAbandonedCutsCheckGoesWhenTheReaderEnds(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	const cutAt = 40_001 + 2*2_000
	cut, reader := holdValidateThenRead(t, d, cfg)

	require.NoError(t, insertCutTestBlock(d, cutAt-2_000))
	select {
	case err := <-cut:
		require.Error(t, err, "the head came within a partition of the cut")
	case <-time.After(120 * time.Second):
		t.Fatal("the cut did not give up once the head came within a partition of it")
	}
	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	require.NoError(t, d.Reconcile(rctx), "a restart while the reader holds the check goes on")

	release := make(chan struct{})
	go func() {
		defer close(release)
		d.RunRetention(ctx, RetentionConfig{RetainBlocks: 0, EnsureInterval: 100 * time.Millisecond})
	}()
	time.Sleep(2 * time.Second)
	require.Equal(t, len(HistoryTables), historyCutChecks(t, d), "the reader still holds the check")
	_, err := reader.Exec(ctx, `COMMIT`)
	require.NoError(t, err)
	select {
	case <-release:
	case <-time.After(30 * time.Second):
		t.Fatal("the check was not dropped after the reader ended")
	}
	require.Zero(t, historyCutChecks(t, d))
	require.NoError(t, insertCutTestBlock(d, cutAt), "ingest goes past the abandoned cut")
}

// With retention switched off after a cut was abandoned, retention still drops the cut's check at its start.
func TestRetentionOffDropsAnAbandonedCutsCheck(t *testing.T) {
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
	d.RunRetention(ctx, RetentionConfig{RetainBlocks: 0})
	require.Zero(t, historyCutChecks(t, d))
	require.NoError(t, insertCutTestBlock(d, 44_001))
}
