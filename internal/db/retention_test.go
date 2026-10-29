package db

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const retentionToken = "0xret0000000000000000000000000000000000001"

// seedHistory writes one block every `step` numbers in [from, to], each with one transaction, one log, one
// internal call, one mint of 7 units of retentionToken and one balances row, the way the indexer stores them.
// Block b is at seedTimestamp(b).
func seedHistory(t *testing.T, d *DB, from, to, step int64) {
	t.Helper()
	seedHistoryAt(t, d, from, to, step, seedEpoch, seedBlockSeconds)
}

// Seeded blocks are 1,000 s apart: a UTC day holds 87 of them, so retention's whole-day rule keeps less than a range
// beyond what the block window keeps.
const (
	seedEpoch        = 1700000000
	seedBlockSeconds = 1000
)

func seedTimestamp(number uint64) uint64 { return seedEpoch + seedBlockSeconds*number }

// seedHistoryAt is seedHistory with block b at epoch0 + b*blockSeconds.
func seedHistoryAt(t *testing.T, d *DB, from, to, step, epoch0, blockSeconds int64) {
	t.Helper()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `
		INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number)
		VALUES ($1, 'RET', 'Retention', 18, 'ERC20', 0) ON CONFLICT (address) DO NOTHING`, retentionToken)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		WITH n AS (SELECT g AS b FROM generate_series($1::bigint, $2::bigint, $3::bigint) g),
		blk AS (
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
			SELECT b, 'blk' || b, 'blk' || (b - 1), $5 + b * $6, 1, 1, 1 FROM n RETURNING number
		),
		txs AS (
			INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
			SELECT 'tx' || b, b, 0, '0xa', '0xb', '0', 0, 0, '0x', 1, 0 FROM n RETURNING hash
		),
		lg AS (
			INSERT INTO logs (tx_hash, log_index, address, topic0, data, block_number, timestamp)
			SELECT 'tx' || b, 0, $4, '0xddf2', '0x', b, $5 + b * $6 FROM n RETURNING id
		),
		itx AS (
			INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value, call_type)
			SELECT 'tx' || b, b, '0', '0xa', '0xb', '0', 'call' FROM n RETURNING id
		),
		tt AS (
			INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp)
			SELECT 'tx' || b, 0, $4, '0x0000000000000000000000000000000000000000', '0xb', 7, b, $5 + b * $6 FROM n RETURNING id
		)
		INSERT INTO balances (address, token_address, block_number, balance)
		SELECT '0xb', $4, b, 7 * b FROM n`, from, to, step, retentionToken, epoch0, blockSeconds)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET last_block = EXCLUDED.last_block`, to)
	require.NoError(t, err)
}

type retentionTotals struct {
	blocks, txs, transfers, tokenTransfers int64
	supply                                 string
}

// totalsAfterReseed runs the startup re-seed (Migrate) and reads what it stored.
func totalsAfterReseed(t *testing.T, d *DB) retentionTotals {
	t.Helper()
	require.NoError(t, d.Migrate())
	var r retentionTotals
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT (SELECT count FROM chain_counters WHERE name = 'blocks_total'),
		       (SELECT count FROM chain_counters WHERE name = 'transactions_total'),
		       (SELECT count FROM chain_counters WHERE name = 'transfers_total'),
		       transfer_count, total_supply::TEXT
		FROM tokens WHERE address = $1`, retentionToken).Scan(&r.blocks, &r.txs, &r.transfers, &r.tokenTransfers, &r.supply))
	return r
}

func partitionRanges(t *testing.T, d *DB) []string {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `
		SELECT suffix || ':' || COALESCE(hi::TEXT, 'max') || ':' || state FROM history_partitions ORDER BY lo NULLS FIRST`)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

func TestRetentionOffLeavesOneUnboundedPartition(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	seedHistory(t, d, 1, 5000, 500)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.RunRetention(ctx, RetentionConfig{RetainBlocks: 0, Interval: time.Millisecond})
	require.NoError(t, ctx.Err(), "RunRetention with no window on an uncut database returns at once")

	require.Equal(t, []string{"legacy:max:attached"}, partitionRanges(t, d))
	made, err := d.EnsureHistoryPartitions(context.Background(), 1_000_000, 1000)
	require.NoError(t, err)
	require.Zero(t, made, "no partitions are maintained before the cut")
}

func TestRetentionCutsPrunesAndKeepsTotals(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	archive := t.TempDir()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000, ArchiveDir: archive}

	seedHistory(t, d, 0, 40_001, 1)
	before := totalsAfterReseed(t, d)

	require.NoError(t, d.retentionPass(ctx, cfg))
	require.Equal(t, []string{"legacy:44001:attached", "p44001:46001:attached"},
		partitionRanges(t, d), "cut at head + 2 partitions; a partition covers head + 2 partitions")

	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))

	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	// head 80,001 − retention 20,000 = 60,001: the legacy range and the ranges up to 60,000 go.
	require.Equal(t, uint64(60_000), wm)
	var below int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM blocks WHERE number <= $1`, wm).Scan(&below))
	require.Zero(t, below)
	for _, tbl := range HistoryTables {
		var exists bool
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl+"_legacy").Scan(&exists))
		require.False(t, exists, "%s_legacy dropped", tbl)
	}

	more := int64(40_000) // blocks 40,002..80,001
	after := totalsAfterReseed(t, d)
	require.Equal(t, before.blocks+more, after.blocks, "blocks_total counts pruned blocks")
	require.Equal(t, before.txs+more, after.txs)
	require.Equal(t, before.transfers+more, after.transfers)
	require.Equal(t, before.tokenTransfers+more, after.tokenTransfers, "transfer_count counts pruned transfers")
	require.Equal(t, fmt.Sprint(7*(before.tokenTransfers+more)), after.supply, "supply counts pruned mints")

	var archived, rows int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(sum(row_count), 0) FROM history_archives WHERE suffix = 'legacy' AND tbl = 'transactions'`).Scan(&archived, &rows))
	require.Equal(t, int64(1), archived)
	require.Equal(t, rows, countGzipLines(t, filepath.Join(archive, "legacy", "transactions.copy.gz")))
	require.Equal(t, int64(len(HistoryTables)), countArchives(t, d, "p58001"))
}

func TestRetentionWaitsForDailyStats(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}

	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	_, err := d.pool.Exec(ctx, `UPDATE daily_stats_watermark SET last_block = 10000`)
	require.NoError(t, err)
	closeLedger(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))

	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Zero(t, wm, "nothing is pruned before daily_stats has aggregated it")
}

func TestReorgDeletesAcrossPartitions(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}))
	seedHistory(t, d, 44_001, 44_001, 1)

	require.NoError(t, d.DeleteBlock(ctx, 44_001))
	for _, q := range []string{
		`SELECT count(*) FROM blocks WHERE number = 44001`,
		`SELECT count(*) FROM transactions WHERE block_number = 44001`,
		`SELECT count(*) FROM internal_transactions WHERE block_number = 44001`,
		`SELECT count(*) FROM token_transfers WHERE block_number = 44001`,
	} {
		var n int64
		require.NoError(t, d.pool.QueryRow(ctx, q).Scan(&n))
		require.Zero(t, n, q)
	}
}

var testLedger = LedgerConfig{SpanBlocks: 1000}

// closeLedger closes every span that is final and complete, as the ledger closer would over time.
func closeLedger(t *testing.T, d *DB) {
	t.Helper()
	_, err := d.CloseLedgerSpans(context.Background(), testLedger, 1_000_000)
	require.NoError(t, err)
}

// ensurePartitions creates what the retention pass keeps ahead of a head that ingest is about to reach.
func ensurePartitions(t *testing.T, d *DB, head uint64, cfg RetentionConfig) {
	t.Helper()
	_, err := d.EnsureHistoryPartitions(context.Background(), head, cfg.PartitionBlocks)
	require.NoError(t, err)
}

func countArchives(t *testing.T, d *DB, suffix string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, d.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM history_archives WHERE suffix = $1`, suffix).Scan(&n))
	return n
}

func countGzipLines(t *testing.T, path string) int64 {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	var n int64
	s := bufio.NewScanner(gz)
	s.Buffer(make([]byte, 1<<20), 1<<24)
	for s.Scan() {
		n++
	}
	require.NoError(t, s.Err())
	return n
}

// A crash after the watermark moved and before the detach leaves the range 'detaching' with its partitions still
// attached: the next pass finishes it, and the re-seed (rows from the first retained block + offsets) keeps the
// totals meanwhile.
func TestRetentionResumesAfterCrashBeforeDetach(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}

	seedHistory(t, d, 0, 40_001, 1)
	before := totalsAfterReseed(t, d)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)

	closeLedger(t, d)
	_, err := d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
	require.NoError(t, err)
	require.Equal(t, before.blocks+40_000, totalsAfterReseed(t, d).blocks, "attached rows below the watermark are not counted twice")

	require.NoError(t, d.retentionPass(ctx, cfg))
	var exists bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass('blocks_legacy') IS NOT NULL`).Scan(&exists))
	require.False(t, exists)
	require.Equal(t, before.blocks+40_000, totalsAfterReseed(t, d).blocks)
}

// A DETACH CONCURRENTLY cancelled while it waits for older snapshots leaves the partition "detach pending"; the
// next pass finalizes it instead of failing on it.
func TestRetentionFinalizesPendingDetach(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}

	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	_, err := d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
	require.NoError(t, err)

	holder, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = holder.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`)
	require.NoError(t, err)
	var n int64
	require.NoError(t, holder.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&n))
	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SET statement_timeout = '1s'`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `ALTER TABLE transactions DETACH PARTITION transactions_legacy CONCURRENTLY`)
	require.Error(t, err, "the detach waits for the open snapshot and is cancelled")
	_, err = conn.Exec(ctx, `RESET statement_timeout`)
	require.NoError(t, err)
	conn.Release()
	require.NoError(t, holder.Rollback(ctx))
	var pending bool
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT inhdetachpending FROM pg_inherits WHERE inhrelid = 'transactions_legacy'::regclass`).Scan(&pending))
	require.True(t, pending)

	require.NoError(t, d.retentionPass(ctx, cfg))
	var exists bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass('transactions_legacy') IS NOT NULL`).Scan(&exists))
	require.False(t, exists, "finalized, then dropped")
}

// A detached range is dropped only once every table's archive is recorded: an archive that fails leaves the
// detached tables in place, and the next pass with a working archive finishes the job.
func TestRetentionDropsOnlyArchivedRanges(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	blocked := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocked, nil, 0o644))
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000, ArchiveDir: blocked}

	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 80_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	require.Error(t, d.retentionPass(ctx, cfg), "the archive directory is a file")

	var state string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT state FROM history_partitions WHERE suffix = 'legacy'`).Scan(&state))
	require.Equal(t, "detached", state)
	var exists bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass('blocks_legacy') IS NOT NULL`).Scan(&exists))
	require.True(t, exists, "not dropped without its archive")

	cfg.ArchiveDir = t.TempDir()
	require.NoError(t, d.retentionPass(ctx, cfg))
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass('blocks_legacy') IS NOT NULL`).Scan(&exists))
	require.False(t, exists)
	require.Equal(t, int64(len(HistoryTables)), countArchives(t, d, "legacy"))
}
