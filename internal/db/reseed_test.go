package db

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func wantTotals(blocks int64) retentionTotals {
	return retentionTotals{blocks: blocks, txs: blocks, transfers: blocks, tokenTransfers: blocks, supply: fmt.Sprint(7 * blocks)}
}

// requireTotalsAcrossRestarts re-seeds twice (two restarts) and checks the totals each time.
func requireTotalsAcrossRestarts(t *testing.T, d *DB, want retentionTotals, state string) {
	t.Helper()
	for restart := 1; restart <= 2; restart++ {
		require.Equal(t, want, totalsAfterReseed(t, d), "%s, restart %d", state, restart)
	}
}

func prunedButAttachedNow(t *testing.T, d *DB) map[string][]string {
	t.Helper()
	pruned, err := prunedButAttached(context.Background(), d.pool)
	require.NoError(t, err)
	return pruned
}

// The startup re-seed counts each history parent whole and subtracts the partitions a prune has moved past but not
// detached yet: exact in every state a restart can find, the legacy partition's and a regular one's.
func TestReseedTotalsAreExactInEveryPruneState(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}

	seedHistory(t, d, 0, 40_001, 1)
	requireTotalsAcrossRestarts(t, d, wantTotals(40_002), "(a) nothing pruned")
	require.Empty(t, prunedButAttachedNow(t, d))

	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 100_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	_, err := d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
	require.NoError(t, err)
	require.Equal(t, []string{"blocks_legacy"}, prunedButAttachedNow(t, d)["blocks"])
	requireTotalsAcrossRestarts(t, d, wantTotals(80_002), "(d) the legacy partition below the watermark, still attached")

	require.NoError(t, d.retentionPass(ctx, cfg))
	wm, _, err := d.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(60_000), wm)
	require.Empty(t, prunedButAttachedNow(t, d))
	requireTotalsAcrossRestarts(t, d, wantTotals(80_002), "(b) after prunes")

	seedHistory(t, d, 80_002, 100_001, 1)
	closeLedger(t, d)
	_, err = d.pool.Exec(ctx, `SELECT prune_advance('p60001')`)
	require.NoError(t, err)
	require.Equal(t, []string{"transactions_p60001"}, prunedButAttachedNow(t, d)["transactions"])
	requireTotalsAcrossRestarts(t, d, wantTotals(100_002), "(c) a partition below the watermark, still attached")

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
	_, err = conn.Exec(ctx, `ALTER TABLE transactions DETACH PARTITION transactions_p60001 CONCURRENTLY`)
	require.Error(t, err, "the detach waits for the open snapshot and is cancelled, pending")
	_, err = conn.Exec(ctx, `RESET statement_timeout`)
	require.NoError(t, err)
	conn.Release()
	require.NoError(t, holder.Rollback(ctx))
	require.Empty(t, prunedButAttachedNow(t, d)["transactions"], "a detach-pending partition is not read through its parent")
	requireTotalsAcrossRestarts(t, d, wantTotals(100_002), "(c) with the partition's detach pending")
}

// The counts that scan whole history tables carry no filter on the block and call no function, and every one of them
// is a read-only SELECT: forced to consider parallel plans, Postgres gives each a Gather. Inside a statement that
// writes (an earlier INSERT ... VALUES ((SELECT COUNT(*) ...))) it never does: that is what ran serially in production.
func TestReseedReadsArePlannedInParallel(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 2_000, 1)

	var reads []string
	pruned := map[string][]string{"blocks": {"blocks_legacy"}, "transactions": {"transactions_legacy"}, "token_transfers": {"token_transfers_legacy"}}
	for _, table := range []string{"blocks", "transactions", "token_transfers"} {
		whole, subtract := retainedCountSQLs(table, pruned[table])
		reads = append(reads, whole)
		reads = append(reads, subtract...)
	}
	reads = append(reads, retainedTokenTotalsSQL(pruned["token_transfers"]))
	for _, sql := range reads {
		require.NotContains(t, sql, "history_from")
		require.NotContains(t, sql, "block_number")
		require.NotContains(t, sql, "WHERE")
	}

	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	for _, set := range []string{"parallel_setup_cost = 0", "parallel_tuple_cost = 0", "min_parallel_table_scan_size = 0"} {
		_, err := conn.Exec(ctx, "SET "+set)
		require.NoError(t, err)
	}
	plan := func(sql string) string {
		rows, err := conn.Query(ctx, "EXPLAIN "+sql)
		require.NoError(t, err)
		defer rows.Close()
		var out string
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			out += line + "\n"
		}
		return out
	}
	for _, sql := range reads {
		require.Contains(t, plan(sql), "Gather", "a parallel plan for: %s", sql)
	}

	rec := &recordingExecer{execer: conn}
	require.NoError(t, reseedCounters(ctx, rec))
	history := regexp.MustCompile(`FROM "?(blocks|transactions|token_transfers)(_[a-z0-9]+)?"?\b`)
	for _, sql := range rec.statements {
		if !history.MatchString(sql) || sql == zeroTokenCountersSQL {
			continue
		}
		require.Regexp(t, `^\s*SELECT`, sql, "every re-seed statement that reads history is a read-only SELECT")
		require.Contains(t, plan(sql), "Gather", "a parallel plan for: %s", sql)
	}
	require.NotContains(t, plan(`INSERT INTO chain_counters (name, count) VALUES ('transactions_total', (SELECT COUNT(*) FROM transactions))
		ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count`), "Gather", "the premise: a writing statement gets no parallel plan")
}

// recordingExecer records every statement the re-seed runs.
type recordingExecer struct {
	execer
	statements []string
}

func (r *recordingExecer) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.statements = append(r.statements, sql)
	return r.execer.Exec(ctx, sql, args...)
}

func (r *recordingExecer) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.statements = append(r.statements, sql)
	return r.execer.Query(ctx, sql, args...)
}

func (r *recordingExecer) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	r.statements = append(r.statements, sql)
	return r.execer.QueryRow(ctx, sql, args...)
}
