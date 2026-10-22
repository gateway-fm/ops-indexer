package db

import (
	"context"
	"fmt"
	"regexp"
	"testing"

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

// The counts that scan whole history tables carry no filter on the block, so Postgres can scan them in parallel
// instead of an index-only scan with heap fetches; only the pruned-but-attached partitions are subtracted, whole.
func TestReseedCountsHistoryParentsWithoutAFilter(t *testing.T) {
	pruned := map[string][]string{
		"blocks": {"blocks_legacy"}, "transactions": {"transactions_legacy"}, "token_transfers": {"token_transfers_legacy"},
	}
	for name, sql := range map[string]string{
		"chain": reseedChainCountersSQL(pruned), "token": reseedTokenCountersSQL(pruned), "zero": zeroTokenCountersSQL,
	} {
		require.NotContains(t, sql, "history_from", name)
		require.NotContains(t, sql, "block_number", name)
		require.False(t, regexp.MustCompile(`FROM "?(blocks|transactions|token_transfers)(_legacy)?"? WHERE (number|block_number)`).MatchString(sql), name)
	}
	require.Contains(t, reseedChainCountersSQL(pruned), `(SELECT COUNT(*) FROM "transactions") - (SELECT COUNT(*) FROM "transactions_legacy")`)
}
