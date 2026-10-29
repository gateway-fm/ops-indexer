package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// history_from() inlines to a CASE over pruned_through(), which reads prune_state and cannot be inlined: as a
// sequential scan's filter it ran once per row (289 M transactions at a production deployment's startup re-seed). Every query
// evaluates it once. The test forces the sequential scans a large table gets.
func TestHistoryBoundIsEvaluatedOncePerQuery(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistoryAt(t, d, 0, 5_000, 1, 1700000000, 1)
	_, err := d.pool.Exec(ctx, `DELETE FROM daily_stats_watermark`)
	require.NoError(t, err)

	calls := func(run func(tx pgx.Tx) error) int64 {
		var n int64
		require.NoError(t, pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SET LOCAL track_functions = 'all'; SET LOCAL enable_indexscan = off;
				SET LOCAL enable_indexonlyscan = off; SET LOCAL enable_bitmapscan = off`); err != nil {
				return err
			}
			if err := run(tx); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `
				SELECT COALESCE(pg_stat_get_xact_function_calls('pruned_through()'::regprocedure), 0)`).Scan(&n)
		}))
		return n
	}

	require.Less(t, calls(func(tx pgx.Tx) error { return reseedCounters(ctx, tx) }), int64(50),
		"the startup re-seed over 5,001 blocks, transactions and transfers")
	require.Less(t, calls(func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `SELECT daily_stats_advance()`)
		return err
	}), int64(50), "daily_stats' first day counts the transactions before it")
	require.Less(t, calls(func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `SELECT count(*) FROM prune_offsets`)
		return err
	}), int64(50))
}
