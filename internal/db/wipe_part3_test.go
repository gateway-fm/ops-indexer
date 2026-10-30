package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A FORCE_REINDEX wipe leaves nothing of the old chain's ledger checks, stats backfill or staging: none of it may be
// taken for the new chain's.
func TestAWipeForgetsTheLedgerChecksAndTheStatsBackfill(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InitStatsBackfill(ctx, 999, 1_700_000_999))
	_, err := d.pool.Exec(ctx, `
		INSERT INTO stats_backfill_balance_keys (token_address, address, block_number) VALUES ('0xt', '0xa', 5);
		INSERT INTO daily_stats_base (date, first_block) VALUES ('2023-11-14', 0);
		INSERT INTO daily_active_base (date, address) VALUES ('2023-11-14', '0xa');
		INSERT INTO ledger_spans (lo, hi, source) VALUES (0, 100, 'node');
		INSERT INTO ledger_stage_chain (lo, name, value) VALUES (100, 'blocks_total', 7);
		INSERT INTO ledger_stage_token (lo, token, transfers, supply) VALUES (100, '0xt', 1, 1);
		INSERT INTO ledger_stage_address (lo, address, txs, sent, internal, transfers, first_seen, last_seen)
		VALUES (100, '0xa', 1, 1, 0, 0, 100, 100);
		INSERT INTO ledger_reverted_address (address) VALUES ('0xa');
		UPDATE ledger_verify_state SET value = 9 WHERE name IN ('address_reconcile_next', 'address_days_recount', 'running_cycles');
		CREATE TABLE address_rebase_stage (address TEXT)`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `INSERT INTO ledger_repairs SELECT * FROM json_populate_record(NULL::ledger_repairs, '{}')`)
	if err != nil {
		t.Logf("ledger_repairs not seeded: %v", err)
	}

	require.NoError(t, d.WipeAllData(ctx))

	for _, table := range []string{"stats_backfill", "stats_backfill_balance_keys", "daily_stats_base", "daily_active_base",
		"ledger_stage_chain", "ledger_stage_token", "ledger_stage_address", "ledger_reverted_address", "ledger_repairs",
		"ledger_quarantine", "ledger_spans"} {
		var n int
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n))
		require.Zero(t, n, table)
	}
	var progress int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT COALESCE(sum(value), 0) FROM ledger_verify_state
		WHERE name IN ('address_reconcile_next', 'address_days_recount', 'running_cycles')`).Scan(&progress))
	require.Zero(t, progress, "the checks' progress starts over")
	var stage bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT to_regclass('address_rebase_stage') IS NOT NULL`).Scan(&stage))
	require.False(t, stage)
	state, err := d.StatsBackfillState(ctx)
	require.NoError(t, err)
	require.False(t, state.Exists, "no backfill of the old chain")
}
