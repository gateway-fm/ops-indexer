package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An autovacuum of a history table is not a reader to wait for. The cut's lock request cancels it, as it
// did before the long-reader check, instead of skipping every attempt for as long as the autovacuum runs.
func TestCutCancelsAnAutovacuumOfAHistoryTable(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 4_001, 1)

	var leaf string
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = 'transactions' ORDER BY pg_relation_size(c.oid) DESC LIMIT 1`).Scan(&leaf))
	_, err := d.pool.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %[1]s SET (autovacuum_vacuum_threshold = 0, autovacuum_vacuum_scale_factor = 0,
			autovacuum_vacuum_cost_delay = 100, autovacuum_vacuum_cost_limit = 1);
		UPDATE %[1]s SET gas_used = gas_used + 1;
		SELECT pg_stat_force_next_flush()`, leaf))
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `ALTER SYSTEM SET autovacuum_naptime = '1s'`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `SELECT pg_reload_conf()`)
	require.NoError(t, err)
	defer func() {
		_, _ = d.pool.Exec(ctx, `ALTER SYSTEM RESET autovacuum_naptime`)
		_, _ = d.pool.Exec(ctx, `SELECT pg_reload_conf()`)
	}()

	autovacuuming := func() bool {
		var n int
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE backend_type = 'autovacuum worker' AND datname = current_database() AND query LIKE '%' || $1 || '%'`,
			leaf).Scan(&n))
		return n > 0
	}
	for deadline := time.Now().Add(30 * time.Second); !autovacuuming(); time.Sleep(200 * time.Millisecond) {
		require.True(t, time.Now().Before(deadline), "no autovacuum of %s started within 30 s", leaf)
	}
	time.Sleep(1200 * time.Millisecond)
	require.True(t, autovacuuming(), "the autovacuum of %s ended before the cut; it is too short to test", leaf)

	cutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, d.retentionPass(cutCtx, RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}),
		"the cut waited for the autovacuum of %s", leaf)
	cut, err := d.HistoryCut(ctx)
	require.NoError(t, err)
	require.True(t, cut)
}
