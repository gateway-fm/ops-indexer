package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// spanStageWaiters is how many sessions wait for a single-key advisory lock, the form of the span stage lock.
func spanStageWaiters(t *testing.T, d *DB) int {
	t.Helper()
	var waiting int
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pg_locks l JOIN pg_database db ON db.oid = l.database
		WHERE l.locktype = 'advisory' AND NOT l.granted AND db.datname = current_database() AND l.objsubid = 1`).
		Scan(&waiting))
	return waiting
}

// Retention can hand a span the retained recount is staging to the node recount: it moves the pruned watermark past
// the span before it detaches the span's history, which the retained recount's share of the history gate delays. The
// two recounts stage the same span's rows and take the rebase lock in either order; they now take the span's stage
// lock first, so one waits for the other instead of each holding what the other wants (40P01). Both orders: the node
// recount starting once the retained recount has staged the span, and the retained recount starting while the node
// recount holds the span's stage lock and its stage rows.
func TestTheRetainedAndNodeRecountsOfASpanHandedOverDoNotDeadlock(t *testing.T) {
	finishes := func(t *testing.T, done <-chan error, what string) {
		t.Helper()
		select {
		case err := <-done:
			require.NoError(t, err, what)
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not finish: deadlock", what)
		}
	}

	t.Run("node recount during the retained recount", func(t *testing.T) {
		d, ctx := repairFixture(t)
		repairUntilDone(t, d)
		lo, hi, truth := firstSpan(t, d)
		settled := make(chan error, 1)
		staged := false
		retainedRecountStagedHook = func(l int64) {
			if staged || l != lo {
				return
			}
			staged = true
			_, err := d.pool.Exec(ctx, `UPDATE prune_state SET pruned_through = $1 WHERE id = 1`, hi-1)
			require.NoError(t, err, "retention hands the span to the node recount")
			go func() {
				c, err := d.SettleNodeSpan(ctx, lo, hi, truth, true)
				if err == nil && c.State != "ok" && c.State != "repaired" {
					t.Errorf("the node recount settled the span %q", c.State)
				}
				settled <- err
			}()
			select {
			case err := <-settled:
				settled <- err
			case <-time.After(2 * time.Second):
			}
		}
		t.Cleanup(func() { retainedRecountStagedHook = nil })
		_, ok, err := d.RecountRetainedSpan(ctx, true, "")
		require.NoError(t, err, "the retained recount")
		require.True(t, ok)
		require.True(t, staged)
		retainedRecountStagedHook = nil
		finishes(t, settled, "the node recount")
	})

	t.Run("retained recount during the node recount", func(t *testing.T) {
		d, ctx := repairFixture(t)
		repairUntilDone(t, d)
		lo, hi, _ := firstSpan(t, d)
		node, err := d.pool.Begin(ctx)
		require.NoError(t, err)
		defer node.Rollback(context.Background())
		for _, q := range []string{spanStageLockSQL, `SELECT pg_advisory_xact_lock(4672, 1)`,
			`SELECT pg_advisory_xact_lock(4674, 1)`, `SELECT ledger_stage_from_rows($1, $2)`} {
			args := []any{}
			switch q {
			case spanStageLockSQL:
				args = []any{lo}
			case `SELECT ledger_stage_from_rows($1, $2)`:
				args = []any{lo, hi}
			}
			_, err := node.Exec(ctx, q, args...)
			require.NoError(t, err)
		}
		recounted := make(chan error, 1)
		go func() {
			_, _, err := d.RecountRetainedSpan(ctx, true, "")
			recounted <- err
		}()
		require.Eventually(t, func() bool { return spanStageWaiters(t, d) == 1 }, 10*time.Second, 20*time.Millisecond,
			"the retained recount waits for the span's stage lock")
		require.NoError(t, node.Rollback(ctx))
		finishes(t, recounted, "the retained recount")
	})
}
