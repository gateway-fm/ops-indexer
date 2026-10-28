//go:build faults

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

// gaps lists block ranges below the newest partition that no attached partition covers (after the cut).
func (f *fk) partitionGaps() []string {
	return f.q(`
		WITH p AS (SELECT lo, hi, lag(hi) OVER (ORDER BY lo NULLS FIRST) AS prev FROM history_partitions WHERE state = 'attached')
		SELECT COALESCE(prev::text, '-inf') || '..' || COALESCE(lo::text, '-inf') FROM p
		WHERE (prev IS NULL AND lo IS NOT NULL AND lo > 0) OR (prev IS NOT NULL AND lo IS DISTINCT FROM prev)`)
}

// A FORCE_REINDEX wipe of a pruned database interrupted at a step. The indexer re-runs the wipe at the next start
// only while it still detects the reset (stored blocks far above the node head, or a stale fork too deep): once
// blocks is truncated it starts as on an empty database. So after the restart the new chain must be storable and
// visible from block 0 without a second wipe.
func TestFaultInterruptedWipe(t *testing.T) {
	steps := []string{
		"truncate table tokens",
		"truncate table ledger_spans",
		"truncate table history_archives",
		"update prune_state set pruned_through = 0",
		"drop table if exists daily_stats_refill",
		"history_partition_create(",
	}
	for _, s := range steps {
		for _, a := range []faultkit.Action{faultkit.KillBefore, faultkit.LostAck} {
			s, a := s, a
			t.Run(cell{s, a, true}.String(), func(t *testing.T) {
				t.Parallel()
				f, cfg, _ := pruneReady(t)
				require.Empty(t, f.recoverPasses(cfg, 3))
				require.Equal(t, int64(60_000), f.n(`SELECT pruned_through FROM prune_state`))
				rule := f.proxy.Arm(faultkit.Rule{Match: s, Action: a})
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				err := f.d.WipeAllData(ctx)
				cancel()
				t.Logf("wipe: fired=%d err=%v", f.proxy.Fired(rule), err)
				require.Equal(t, 1, f.proxy.Fired(rule))
				require.NoError(t, f.restart())
				blocks := f.n(`SELECT count(*) FROM blocks`)
				wm := f.n(`SELECT pruned_through FROM prune_state`)
				gaps := f.partitionGaps()
				var failed []string
				for _, b := range []uint64{0, 1, 2, 2_500, 30_000, 59_999} {
					if err := insertCutTestBlock(f.d, b); err != nil {
						failed = append(failed, fmt.Sprintf("%d: %v", b, err))
					}
				}
				visible := f.n(`SELECT count(*) FROM blocks WHERE number >= history_from()`)
				t.Logf("after the restart: blocks left %d, pruned_through %d, gaps %v, inserts failed %v, new blocks visible %d",
					blocks, wm, gaps, failed, visible)
				if blocks > 0 {
					t.Skipf("blocks not truncated: the next start detects the reset again and re-runs the wipe")
				}
				require.Zero(t, wm, "the new chain has nothing pruned")
				require.Empty(t, failed, "the new chain's blocks can be stored")
				require.Equal(t, int64(6), visible, "and read")
			})
		}
	}
}

func TestFaultWipeControl(t *testing.T) {
	f, cfg, _ := pruneReady(t)
	require.Empty(t, f.recoverPasses(cfg, 3))
	require.NoError(t, f.d.WipeAllData(context.Background()))
	require.NoError(t, f.restart())
	t.Logf("after the wipe: pruned_through %d gaps %v", f.n(`SELECT pruned_through FROM prune_state`), f.partitionGaps())
	for _, b := range []uint64{0, 1, 2, 2_500, 30_000, 59_999} {
		require.NoError(t, insertCutTestBlock(f.d, b))
	}
	require.Equal(t, int64(6), f.n(`SELECT count(*) FROM blocks WHERE number >= history_from()`))
}
