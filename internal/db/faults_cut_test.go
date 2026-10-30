//go:build faults

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

func cutReady(t *testing.T) (*fk, RetentionConfig, retentionTotals) {
	f := newFK(t)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000, DetachWait: 2 * time.Second, DropGateWait: 5 * time.Second}
	seedHistory(t, f.d, 0, 40_001, 1)
	return f, cfg, totalsAfterReseed(t, f.d)
}

func (f *fk) checkCut(before retentionTotals) {
	t := f.t
	t.Helper()
	require.Empty(t, f.violations())
	cut, err := f.d.HistoryCut(context.Background())
	require.NoError(t, err)
	require.True(t, cut, "the cut completed")
	require.Zero(t, f.n(`SELECT count(*) FROM pg_constraint WHERE conname = 'history_cut' AND NOT convalidated`), "no unvalidated check")
	require.Equal(t, int64(40_002), f.n(`SELECT count(*) FROM blocks`))
	require.Equal(t, before, totalsAfterReseed(t, f.d))
	require.NoError(t, f.headInsertable())
	cutAt := f.n(`SELECT hi FROM history_partitions WHERE suffix = 'legacy'`)
	require.NoError(t, insertCutTestBlock(f.d, uint64(cutAt)), "a block at the cut goes into the first partition")
}

func TestFaultCutMatrix(t *testing.T) {
	type c2 struct {
		cell
		extra *faultkit.Rule
	}
	var cells []c2
	add := func(match string, actions ...faultkit.Action) {
		for _, a := range actions {
			for _, r := range []bool{false, true} {
				cells = append(cells, c2{cell: cell{match, a, r}})
			}
		}
	}
	K, F, L := faultkit.KillBefore, faultkit.FailCommit, faultkit.LostAck
	add("pg_advisory_lock(", K, L)
	add("history_cut_constrain", K, F, L)
	add(`"blocks_legacy" validate constraint history_cut`, K, L)
	add(`"logs_legacy" validate constraint history_cut`, K, L)
	add(`"balances_legacy" validate constraint history_cut`, K, L)
	add("history_cut_finish", K, F, L)
	add("pg_advisory_unlock(", K)
	// the abandoned cut's release faulted too
	for _, r := range []bool{false, true} {
		cells = append(cells, c2{cell: cell{"history_cut_finish", K, r}, extra: &faultkit.Rule{Match: "drop constraint if exists history_cut", Action: K}})
		cells = append(cells, c2{cell: cell{"history_cut_finish", K, r}, extra: &faultkit.Rule{Match: "drop constraint if exists history_cut", Action: L}})
		cells = append(cells, c2{cell: cell{"history_cut_finish", K, r}, extra: &faultkit.Rule{Match: "drop constraint if exists history_cut", Action: F, Times: 3}})
	}
	for _, c := range cells {
		c := c
		name := c.String()
		if c.extra != nil {
			name += "+release:" + []string{"kill-before", "fail-commit", "lost-ack"}[c.extra.Action]
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f, cfg, before := cutReady(t)
			rule := f.proxy.Arm(faultkit.Rule{Match: c.match, Action: c.action})
			var extra *faultkit.Rule
			if c.extra != nil {
				extra = f.proxy.Arm(*c.extra)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			err := f.d.retentionPass(ctx, cfg)
			cancel()
			t.Logf("faulted pass: fired=%d err=%v", f.proxy.Fired(rule), err)
			if extra != nil {
				t.Logf("release fault fired=%d", f.proxy.Fired(extra))
			}
			require.Equal(t, 1, f.proxy.Fired(rule), "the fault was reached")
			t.Logf("after the faulted pass: %v, checks %d", f.violations(), historyCutChecks(t, f.d))
			if c.restart {
				require.NoError(t, f.restart(), "the restart's migrate and reconcile")
			}
			errs := f.recoverPasses(cfg, 4)
			t.Logf("recovery pass errors: %v", errs)
			require.Less(t, len(errs), 4, "recovery converges")
			f.checkCut(before)
		})
	}
}
