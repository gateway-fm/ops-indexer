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

// pruneReady: cut at 44,001, history 0..80,001, ledger closed: a pass prunes legacy and p44001..p58001 to 60,000.
func pruneReady(t *testing.T) (*fk, RetentionConfig, retentionTotals) {
	f := newFK(t)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000, DetachWait: 2 * time.Second,
		DropGateWait: 5 * time.Second, ArchiveDir: t.TempDir()}
	seedHistory(t, f.d, 0, 40_001, 1)
	require.NoError(t, f.d.retentionPass(context.Background(), cfg))
	ensurePartitions(t, f.d, 100_001, cfg)
	seedHistory(t, f.d, 40_002, 80_001, 1)
	closeLedger(t, f.d)
	return f, cfg, totalsAfterReseed(t, f.d)
}

func (f *fk) checkPruned(before retentionTotals, wm int64) {
	t := f.t
	t.Helper()
	require.Empty(t, f.violations())
	require.Equal(t, wm, f.n(`SELECT pruned_through FROM prune_state`), "watermark")
	require.Equal(t, 80_001-wm, f.n(`SELECT count(*) FROM blocks WHERE number > $1`, wm), "blocks above the watermark")
	require.Zero(t, f.n(`SELECT count(*) FROM blocks WHERE number <= $1`, wm), "blocks at or below the watermark")
	require.Equal(t, before, totalsAfterReseed(t, f.d), "totals keep pruned history exactly once")
	require.NoError(t, f.headInsertable(), "ingest goes on at the head")
}

type cell struct {
	match   string
	action  faultkit.Action
	restart bool
}

func (c cell) String() string {
	r := ""
	if c.restart {
		r = "+restart"
	}
	return fmt.Sprintf("%s/%s%s", []string{"kill-before", "fail-commit", "lost-ack"}[c.action], c.match, r)
}

func TestFaultPruneMatrix(t *testing.T) {
	steps := []string{
		"prune_advance(",
		`detach partition "blocks_legacy" concurrently`,
		`detach partition "logs_legacy" concurrently`,
		"set state = 'detached'",
		`copy "transactions_legacy" to stdout`,
		"insert into history_archives",
		`drop table if exists "blocks_legacy"`,
		`drop table if exists "logs_legacy"`,
		"delete from history_partitions",
		`detach partition "token_transfers_p44001" concurrently`,
		`drop table if exists "balances_p50001"`,
	}
	var cells []cell
	for _, s := range steps {
		for _, a := range []faultkit.Action{faultkit.KillBefore, faultkit.LostAck} {
			for _, r := range []bool{false, true} {
				cells = append(cells, cell{s, a, r})
			}
		}
	}
	for _, c := range cells {
		c := c
		t.Run(c.String(), func(t *testing.T) {
			t.Parallel()
			f, cfg, before := pruneReady(t)
			rule := f.proxy.Arm(faultkit.Rule{Match: c.match, Action: c.action})
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			err := f.d.retentionPass(ctx, cfg)
			cancel()
			fired := f.proxy.Fired(rule)
			t.Logf("faulted pass: fired=%d err=%v", fired, err)
			require.Equal(t, 1, fired, "the fault was reached")
			if c.restart {
				require.NoError(t, f.restart(), "the restart's migrate and reconcile")
			}
			errs := f.recoverPasses(cfg, 6)
			t.Logf("recovery pass errors: %v", errs)
			require.Less(t, len(errs), 6, "recovery converges")
			f.checkPruned(before, 60_000)
		})
	}
}
