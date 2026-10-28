//go:build faults

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// A detach a reader held off stays pending; the FINALIZE that completes it is faulted.
func TestFaultFinalizeMatrix(t *testing.T) {
	for _, a := range []faultkit.Action{faultkit.KillBefore, faultkit.LostAck} {
		for _, r := range []bool{false, true} {
			c := cell{`detach partition "logs_legacy" finalize`, a, r}
			t.Run(c.String(), func(t *testing.T) {
				t.Parallel()
				f, cfg, before := pruneReady(t)
				end := faultkit.LongReader(t, f.url, "logs_legacy")
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				_ = f.d.retentionPass(ctx, cfg)
				cancel()
				end()
				pending := f.n(`SELECT count(*) FROM pg_inherits WHERE inhrelid = to_regclass('logs_legacy') AND inhdetachpending`)
				require.Equal(t, int64(1), pending, "the reader left the detach pending")
				rule := f.proxy.Arm(faultkit.Rule{Match: c.match, Action: c.action})
				ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)
				err := f.d.retentionPass(ctx, cfg)
				cancel()
				t.Logf("faulted pass: fired=%d err=%v", f.proxy.Fired(rule), err)
				require.Equal(t, 1, f.proxy.Fired(rule))
				if c.restart {
					require.NoError(t, f.restart())
				}
				errs := f.recoverPasses(cfg, 4)
				require.Less(t, len(errs), 4, "%v", errs)
				f.checkPruned(before, 60_000)
			})
		}
	}
}

// Partitions kept ahead after an outage: the ensure's statement is faulted.
func TestFaultEnsureMatrix(t *testing.T) {
	for _, a := range []faultkit.Action{faultkit.KillBefore, faultkit.FailCommit, faultkit.LostAck} {
		for _, r := range []bool{false, true} {
			c := cell{"history_partitions_ensure(", a, r}
			t.Run(c.String(), func(t *testing.T) {
				t.Parallel()
				f, cfg, _ := cutReady(t)
				require.NoError(t, f.d.retentionPass(context.Background(), cfg))
				rule := f.proxy.Arm(faultkit.Rule{Match: c.match, Action: c.action, Times: 2})
				_, err := f.d.EnsurePartitionsAhead(context.Background(), 120_000, cfg.PartitionBlocks)
				t.Logf("faulted ensure: fired=%d err=%v", f.proxy.Fired(rule), err)
				require.GreaterOrEqual(t, f.proxy.Fired(rule), 1)
				if f.proxy.Inapplicable(rule) {
					t.Skip("history_partitions_ensure runs outside a transaction: there is no COMMIT to fail")
				}
				if c.restart {
					require.NoError(t, f.restart())
				}
				f.proxy.Disarm()
				for i := 0; i < 10; i++ {
					var made int
					if made, err = f.d.EnsurePartitionsAhead(context.Background(), 120_000, cfg.PartitionBlocks); err == nil && made == 0 {
						break
					}
				}
				require.NoError(t, err)
				require.Empty(t, f.violations())
				require.Empty(t, f.partitionGaps())
				require.NoError(t, insertCutTestBlock(f.d, 120_000))
				require.NoError(t, insertCutTestBlock(f.d, 50_000))
			})
		}
	}
}

// The checks catch what they are for: a range row without its tables, an orphan table, a leftover check, a gap.
func TestFaultHarnessCatches(t *testing.T) {
	f, cfg, _ := cutReady(t)
	require.NoError(t, f.d.retentionPass(context.Background(), cfg))
	ensurePartitions(t, f.d, 60_000, cfg)
	require.Empty(t, f.violations())
	_, err := f.admin.Exec(context.Background(), `ALTER TABLE logs DETACH PARTITION logs_p46001; DROP TABLE logs_p46001;
		CREATE TABLE blocks_p999999 (LIKE blocks)`)
	require.NoError(t, err)
	v := f.violations()
	t.Logf("sabotaged: %v", v)
	require.Len(t, v, 3, "row without table, the same row not attached, orphan table")
	_, err = f.admin.Exec(context.Background(), `DELETE FROM history_partitions WHERE suffix = 'p48001'`)
	require.NoError(t, err)
	require.NotEmpty(t, f.partitionGaps())
}

// A restart with an abandoned cut's check left, the startup's release faulted.
func TestFaultReconcileReleaseFaulted(t *testing.T) {
	for _, a := range []faultkit.Action{faultkit.KillBefore, faultkit.LostAck} {
		a := a
		t.Run(cell{"drop constraint if exists history_cut", a, true}.String(), func(t *testing.T) {
			t.Parallel()
			f, cfg, before := cutReady(t)
			_, err := f.d.pool.Exec(context.Background(), `SELECT history_cut_constrain(44001)`)
			require.NoError(t, err)
			for _, tbl := range HistoryTables {
				_, err := f.d.pool.Exec(context.Background(), `ALTER TABLE `+tbl+`_legacy VALIDATE CONSTRAINT history_cut`)
				require.NoError(t, err)
			}
			f.d.pool.Close()
			f.open()
			rule := f.proxy.Arm(faultkit.Rule{Match: "drop constraint if exists history_cut", Action: a})
			err = f.d.Migrate()
			t.Logf("startup with the release faulted: fired=%d err=%v checks=%d", f.proxy.Fired(rule), err, historyCutChecks(t, f.d))
			require.NoError(t, err, "the startup goes on")
			require.Equal(t, 1, f.proxy.Fired(rule))
			f.proxy.Disarm()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			d2 := f.d
			done := make(chan struct{})
			go func() { defer close(done); d2.RunRetention(ctx, RetentionConfig{RetainBlocks: 0}) }()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("retention did not release the check")
			}
			require.Zero(t, historyCutChecks(t, f.d))
			errs := f.recoverPasses(cfg, 4)
			require.Less(t, len(errs), 4, "%v", errs)
			f.checkCut(before)
		})
	}
}

func TestFaultNodeHeadErrorDuringTheCut(t *testing.T) {
	for _, fault := range []faultkit.Fault{{Err: "upstream unavailable"}, {HTTP: 502}} {
		fault := fault
		t.Run(fmt.Sprintf("%+v", fault), func(t *testing.T) {
			t.Parallel()
			f, cfg, before := cutReady(t)
			node := faultkit.NewNode(t, 40_001)
			node.AddHook(faultkit.MethodHook("eth_blockNumber", fault, nil))
			client, err := rpc.New(node.URL())
			require.NoError(t, err)
			cfg.NodeHead = client.BlockNumber
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			require.NoError(t, f.d.retentionPass(ctx, cfg), "the cut goes on above the stored head")
			require.GreaterOrEqual(t, node.Calls("eth_blockNumber"), 1)
			f.checkCut(before)
		})
	}
}
