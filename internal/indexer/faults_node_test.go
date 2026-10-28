//go:build faults

package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

func u64(v uint64) *uint64 { return &v }

// fkRandom spreads the faulted calls pseudo-randomly, so a fixed number of calls per tick cannot alias with them.
func fkRandom(call int) uint64 { return (uint64(call)*2654435761 + 12345) % 1000003 }

// A node behind the head it reports (null above it), at a restart and while running.
func TestFaultLaggingNode(t *testing.T) {
	for _, m := range fkModes {
		t.Run(m.name+"/at-start", func(t *testing.T) {
			r := newFKRig(t, m.head)
			r.start(m.config())
			r.waitConverged(m.head-1, 90*time.Second)
			r.stop()
			r.node.LagAbove(u64(m.head - 10))
			r.start(m.config())
			time.Sleep(3 * time.Second)
			if err, ok := r.exited(); ok {
				t.Fatalf("the start against a lagging node exited: %v", err)
			}
			r.node.LagAbove(nil)
			r.node.SetHead(m.head + 5)
			r.waitConverged(m.head+4, 90*time.Second)
		})
		t.Run(m.name+"/running", func(t *testing.T) {
			r := newFKRig(t, m.head)
			r.start(m.config())
			r.waitConverged(m.head-1, 90*time.Second)
			r.node.LagAbove(u64(m.head - 5))
			r.node.SetHead(m.head + 10)
			time.Sleep(3 * time.Second)
			r.node.LagAbove(nil)
			r.waitConverged(m.head+9, 90*time.Second)
		})
	}
}

// Header-only lookups that disagree with full-block reads for a stored block: for 3 s, then permanently.
func TestFaultHeaderDisagrees(t *testing.T) {
	for _, m := range fkModes {
		t.Run(m.name+"/transient", func(t *testing.T) {
			r := newFKRig(t, m.head)
			r.start(m.config())
			target := m.head - 1
			r.waitConverged(target, 90*time.Second)
			r.node.HeaderFork(target-2, 7)
			r.node.SetHead(m.head + 5)
			time.Sleep(3 * time.Second)
			r.node.HeaderFork(target-2, 0)
			r.waitConverged(m.head+4, 90*time.Second)
		})
		t.Run(m.name+"/persistent", func(t *testing.T) {
			r := newFKRig(t, m.head)
			r.start(m.config())
			target := m.head - 1
			r.waitConverged(target, 90*time.Second)
			r.node.HeaderFork(target-2, 7)
			r.node.SetHead(m.head + 30)
			deadline := time.Now().Add(30 * time.Second)
			var maxStored int64
			for time.Now().Before(deadline) {
				_ = r.admin.QueryRow(context.Background(), `SELECT COALESCE(max(number), 0) FROM blocks`).Scan(&maxStored)
				if uint64(maxStored) >= m.head+29 {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			v := r.violations(uint64(maxStored))
			t.Logf("persistent header disagreement at %d: stored head %d (node head %d); reorg reads %d; differences %v",
				target-2, maxStored, m.head+30, r.node.Calls("eth_getBlockByNumber"), v)
			if uint64(maxStored) < m.head+29 {
				t.Errorf("ingest stalled at %d below the node head %d while one header lookup disagrees", maxStored, m.head+30)
			}
		})
	}
}

// RPC errors mid-operation: receipts errors, HTTP 500 on block reads, eth_blockNumber errors, receipts null for a
// block for a while.
func TestFaultRPCErrors(t *testing.T) {
	faults := map[string]faultkit.Hook{
		"receipts-error": faultkit.MethodHook("eth_getBlockReceipts", faultkit.Fault{Err: "upstream timeout"},
			func(_ uint64, call int) bool { return fkRandom(call)%7 == 3 }),
		"block-http500": faultkit.MethodHook("eth_getBlockByNumber", faultkit.Fault{HTTP: 503},
			func(_ uint64, call int) bool { return fkRandom(call)%11 == 5 }),
		"blocknumber-error": faultkit.MethodHook("eth_blockNumber", faultkit.Fault{Err: "busy"},
			func(_ uint64, call int) bool { return call > 1 && fkRandom(call)%3 == 1 }),
	}
	for _, m := range fkModes {
		for name, hook := range faults {
			t.Run(m.name+"/"+name, func(t *testing.T) {
				r := newFKRig(t, m.head)
				r.node.AddHook(hook)
				r.start(m.config())
				r.waitConverged(m.head-1, 90*time.Second)
				r.node.SetHead(m.head + 20)
				r.waitConverged(m.head+19, 90*time.Second)
				r.node.ClearHooks()
			})
		}
		t.Run(m.name+"/receipts-null-block", func(t *testing.T) {
			r := newFKRig(t, m.head)
			r.start(m.config())
			r.waitConverged(m.head-1, 90*time.Second)
			nullAt := m.head + 3
			stop := time.Now().Add(8 * time.Second)
			r.node.AddHook(func(method string, params []json.RawMessage, _ int) *faultkit.Fault {
				if method != "eth_getBlockReceipts" || time.Now().After(stop) {
					return nil
				}
				var s string
				_ = json.Unmarshal(params[0], &s)
				if s == fmt.Sprintf("0x%x", nullAt) {
					return &faultkit.Fault{Null: true}
				}
				return nil
			})
			r.node.SetHead(m.head + 10)
			r.waitConverged(m.head+9, 90*time.Second)
		})
	}
}

// One block read the node accepts and does not answer for 3 min: ingest gives it up at the RPC timeout (60 s by
// default) and goes on, instead of waiting for it.
func TestFaultStuckRPC(t *testing.T) {
	for _, m := range []fkMode{fkModes[0], fkModes[2]} {
		t.Run(m.name, func(t *testing.T) {
			r := newFKRig(t, m.head)
			r.start(m.config())
			r.waitConverged(m.head-1, 90*time.Second)
			var hung atomic.Bool
			r.node.AddHook(func(method string, params []json.RawMessage, _ int) *faultkit.Fault {
				if method == "eth_getBlockByNumber" {
					var full bool
					if len(params) > 1 {
						_ = json.Unmarshal(params[1], &full)
					}
					if full && hung.CompareAndSwap(false, true) {
						return &faultkit.Fault{Delay: 3 * time.Minute, Err: "late"}
					}
				}
				return nil
			})
			r.node.SetHead(m.head + 5)
			r.waitConverged(m.head+4, 100*time.Second)
		})
	}
}

// A long reader holding the xmin horizon and a lock on a history table while a reorg is reverted and catch-up
// fills: nothing ingest does needs a lock the reader conflicts with.
func TestFaultLongReader(t *testing.T) {
	for _, m := range fkModes {
		for _, table := range []string{"blocks", "transactions", "token_transfers"} {
			t.Run(m.name+"/"+table, func(t *testing.T) {
				r := newFKRig(t, m.head)
				end := faultkit.LongReader(t, r.url, table)
				defer end()
				r.start(m.config())
				target := m.head - 1
				r.waitConverged(target, 90*time.Second)
				r.node.Fork(target-4, 1)
				r.node.SetHead(m.head + 5)
				r.waitConverged(m.head+4, 90*time.Second)
			})
		}
	}
}

// Stored blocks on a stale fork above the node's head at a restart (a failover to a lagging node on the
// canonical fork); the node then passes the stored head.
func TestFaultStaleAboveNodeHead(t *testing.T) {
	for _, m := range fkModes {
		t.Run(m.name, func(t *testing.T) {
			r := newFKRig(t, m.head)
			r.start(m.config())
			target := m.head - 1
			r.waitConverged(target, 90*time.Second)
			r.stop()
			r.node.Fork(target-9, 3)
			r.node.SetHead(target - 10)
			r.start(m.config())
			time.Sleep(2 * time.Second)
			r.node.SetHead(m.head + 10)
			r.waitConverged(m.head+9, 90*time.Second)
		})
	}
}

// A revert at the start faulted part way (the start check's revert), then a restart.
func TestFaultStartRevertFault(t *testing.T) {
	for _, m := range fkModes {
		for _, act := range fkActions {
			t.Run(m.name+"/"+act.name, func(t *testing.T) {
				r := newFKRig(t, m.head)
				r.start(m.config())
				target := m.head - 1
				r.waitConverged(target, 90*time.Second)
				r.stop()
				r.node.Fork(target-4, 1)
				rule := r.proxy.Arm(faultkit.Rule{Match: "daily_stats_invalidate", Action: act.a, Skip: 2})
				r.start(m.config())
				r.waitFired(rule, 30*time.Second)
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					if _, ok := r.exited(); ok {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
				err, exited := r.exited()
				t.Logf("after the faulted start revert: exited=%v err=%v", exited, err)
				r.stop()
				r.start(m.config())
				r.node.SetHead(m.head + 5)
				r.waitConverged(m.head+4, 90*time.Second)
			})
		}
	}
}

// FORCE_REINDEX on a chain reset at the same height (a stale fork deeper than any reorg), with the wipe faulted.
func TestFaultForceReindex(t *testing.T) {
	for _, act := range append([]struct {
		name string
		a    faultkit.Action
	}{{"none", -1}}, fkActions...) {
		t.Run(act.name, func(t *testing.T) {
			t.Setenv("FORCE_REINDEX", "true")
			m := fkModes[2]
			r := newFKRig(t, m.head)
			r.start(m.config())
			r.waitConverged(m.head-1, 90*time.Second)
			r.stop()
			r.node.Fork(0, 5)
			if act.a >= 0 {
				r.proxy.Arm(faultkit.Rule{Match: "truncate", Action: act.a})
			}
			r.start(m.config())
			time.Sleep(2 * time.Second)
			if err, ok := r.exited(); ok {
				t.Logf("the first start exited: %v; restarting", err)
				r.stop()
				r.start(m.config())
			}
			r.waitConverged(m.head-1, 90*time.Second)
		})
	}
}
