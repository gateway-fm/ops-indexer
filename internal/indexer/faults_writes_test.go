//go:build faults

package indexer

import (
	"fmt"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

type fkMode struct {
	name string
	head uint64
	cfg  func(*Config)
}

var fkModes = []fkMode{
	{"standard", 80, nil},
	{"standard-pipelined", 80, func(c *Config) { c.CommitPipelineDepth = 3 }},
	{"realtime", 300, nil},
	{"realtime-pipelined", 300, func(c *Config) { c.CommitPipelineDepth = 3 }},
}

func (m fkMode) config() *Config {
	c := fkConfig()
	if m.cfg != nil {
		m.cfg(c)
	}
	return c
}

var fkActions = []struct {
	name string
	a    faultkit.Action
}{{"lost-ack", faultkit.LostAck}, {"fail-commit", faultkit.FailCommit}, {"kill-before", faultkit.KillBefore}}

// waitFired waits for the rule to fire, bounded.
func (r *fkRig) waitFired(rule *faultkit.Rule, timeout time.Duration) {
	r.t.Helper()
	deadline := time.Now().Add(timeout)
	for r.proxy.Fired(rule) == 0 {
		if time.Now().After(deadline) {
			r.t.Fatalf("the fault on %q never fired within %s", rule.Match, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A reorg of 5 blocks at the stored head, with the k-th DeleteBlock of the revert faulted; optionally the process
// restarts right after the fault. The stored chain must end on the new fork with every counter right.
func TestFaultRevertFault(t *testing.T) {
	for _, m := range fkModes {
		for _, act := range fkActions {
			for _, skip := range []int{0, 2} {
				for _, restart := range []bool{false, true} {
					name := fmt.Sprintf("%s/%s/delete%d/restart=%v", m.name, act.name, skip+1, restart)
					t.Run(name, func(t *testing.T) {
						r := newFKRig(t, m.head)
						r.start(m.config())
						target := m.head - 1
						r.waitConverged(target, 90*time.Second)
						rule := r.proxy.Arm(faultkit.Rule{Match: "daily_stats_invalidate", Action: act.a, Skip: skip})
						r.node.Fork(target-4, 1)
						r.node.SetHead(m.head + 5)
						r.waitFired(rule, 30*time.Second)
						if restart {
							r.stop()
							r.start(m.config())
						}
						r.waitConverged(m.head+4, 90*time.Second)
					})
				}
			}
		}
	}
}

// The block write faulted: a commit whose acknowledgement is lost, one that rolls back, one never sent; at the
// 3rd block written, and in realtime mode at the 150th too (catch-up's range), with and without a restart.
func TestFaultBlockWriteFault(t *testing.T) {
	for _, m := range fkModes {
		for _, act := range fkActions {
			skips := []int{3}
			if m.head == 300 {
				skips = append(skips, 150)
			}
			for _, skip := range skips {
				for _, restart := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/write%d/restart=%v", m.name, act.name, skip+1, restart), func(t *testing.T) {
						r := newFKRig(t, m.head)
						rule := r.proxy.Arm(faultkit.Rule{Match: "insert into blocks", Action: act.a, Skip: skip})
						r.start(m.config())
						r.waitFired(rule, 60*time.Second)
						if restart {
							r.stop()
							r.start(m.config())
						}
						r.waitConverged(m.head-1, 90*time.Second)
						r.node.SetHead(m.head + 10)
						r.waitConverged(m.head+9, 90*time.Second)
					})
				}
			}
		}
	}
}

// The gap collector's and catch-up's writes faulted (realtime mode from an empty database: every block below the
// head is catch-up's).
func TestFaultRangeWriteFault(t *testing.T) {
	for _, mt := range []string{"insert into missing_block_ranges", "delete from missing_block_ranges", "update indexer_progress"} {
		for _, act := range fkActions {
			for _, restart := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/restart=%v", mt, act.name, restart), func(t *testing.T) {
					m := fkModes[2]
					r := newFKRig(t, 300)
					rule := r.proxy.Arm(faultkit.Rule{Match: mt, Action: act.a})
					r.start(m.config())
					r.waitFired(rule, 60*time.Second)
					if restart {
						r.stop()
						r.start(m.config())
					}
					r.waitConverged(299, 90*time.Second)
					r.node.SetHead(320)
					r.waitConverged(319, 90*time.Second)
				})
			}
		}
	}
}
