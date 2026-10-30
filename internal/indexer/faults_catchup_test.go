//go:build faults

package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

// Catch-up's existence check fails once (a dropped connection) for a block it has dequeued; and, separately, a block
// whose write fails and whose requeue fails too. Is the block ever indexed while the process runs?
func TestFaultCatchupDroppedBlock(t *testing.T) {
	cases := map[string][]faultkit.Rule{
		"hasblock-error": {{Match: "select exists(select 1 from blocks where number", Action: faultkit.KillBefore, Skip: 40}},
		"write-and-requeue-fail": {
			{Match: "insert into blocks", Action: faultkit.FailCommit, Skip: 40},
			{Match: "values ($1, $1)", Action: faultkit.KillBefore},
		},
	}
	for name, rules := range cases {
		t.Run(name, func(t *testing.T) {
			m := fkModes[2]
			r := newFKRig(t, m.head)
			var armed []*faultkit.Rule
			for _, rule := range rules {
				armed = append(armed, r.proxy.Arm(rule))
			}
			r.start(m.config())
			r.waitFired(armed[0], 60*time.Second)
			r.node.SetHead(m.head + 10)
			r.waitConverged(m.head+9, 60*time.Second)
		})
	}
}

// A restart with a gap in the stored chain, the startup gap scan's batch faulted (its statement killed, as a statement
// timeout or a dropped connection ends it): the gap is still found and filled while the process runs.
func TestFaultStartupScanFault(t *testing.T) {
	for _, act := range fkActions {
		t.Run(act.name, func(t *testing.T) {
			m := fkModes[2]
			r := newFKRig(t, m.head)
			r.start(m.config())
			r.waitConverged(m.head-1, 90*time.Second)
			r.stop()
			d, err := db.New(r.url)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range []uint64{150, 151, 200} {
				if err := d.DeleteBlock(context.Background(), n); err != nil {
					t.Fatal(err)
				}
			}
			d.Close()
			rule := r.proxy.Arm(faultkit.Rule{Match: "statement_timeout", Action: act.a})
			r.start(m.config())
			r.waitFired(rule, 30*time.Second)
			r.waitConverged(m.head-1, 90*time.Second)
		})
	}
}

// Header lookups disagree with full reads for the stored head at a start: the start check sees a stale head.
func TestFaultStartHeaderDisagrees(t *testing.T) {
	for _, m := range fkModes {
		t.Run(m.name, func(t *testing.T) {
			r := newFKRig(t, m.head)
			r.start(m.config())
			target := m.head - 1
			r.waitConverged(target, 90*time.Second)
			r.stop()
			r.node.HeaderFork(target, 7)
			r.start(m.config())
			time.Sleep(2 * time.Second)
			if err, ok := r.exited(); ok {
				t.Fatalf("the start exited: %v", err)
			}
			r.node.HeaderFork(target, 0)
			r.node.SetHead(m.head + 5)
			r.waitConverged(m.head+4, 90*time.Second)
		})
	}
}

// The opt-in jump to the head: the write that hands the skipped blocks to catch-up faulted.
func TestFaultJumpFault(t *testing.T) {
	for _, act := range fkActions {
		t.Run(act.name, func(t *testing.T) {
			m := fkMode{"realtime-jump", 300, func(c *Config) { c.RealtimeMaxBehind = 50 }}
			r := newFKRig(t, m.head)
			r.start(m.config())
			r.waitConverged(m.head-1, 90*time.Second)
			rule := r.proxy.Arm(faultkit.Rule{Match: "insert into missing_block_ranges", Action: act.a})
			r.node.SetHead(m.head + 400)
			r.waitFired(rule, 30*time.Second)
			r.waitConverged(m.head+399, 120*time.Second)
		})
	}
}
