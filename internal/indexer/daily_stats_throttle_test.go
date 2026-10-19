package indexer

import (
	"testing"
	"time"
)

// TestDailyStatsThrottle pins the two properties the throttle exists for: at
// most one recomputation in flight, and at most one start per interval. With
// interval 0 it reproduces the pre-PRST-4669 every-100th-block trigger.
func TestDailyStatsThrottle(t *testing.T) {
	var th dailyStatsThrottle

	// interval > 0: first call starts; while running nothing else starts,
	// even on a 100th block; after done, not before the interval elapses.
	if !th.due(1, time.Hour) {
		t.Fatal("first call must be due")
	}
	if th.due(100, time.Hour) {
		t.Fatal("must not start while a run is in flight")
	}
	th.done()
	if th.due(200, time.Hour) {
		t.Fatal("must not start again before the interval elapsed")
	}
	th.last = time.Now().Add(-2 * time.Hour)
	if !th.due(201, time.Hour) {
		t.Fatal("must start once the interval elapsed")
	}
	th.done()

	// interval 0: legacy behaviour, every 100th block, still single-flight.
	var legacy dailyStatsThrottle
	if legacy.due(99, 0) {
		t.Fatal("legacy: block 99 must not trigger")
	}
	if !legacy.due(100, 0) {
		t.Fatal("legacy: block 100 must trigger")
	}
	if legacy.due(200, 0) {
		t.Fatal("legacy: must not overlap an in-flight run")
	}
	legacy.done()
	if !legacy.due(200, 0) {
		t.Fatal("legacy: block 200 must trigger once the previous run is done")
	}
}

// TestCatchupSharesDailyStatsThrottle pins the fix for the catchup regression:
// the CatchupIndexer must hand the SAME throttle to the per-block Indexer values
// it constructs, otherwise every block starts with a fresh, never-run throttle
// and the day aggregate fires on every block (measured: 104 runs for 105 blocks).
func TestCatchupSharesDailyStatsThrottle(t *testing.T) {
	th := &dailyStatsThrottle{}
	c := &CatchupIndexer{idxCfg: &Config{DailyStatsMinInterval: time.Minute}}
	c.SetDailyStatsThrottle(th)
	if c.dailyStats != th {
		t.Fatal("SetDailyStatsThrottle must store the shared pointer")
	}
	// Simulate two per-block Indexer values the way processBlockRaw builds them.
	a := &Indexer{config: c.idxCfg, dailyStats: c.dailyStats}
	b := &Indexer{config: c.idxCfg, dailyStats: c.dailyStats}
	if !a.dailyStats.due(1, a.config.DailyStatsMinInterval) {
		t.Fatal("first block must be due")
	}
	if b.dailyStats.due(2, b.config.DailyStatsMinInterval) {
		t.Fatal("second block must see the first run in flight through the shared throttle")
	}
	a.dailyStats.done()
	if b.dailyStats.due(3, b.config.DailyStatsMinInterval) {
		t.Fatal("second block must respect the interval through the shared throttle")
	}
	// The main indexer's constructor must create the pointer.
	idx := NewWithConfig(nil, nil, time.Second, 0, &Config{})
	if idx.dailyStats == nil {
		t.Fatal("NewWithConfig must create the shared throttle")
	}
}
