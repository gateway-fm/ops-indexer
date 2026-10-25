package indexer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

var (
	day1 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	day2 = day1.Add(24 * time.Hour)
)

// TestDailyStatsThrottle pins the two properties the throttle exists for: at
// most one recomputation in flight, and at most one start per interval. With
// interval 0 it reproduces the pre-PRST-4669 every-100th-block trigger.
func TestDailyStatsThrottle(t *testing.T) {
	var th dailyStatsThrottle

	if days := th.due(1, day1, time.Hour); days == nil {
		t.Fatal("first call must be due")
	}
	if days := th.due(100, day1, time.Hour); days != nil {
		t.Fatal("must not start while a run is in flight")
	}
	th.done()
	if days := th.due(200, day1, time.Hour); days != nil {
		t.Fatal("must not start again before the interval elapsed")
	}
	th.last = time.Now().Add(-2 * time.Hour)
	if days := th.due(201, day1, time.Hour); days == nil {
		t.Fatal("must start once the interval elapsed")
	}
	th.done()

	var legacy dailyStatsThrottle
	if days := legacy.due(99, day1, 0); days != nil {
		t.Fatal("legacy: block 99 must not trigger")
	}
	if days := legacy.due(100, day1, 0); days == nil {
		t.Fatal("legacy: block 100 must trigger")
	}
	if days := legacy.due(200, day1, 0); days != nil {
		t.Fatal("legacy: must not overlap an in-flight run")
	}
	legacy.done()
	if days := legacy.due(200, day1, 0); days == nil {
		t.Fatal("legacy: block 200 must trigger once the previous run is done")
	}
}

// Every block marks its day dirty and a run recomputes all dirty days, so a day
// is recomputed after its last block even when catch-up commits older days
// after newer ones and the slot is taken by the head.
func TestDailyStatsThrottleRecomputesEveryDirtyDay(t *testing.T) {
	var th dailyStatsThrottle
	if days := th.due(1, day2, time.Hour); len(days) != 1 || !days[0].Equal(day2) {
		t.Fatalf("first block: got %v, want [day2]", days)
	}
	th.done()
	if days := th.due(2, day1, time.Hour); days != nil {
		t.Fatalf("within the interval: got %v, want no run", days)
	}
	if days := th.due(3, day2, time.Hour); days != nil {
		t.Fatalf("within the interval: got %v, want no run", days)
	}
	th.last = time.Now().Add(-2 * time.Hour)
	days := th.due(4, day2, time.Hour)
	if len(days) != 2 || !days[0].Equal(day1) || !days[1].Equal(day2) {
		t.Fatalf("after the interval: got %v, want [day1 day2]", days)
	}
	th.done()
	th.last = time.Now().Add(-2 * time.Hour)
	if days := th.due(5, day2, time.Hour); len(days) != 1 || !days[0].Equal(day2) {
		t.Fatalf("dirty days are cleared by a run: got %v, want [day2]", days)
	}
}

// A block committed while a run is in flight marks its day for the next run.
func TestDailyStatsThrottleKeepsDaysMarkedWhileRunning(t *testing.T) {
	var th dailyStatsThrottle
	th.due(1, day1, time.Hour)
	if days := th.due(2, day1, time.Hour); days != nil {
		t.Fatal("must not start while a run is in flight")
	}
	th.done()
	th.last = time.Now().Add(-2 * time.Hour)
	if days := th.due(3, day2, time.Hour); len(days) != 2 || !days[0].Equal(day1) {
		t.Fatalf("got %v, want day1 (marked while running) and day2", days)
	}
}

// The per-block Indexer values built by the catch-up and realtime indexers must
// share the main indexer's throttle. A fresh throttle per block fires the day
// aggregate on every block (measured: 104 runs for 105 blocks); none at all
// stops daily_stats at the head.
func TestSubIndexersShareDailyStatsThrottle(t *testing.T) {
	idx := NewWithConfig(nil, nil, time.Second, 0, &Config{DailyStatsMinInterval: time.Minute})
	if idx.dailyStats == nil {
		t.Fatal("NewWithConfig must create the shared throttle")
	}
	catchup := idx.newCatchupIndexer(&CatchupConfig{Workers: 1, BatchSize: 1, QueueSize: 1})
	realtime := idx.newRealtimeIndexer(&RealtimeConfig{ConfirmationBlocks: 1, PollInterval: time.Second})
	for name, blockIndexer := range map[string]*Indexer{
		"catchup":  catchup.blockIndexer(),
		"realtime": realtime.blockIndexer(),
	} {
		if blockIndexer.dailyStats != idx.dailyStats {
			t.Errorf("%s per-block Indexer does not share the main indexer's throttle", name)
		}
	}
}

// A day whose recomputation failed goes back into the dirty set for the next run.
func TestDailyStatsThrottleRetriesAFailedDay(t *testing.T) {
	var th dailyStatsThrottle
	th.due(1, day1, time.Hour)
	th.markDirty(day1)
	th.done()
	th.last = time.Now().Add(-2 * time.Hour)
	if days := th.due(2, day2, time.Hour); len(days) != 2 || !days[0].Equal(day1) {
		t.Fatalf("got %v, want the failed day1 retried with day2", days)
	}
}

// The last blocks before a quiet period have no later block to start their run,
// so a timer advances daily_stats once the interval has passed.
func TestDailyStatsAdvancedAfterQuietPeriod(t *testing.T) {
	db := new(MockDatabase)
	advanced := make(chan struct{}, 4)
	db.On("AdvanceDailyStats", mock.Anything).
		Run(func(mock.Arguments) { advanced <- struct{}{} }).
		Return(0, nil)
	idx := NewWithConfig(db, nil, time.Second, 0, &Config{DailyStatsMinInterval: 50 * time.Millisecond})

	idx.scheduleDailyStats(t.Context(), 1, day1)
	<-advanced
	idx.scheduleDailyStats(t.Context(), 2, day2)
	awaitAdvance(t, advanced, "day2")

	day3 := day2.Add(24 * time.Hour)
	idx.dailyStats.markDirty(day3)
	idx.scheduleDeferredDailyStats(t.Context())
	awaitAdvance(t, advanced, "day3")
}

func awaitAdvance(t *testing.T, advanced <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-advanced:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s was never advanced: no later block arrived to start its run", what)
	}
}
