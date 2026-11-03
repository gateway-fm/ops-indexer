package db

import (
	"context"
	"fmt"
	"io/fs"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/tern/v2/migrate"
	"github.com/stretchr/testify/require"
)

// onFoldCounted runs f between a fold's count and its apply, with the number of counts so far.
func onFoldCounted(t *testing.T, f func(count int)) *atomic.Int64 {
	t.Helper()
	var counts atomic.Int64
	foldCountedHook = func() { f(int(counts.Add(1))) }
	t.Cleanup(func() { foldCountedHook = nil })
	return &counts
}

func foldGeneration(t *testing.T, d *DB) int64 {
	t.Helper()
	var g int64
	require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT generation FROM daily_stats_watermark WHERE id = 1`).Scan(&g))
	return g
}

func foldWatermark(t *testing.T, d *DB) int64 {
	t.Helper()
	var w int64
	require.NoError(t, d.pool.QueryRow(context.Background(), `SELECT last_block FROM daily_stats_watermark WHERE id = 1`).Scan(&w))
	return w
}

// forkedLateBlock is lateBlock n on another fork: more gas and one transaction fewer.
func forkedLateBlock(n int) *BlockData {
	data := lateBlock(n)
	data.Block.Hash = fmt.Sprintf("0xfork%059x", n)
	data.Block.GasUsed += 1000
	if len(data.Transactions) > 1 {
		data.Transactions = data.Transactions[:len(data.Transactions)-1]
		data.Block.TransactionCount--
	}
	return data
}

func replaceBlocks(t *testing.T, d *DB, lo, hi int, block func(int) *BlockData) {
	t.Helper()
	ctx := context.Background()
	for n := hi; n >= lo; n-- {
		require.NoError(t, d.DeleteBlock(ctx, uint64(n)))
	}
	for n := lo; n <= hi; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, block(n)))
	}
}

// The count holds no lock a revert or a block commit waits for.
func TestTheFoldCountsWithoutHoldingTheCommitLock(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 60; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	counts := onFoldCounted(t, func(int) {
		var free bool
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(4670, 1)`).Scan(&free))
		require.True(t, free, "(4670, 1) is free while the fold counts")
	})
	advanceUntilStable(t, d)
	require.Positive(t, counts.Load())
}

// A revert that commits between the count and the apply, and the blocks that replace it, are counted as the
// stored blocks are: the apply sees the revert's generation and the fold counts again. A revert of a block above the
// watermark, inside the range the fold counted, takes daily_stats_invalidate's early return; it moves the generation
// all the same.
func TestAFoldCountsAgainAfterARevertBetweenItsCountAndItsApply(t *testing.T) {
	for _, c := range []struct {
		name   string
		lo, hi int
		before bool
	}{
		{"a block in the counted range, between count and apply", 110, 110, false},
		{"blocks below the watermark, between count and apply", 95, 110, false},
		{"a block in the counted range, before the count", 110, 110, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			for n := 1; n <= 100; n++ {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
			}
			advanceUntilStable(t, d)
			require.EqualValues(t, 100, foldWatermark(t, d))
			for n := 101; n <= 110; n++ {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
			}
			if c.before {
				replaceBlocks(t, d, c.lo, c.hi, forkedLateBlock)
			}
			counts := onFoldCounted(t, func(count int) {
				if count == 1 && !c.before {
					replaceBlocks(t, d, c.lo, c.hi, forkedLateBlock)
				}
			})
			_, err := d.AdvanceDailyStats(ctx)
			require.NoError(t, err)
			if !c.before {
				require.GreaterOrEqual(t, counts.Load(), int64(2), "the apply refused the first count")
			}
			advanceUntilStable(t, d)
			requireStoredDaysEqualRecompute(t, d, c.name)
		})
	}
}

// Blocks a catch-up stores below the watermark and blocks stored above the counted range while the fold counts are
// each counted once.
func TestBlocksStoredWhileTheFoldCountsAreCountedOnce(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 100; n++ {
		if n != 50 {
			require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
		}
	}
	advanceUntilStable(t, d)
	for n := 101; n <= 110; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	onFoldCounted(t, func(count int) {
		if count == 1 {
			require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(50)))
			for n := 111; n <= 115; n++ {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
			}
		}
	})
	advanceUntilStable(t, d)
	requireStoredDaysEqualRecompute(t, d, "a late block and new blocks stored during the count")
}

// A refill writes days below the watermark. One that commits between a fold's count and its apply is kept: the fold
// staged every day's aggregate as it was before the refill, and applying them would write the refill back.
func TestARefillBetweenTheFoldsCountAndItsApplyIsKept(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 120; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(121)))
	day := time.Unix(lateBlocksMidnight-86400, 0).UTC()
	refill := RefillDay{Date: day, FirstBlock: 31, LastBlock: 60, Blocks: 30, Txs: 45, GasUsed: 123_456_789, SizeSum: 99,
		SizeN: 30, BlockTimeSum: 86_100, BlockTimeN: 30, GasPriceSum: big.NewInt(7)}
	counts := onFoldCounted(t, func(count int) {
		if count == 1 {
			_, err := d.WriteRefilledDailyStats(ctx, []RefillDay{refill}, false)
			require.NoError(t, err)
		}
	})
	_, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, counts.Load(), int64(2), "the apply refused the first count")
	var gas int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT gas_used FROM daily_stats_acc WHERE date = $1`, day).Scan(&gas))
	require.EqualValues(t, refill.GasUsed, gas, "the refilled day keeps the refill's sums")
}

// A rebase that moves a first_seen counts it in address_days_recount without the fold's lock, and the day recount
// that clears it takes the lock and moves the generation. Either, between the count and the apply, makes the fold
// count again from address_stats as it now is.
func TestAFirstSeenMoveBetweenTheFoldsCountAndItsApplyIsCountedAgain(t *testing.T) {
	for _, c := range []struct {
		name   string
		writer func(t *testing.T, d *DB)
	}{
		{"a rebase moves a first_seen", func(t *testing.T, d *DB) {
			_, err := d.pool.Exec(context.Background(), `UPDATE ledger_verify_state SET value = value + 1 WHERE name = 'address_days_recount'`)
			require.NoError(t, err)
		}},
		{"the day recount clears a pending move", func(t *testing.T, d *DB) {
			require.NoError(t, d.recountPendingDays(context.Background()))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			for n := 1; n <= 100; n++ {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
			}
			advanceUntilStable(t, d)
			_, err := d.pool.Exec(ctx, `UPDATE ledger_verify_state SET value = 1 WHERE name = 'address_days_recount'`)
			require.NoError(t, err)
			require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(101)))
			counts := onFoldCounted(t, func(count int) {
				if count == 1 {
					c.writer(t, d)
				}
			})
			_, err = d.AdvanceDailyStats(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(t, counts.Load(), int64(2), "the apply refused the first count")
		})
	}
}

// Every transaction that writes daily_stats, daily_stats_acc, daily_active_addresses or the watermark under (4670, 1)
// moves the generation, which is how a fold's apply knows its count is still the state it writes over.
func TestEveryWriterOfTheDailyTablesMovesTheGeneration(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 120; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	moves := func(label string, write func()) {
		t.Helper()
		before := foldGeneration(t, d)
		write()
		require.Greater(t, foldGeneration(t, d), before, label)
	}
	moves("a revert below the watermark", func() { require.NoError(t, d.DeleteBlock(ctx, 120)) })
	require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(120)))
	require.Less(t, foldWatermark(t, d), int64(120))
	moves("a revert above the watermark", func() { require.NoError(t, d.DeleteBlock(ctx, 120)) })
	require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(120)))
	moves("a fold", func() {
		_, err := d.AdvanceDailyStats(ctx)
		require.NoError(t, err)
	})
	moves("a refill", func() {
		_, err := d.WriteRefilledDailyStats(ctx, []RefillDay{{Date: time.Unix(lateBlocksMidnight-86400, 0).UTC(), FirstBlock: 31,
			LastBlock: 60, Blocks: 30, Txs: 45, GasUsed: 1, BlockTimeN: 30, BlockTimeSum: 86_100}}, false)
		require.NoError(t, err)
	})
	_, err := d.pool.Exec(ctx, `UPDATE ledger_verify_state SET value = 1 WHERE name = 'address_days_recount'`)
	require.NoError(t, err)
	moves("a day recount after a first_seen move", func() { require.NoError(t, d.recountPendingDays(ctx)) })
	_, err = d.pool.Exec(ctx, `TRUNCATE address_stats`)
	require.NoError(t, err)
	moves("the address_stats rebuild's day recount", func() { require.NoError(t, d.RebuildAddressStats(ctx)) })
}

func TestTheStatsBackfillCompletionMovesTheGeneration(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	const start = int64(1_000)
	ts := func(n int64) int64 { return dayStart - 200*7 + (n-start)*7 }
	nextDay := start
	for ts(nextDay) < dayStart {
		nextDay++
	}
	require.NoError(t, d.InitStatsBackfill(ctx, uint64(start-1), uint64(ts(start-1))))
	storeBlocks(t, d, span(start, nextDay+50), ts)
	advanceUntilStable(t, d)
	_, err := d.pool.Exec(ctx, `UPDATE stats_backfill SET through = target`)
	require.NoError(t, err)
	before := foldGeneration(t, d)
	require.NoError(t, d.CompleteStatsBackfill(ctx))
	require.Greater(t, foldGeneration(t, d), before)
}

// A revert between every count and its apply would hold the fold back for good. After a few calls that made no
// progress the fold advances once under the lock, as it used to, over at most the cap and without a day recount.
func TestRepeatedRevertsDoNotHoldTheFoldBack(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 100; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	for n := 101; n <= 110; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	fork := false
	onFoldCounted(t, func(int) {
		fork = !fork
		if fork {
			replaceBlocks(t, d, 110, 110, forkedLateBlock)
		} else {
			replaceBlocks(t, d, 110, 110, lateBlock)
		}
	})
	for call := 1; call <= foldStallsBeforeLocked && foldWatermark(t, d) < 110; call++ {
		_, err := d.AdvanceDailyStats(ctx)
		require.NoError(t, err)
	}
	require.EqualValues(t, 110, foldWatermark(t, d), "the locked fold advanced")
	foldCountedHook = nil
	advanceUntilStable(t, d)
	requireStoredDaysEqualRecompute(t, d, "after the locked fold")
}

// One call catches up, in applies of at most the cap.
func TestTheFoldAppliesAtMostTheCapAtATime(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	d.DailyStatsFoldMaxBlocks = 7
	for n := 1; n <= 30; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	for n := 31; n <= 90; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	before := foldWatermark(t, d)
	var counted []int64
	onFoldCounted(t, func(int) { counted = append(counted, foldWatermark(t, d)) })
	upto, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 90, upto)
	require.Len(t, counted, int((90-before+6)/7))
	for i := 1; i < len(counted); i++ {
		require.LessOrEqual(t, counted[i]-counted[i-1], int64(7))
	}
	requireStoredDaysEqualRecompute(t, d, "caught up seven blocks at a time")
}

// 037 rolls back to the definitions before it, and the fold runs again once it is applied again.
func TestMigration037RollsBackAndForward(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 60; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	require.NoError(t, err)
	sub, err := fs.Sub(migrations, "migrations")
	require.NoError(t, err)
	require.NoError(t, m.LoadMigrations(sub))
	latest := int32(len(m.Migrations))
	require.NoError(t, m.MigrateTo(ctx, 36))
	var columns, folds int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM information_schema.columns WHERE table_name = 'daily_stats_watermark' AND column_name = 'generation'),
		       (SELECT count(*) FROM pg_proc WHERE proname = 'daily_stats_fold_apply')`).Scan(&columns, &folds))
	require.Zero(t, columns+folds)
	var invalidate string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT prosrc FROM pg_proc WHERE proname = 'daily_stats_invalidate'`).Scan(&invalidate))
	require.NotContains(t, invalidate, "generation")
	require.Contains(t, invalidate, "DELETE FROM daily_stats_acc WHERE date >= d")
	_, err = d.pool.Exec(ctx, `SELECT daily_stats_invalidate(60)`)
	require.NoError(t, err, "015's invalidate runs on the rolled-back schema")

	require.NoError(t, m.MigrateTo(ctx, latest))
	for n := 61; n <= 90; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	require.EqualValues(t, 90, foldWatermark(t, d))
	requireStoredDaysEqualRecompute(t, d, "after 037 down and up")
}
