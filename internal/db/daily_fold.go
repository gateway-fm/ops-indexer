package db

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

// lateDays remembers the days that may hold a block stored below the daily_stats watermark (migration 012), which
// daily_stats_advance never reaches: on a fresh sync realtime stores the head first and the first advance starts at
// the head's day, and catch-up stores the earlier blocks afterwards; an invalidated day restarts at its first stored
// block. Every block write notes its day, unless the block is above the watermark the last advance returned, where
// the advance itself counts it. A restart forgets the notes, so the first advance after one looks for such days in
// the stored blocks instead.
type lateDays struct {
	mu        sync.Mutex
	watermark *int64
	days      map[int64]struct{}
	searched  bool
	recounts  atomic.Int64
}

func (l *lateDays) noteBlock(number, timestamp uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.watermark != nil && int64(number) > *l.watermark {
		return
	}
	if l.days == nil {
		l.days = map[int64]struct{}{}
	}
	l.days[int64(timestamp)/86400] = struct{}{}
}

func (l *lateDays) take() (days []int64, search bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for day := range l.days {
		days = append(days, day)
	}
	l.days = nil
	return days, !l.searched
}

func (l *lateDays) giveBack(days []int64, search bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.days == nil {
		l.days = map[int64]struct{}{}
	}
	for _, day := range days {
		l.days[day] = struct{}{}
	}
	if search {
		l.searched = false
	}
}

func (l *lateDays) advanced(watermark int64, searched bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.watermark = &watermark
	if searched {
		l.searched = true
	}
}

// foldCountedHook, when a test sets it, runs between a fold's count and its apply.
var foldCountedHook func()

const (
	// defaultFoldMaxBlocks bounds the blocks one fold counts, and with them its snapshot and the rows it writes under
	// the lock.
	defaultFoldMaxBlocks = 2000
	// foldCounts is how many times one fold counts before it gives up for this call: each count after the first
	// follows an apply that found a writer of the daily tables committed since the count's snapshot.
	foldCounts = 3
	// foldStallsBeforeLocked is how many calls in a row may apply nothing before one folds under the lock.
	foldStallsBeforeLocked = 5
)

// foldStageSQL creates this connection's staging tables once: a read-only transaction cannot create them, and
// creating them for every fold would write the catalog as often.
const foldStageSQL = `
	CREATE TEMP TABLE IF NOT EXISTS fold_acc (LIKE daily_stats_acc INCLUDING ALL);
	CREATE TEMP TABLE IF NOT EXISTS fold_active (LIKE daily_active_addresses INCLUDING ALL);
	CREATE TEMP TABLE IF NOT EXISTS fold_gone (LIKE daily_active_addresses INCLUDING ALL);
	CREATE TEMP TABLE IF NOT EXISTS fold_reset (date DATE PRIMARY KEY);
	CREATE TEMP TABLE IF NOT EXISTS fold_stats (LIKE daily_stats INCLUDING ALL)`

// emptyFoldStageSQL empties the staging tables, before each count and before the connection goes back to the pool,
// where they would otherwise keep the last fold's rows for as long as the session lives.
const emptyFoldStageSQL = `TRUNCATE fold_acc, fold_active, fold_gone, fold_reset, fold_stats`

// bumpDailyGenerationSQL is run under (4670, 1) by every transaction that writes the daily tables, so that a fold
// counted before it does not apply over it (migration 037).
const bumpDailyGenerationSQL = `UPDATE daily_stats_watermark SET generation = generation + 1 WHERE id = 1`

// dailyTables names the aggregate and the rows rewriteFrom writes: the live ones, or a fold's staged copies.
type dailyTables struct{ acc, stats string }

var (
	liveDays   = dailyTables{acc: "daily_stats_acc", stats: "daily_stats"}
	stagedDays = dailyTables{acc: "fold_acc", stats: "fold_stats"}
)

// foldSnapshot is what a fold's count read and its apply requires unchanged: the watermark, the generation, the
// first_seen moves no day recount has taken in yet, and the pruned height.
type foldSnapshot struct {
	watermark, generation, pruned int64
	moves                         *int64
}

// foldCount is one staged fold: counted through upto; the address sets of the days before keptFrom are dropped
// (nil when the watermark does not move); later are complete late days left for the next count, pending those
// still missing blocks.
type foldCount struct {
	snapshot       foldSnapshot
	upto           int64
	keptFrom       *time.Time
	recounted      bool
	later, pending []int64
}

// AdvanceDailyStats folds the blocks indexed since the last call into daily_stats (migration 012) and returns the
// block it aggregated through. It recounts every day that took a block below that block (lateDays) from the stored
// blocks once the day's blocks are complete, so each block counts once whatever order it was stored in, and carries
// the count of earlier transactions through every later day. A day still missing blocks waits for a later advance.
//
// The fold counts without (4670, 1), the lock its writes take and a revert takes first (PRST-5627). It counts in
// one snapshot, into this connection's staging tables, at most DailyStatsFoldMaxBlocks blocks and one late day
// with the days its recount carries into; daily_stats_fold_apply (037) then writes them under the lock if no writer
// of the daily tables committed since the snapshot, and the fold goes on until it is through the stored blocks and
// the late days. A refused apply counts again, foldCounts times at most. A call that applies nothing is a stall;
// after foldStallsBeforeLocked in a row one capped advance runs under the lock without a day recount, so reverts
// between every count and its apply cannot hold the days back.
func (d *DB) AdvanceDailyStats(ctx context.Context) (uint64, error) {
	days, search := d.lateDays.take()
	searched := false
	if search {
		found, ok, err := d.searchLateDays(ctx)
		if err != nil {
			d.lateDays.giveBack(days, true)
			return 0, err
		}
		days, searched = append(days, found...), ok
	}
	upto, pending, done, err := d.fold(ctx, days)
	if err != nil || upto == nil {
		d.lateDays.giveBack(days, search)
		return 0, err
	}
	if done {
		d.lateDays.giveBack(pending, search && !searched)
	} else {
		d.lateDays.giveBack(days, search)
	}
	d.lateDays.advanced(*upto, searched && done)
	d.reportDailyStatsGap(ctx, *upto)
	return uint64(*upto), nil
}

// fold runs counts and applies on one connection, whose session holds the staging tables, until the watermark is
// through the stored blocks and the late days are recounted (done), or a count is refused foldCounts times. It
// returns the watermark and the late days still missing blocks; nil before the first watermark.
func (d *DB) fold(ctx context.Context, days []int64) (upto *int64, pending []int64, done bool, err error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	staged := false
	defer func() {
		if staged {
			emptyFoldStage(ctx, conn)
		}
		conn.Release()
	}()
	var started bool
	if err := conn.QueryRow(ctx, startDailyStatsWatermarkSQL).Scan(&started); err != nil || !started {
		return nil, nil, false, err
	}
	if _, err := conn.Exec(ctx, foldStageSQL); err != nil {
		return nil, nil, false, err
	}
	staged = true
	limit := d.DailyStatsFoldMaxBlocks
	if limit <= 0 {
		limit = defaultFoldMaxBlocks
	}
	for {
		var c foldCount
		applied := false
		for n := 0; n < foldCounts && !applied; n++ {
			if c, err = countFold(ctx, conn, &d.lateDays, limit, days); err != nil {
				return nil, nil, false, err
			}
			if c.upto == c.snapshot.watermark && !c.recounted {
				return &c.upto, append(pending, c.pending...), true, nil
			}
			if foldCountedHook != nil {
				foldCountedHook()
			}
			s := c.snapshot
			if err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				return tx.QueryRow(ctx, `SELECT daily_stats_fold_apply($1, $2, $3, $4, $5, $6)`,
					s.watermark, s.generation, s.moves, s.pruned, c.upto, c.keptFrom).Scan(&applied)
			}); err != nil {
				return nil, nil, false, err
			}
			if !applied {
				metrics.DailyStatsFoldRefused()
			}
		}
		if !applied {
			if d.foldStalls.Add(1) < foldStallsBeforeLocked {
				return &c.snapshot.watermark, nil, false, nil
			}
			d.foldStalls.Store(0)
			metrics.DailyStatsFoldLocked()
			upto, err := lockedFold(ctx, conn, limit)
			return upto, nil, false, err
		}
		d.foldStalls.Store(0)
		pending, days = append(pending, c.pending...), c.later
		if c.upto-c.snapshot.watermark < limit && len(days) == 0 {
			return &c.upto, pending, true, nil
		}
	}
}

// emptyFoldStage empties the staging tables even when the fold's context is done, and closes the connection when it
// cannot, so that the pool does not hand it out again holding them.
func emptyFoldStage(ctx context.Context, conn *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, emptyFoldStageSQL); err != nil {
		_ = conn.Conn().Close(ctx)
	}
}

// countFold stages one fold in a read-only snapshot: a copy of daily_stats_acc, what daily_stats_advance would add
// for the next contiguous stored blocks above the watermark, up to limit of them, and the recount of the first
// complete late day with the days it carries into (recountDays), each day's daily_stats row they change, and the
// address pairs to add to daily_active_addresses (fold_active) and to take out of it (fold_gone).
func countFold(ctx context.Context, conn *pgxpool.Conn, late *lateDays, limit int64, days []int64) (c foldCount, err error) {
	if _, err := conn.Exec(ctx, emptyFoldStageSQL); err != nil {
		return c, err
	}
	err = pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		s := &c.snapshot
		if err := tx.QueryRow(ctx, `
			SELECT last_block, generation, (SELECT value FROM ledger_verify_state WHERE name = 'address_days_recount'), pruned_through()
			FROM daily_stats_watermark WHERE id = 1`).Scan(&s.watermark, &s.generation, &s.moves, &s.pruned); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fold_acc SELECT * FROM daily_stats_acc`); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(max(number), $1) FROM (
				SELECT number, row_number() OVER (ORDER BY number) AS rn
				FROM blocks WHERE number > $1 AND number <= $1 + $2
			) s WHERE number = $1 + rn`, s.watermark, limit).Scan(&c.upto); err != nil {
			return err
		}
		if c.upto > s.watermark {
			if err := stageAdvance(ctx, tx, s.watermark, c.upto); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `SELECT daily_stats_day(timestamp) - 1 FROM blocks WHERE number = $1`, c.upto).Scan(&c.keptFrom); err != nil {
				return err
			}
		}
		from, later, pending, err := recountDays(ctx, tx, late, c.upto, days)
		if err != nil {
			return err
		}
		c.later, c.pending, c.recounted = later, pending, from != nil
		if c.upto > s.watermark {
			spanned, err := firstOfSeveralDays(ctx, tx, s.watermark+1, c.upto)
			if err != nil {
				return err
			}
			if spanned != nil && (from == nil || *spanned < *from) {
				from = spanned
			}
		}
		if from != nil {
			if err := rewriteFrom(ctx, tx, stagedDays, *from, c.upto); err != nil {
				return err
			}
		}
		return stageRecountedSets(ctx, tx, c.upto)
	})
	return c, err
}

// stageRecountedSets turns the address sets of the recounted days (fold_reset) into what the apply changes in
// daily_active_addresses, so that the work under (4670, 1) is the difference, not the sets. Only blocks above the
// watermark read a day's set, and they are on the watermark's day or later: a recounted day before it keeps the set
// it has; on the watermark's day the addresses already in its set are not added again (fold_active), and those no
// longer in the day's blocks are taken out (fold_gone).
func stageRecountedSets(ctx context.Context, tx pgx.Tx, watermark int64) error {
	var day *time.Time
	if err := tx.QueryRow(ctx, `SELECT (SELECT daily_stats_day(timestamp) FROM blocks WHERE number = $1)`, watermark).Scan(&day); err != nil {
		return err
	}
	for _, q := range []string{`
		DELETE FROM fold_active f USING fold_reset r WHERE f.date = r.date AND f.date < $1`, `
		INSERT INTO fold_gone (date, address)
		SELECT l.date, l.address FROM daily_active_addresses l JOIN fold_reset r USING (date)
		WHERE l.date >= $1 AND NOT EXISTS (SELECT 1 FROM fold_active f WHERE f.date = l.date AND f.address = l.address)`, `
		DELETE FROM fold_active f USING fold_reset r, daily_active_addresses l
		WHERE f.date = r.date AND f.date >= $1 AND l.date = f.date AND l.address = f.address`,
	} {
		if _, err := tx.Exec(ctx, q, day); err != nil {
			return err
		}
	}
	return nil
}

// stageAdvance stages what daily_stats_advance (021) writes for the blocks above wm through upto: their sums added
// to their days, each day's addresses not in its set yet, the transactions before each new day, and daily_stats
// from the first block's day on.
func stageAdvance(ctx context.Context, tx pgx.Tx, wm, upto int64) error {
	for _, q := range []string{`
		INSERT INTO fold_acc AS a (date, first_block, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n)
		SELECT daily_stats_day(b.timestamp), min(b.number), count(*), sum(b.gas_used), COALESCE(sum(b.size), 0), count(b.size),
		       COALESCE(sum(b.timestamp - p.timestamp), 0), count(p.number)
		FROM blocks b LEFT JOIN blocks p ON p.number = b.number - 1
		WHERE b.number > $1 AND b.number <= $2
		GROUP BY 1
		ON CONFLICT (date) DO UPDATE SET
			first_block = LEAST(a.first_block, EXCLUDED.first_block),
			blocks = a.blocks + EXCLUDED.blocks,
			gas_used = a.gas_used + EXCLUDED.gas_used,
			size_sum = a.size_sum + EXCLUDED.size_sum,
			size_n = a.size_n + EXCLUDED.size_n,
			block_time_sum = a.block_time_sum + EXCLUDED.block_time_sum,
			block_time_n = a.block_time_n + EXCLUDED.block_time_n`, `
		INSERT INTO fold_acc AS a (date, txs, gas_price_sum, successful_txs, failed_txs)
		SELECT daily_stats_day(b.timestamp), count(*), sum(t.gas_price),
		       count(*) FILTER (WHERE t.status = 1), count(*) FILTER (WHERE t.status = 0)
		FROM transactions t JOIN blocks b ON b.number = t.block_number
		WHERE t.block_number > $1 AND t.block_number <= $2
		GROUP BY 1
		ON CONFLICT (date) DO UPDATE SET
			txs = a.txs + EXCLUDED.txs,
			gas_price_sum = a.gas_price_sum + EXCLUDED.gas_price_sum,
			successful_txs = a.successful_txs + EXCLUDED.successful_txs,
			failed_txs = a.failed_txs + EXCLUDED.failed_txs`, `
		WITH fresh AS (
			INSERT INTO fold_active (date, address)
			SELECT DISTINCT daily_stats_day(b.timestamp), x.address
			FROM transactions t JOIN blocks b ON b.number = t.block_number
			CROSS JOIN LATERAL (VALUES (t.from_address), (t.to_address)) AS x(address)
			WHERE t.block_number > $1 AND t.block_number <= $2 AND x.address IS NOT NULL
			  AND NOT EXISTS (SELECT 1 FROM daily_active_addresses l
			                  WHERE l.date = daily_stats_day(b.timestamp) AND l.address = x.address)
			ON CONFLICT DO NOTHING
			RETURNING date
		)
		INSERT INTO fold_acc AS a (date, active_addresses)
		SELECT date, count(*) FROM fresh GROUP BY 1
		ON CONFLICT (date) DO UPDATE SET active_addresses = a.active_addresses + EXCLUDED.active_addresses`, `
		INSERT INTO fold_acc AS a (date, token_transfers)
		SELECT daily_stats_day(b.timestamp), count(*)
		FROM token_transfers tt JOIN blocks b ON b.number = tt.block_number
		WHERE tt.block_number > $1 AND tt.block_number <= $2
		GROUP BY 1
		ON CONFLICT (date) DO UPDATE SET token_transfers = a.token_transfers + EXCLUDED.token_transfers`,
	} {
		if _, err := tx.Exec(ctx, q, wm, upto); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fold_acc a SET txs_before = COALESCE(
			(SELECT p.txs_before + p.txs FROM fold_acc p
			 WHERE p.date < a.date AND p.txs_before IS NOT NULL ORDER BY p.date DESC LIMIT 1),
			(SELECT count(*) FROM transactions t JOIN blocks b ON t.block_number = b.number
			 WHERE b.timestamp < daily_stats_epoch(a.date) AND t.block_number >= (SELECT history_from()))
			+ prune_offset('chain', 'transactions_total'))
		WHERE a.txs_before IS NULL`); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(writeDaysSQL, stagedDays.acc, stagedDays.stats, "a.date >= (SELECT daily_stats_day(timestamp) FROM blocks WHERE number = $1 + 1)"),
		wm, upto)
	return err
}

// lockedFold is the fold as it ran before PRST-5627, under (4670, 1) throughout: daily_stats_advance over at most
// limit blocks and the days it spans rewritten, without a day recount.
func lockedFold(ctx context.Context, conn *pgxpool.Conn, limit int64) (*int64, error) {
	var upto *int64
	err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		var before *int64
		if err := tx.QueryRow(ctx, `
			SELECT (SELECT last_block FROM daily_stats_watermark WHERE id = 1)
			FROM pg_advisory_xact_lock(4670, 1)`).Scan(&before); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT daily_stats_advance($1)`, limit).Scan(&upto); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, bumpDailyGenerationSQL); err != nil {
			return err
		}
		if upto == nil || before == nil || *upto <= *before {
			return nil
		}
		spanned, err := firstOfSeveralDays(ctx, tx, *before+1, *upto)
		if err != nil || spanned == nil {
			return err
		}
		return rewriteFrom(ctx, tx, liveDays, *spanned, *upto)
	})
	return upto, err
}

// searchLateDays lists the days whose blocks up to the watermark are not all counted: a day aggregated with fewer
// blocks or block-time pairs than are stored, or a day with stored blocks and no row at all. A day without an
// aggregate whose row counts its stored blocks was written before the incremental path and is left alone. It runs
// without the advance's lock, before the first watermark exists not at all (ok false).
func (d *DB) searchLateDays(ctx context.Context) (days []int64, ok bool, err error) {
	var watermark *int64
	if err := d.pool.QueryRow(ctx, `SELECT last_block FROM daily_stats_watermark WHERE id = 1`).Scan(&watermark); err != nil &&
		!errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	if watermark == nil {
		return nil, false, nil
	}
	rows, err := d.pool.Query(ctx, `
		WITH stored AS (
			SELECT daily_stats_day(b.timestamp) AS date, count(*) AS blocks, count(p.number) AS pairs
			FROM blocks b LEFT JOIN blocks p ON p.number = b.number - 1
			WHERE b.number <= $1
			GROUP BY 1
		)
		SELECT s.date - DATE '1970-01-01'
		FROM stored s LEFT JOIN daily_stats_acc a USING (date) LEFT JOIN daily_stats d USING (date)
		WHERE CASE WHEN a.date IS NOT NULL THEN a.blocks <> s.blocks OR a.block_time_n <> s.pairs
		           ELSE d.date IS NULL OR d.total_blocks <> s.blocks END`, *watermark)
	if err != nil {
		return nil, false, err
	}
	days, err = pgx.CollectRows(rows, pgx.RowTo[int64])
	return days, err == nil, err
}

// recountDays stages the recount of the first of the given days whose blocks up to the watermark are complete, from
// those blocks: the sums daily_stats_advance would have built had every block arrived in order, and its address set.
// A day's blocks are complete when none is missing between its first and last, the block before its first is stored
// (or none below it is), and the block after its last is stored or is the watermark. The day of that next block is
// recounted with it, for the block-time pair across the boundary, and so on while that day is complete too. A day
// older than every aggregated day takes the days up to the first aggregated one with it, so that each day's count of
// earlier transactions follows from the aggregated day before it. A day whose first blocks are pruned cannot be
// recounted and keeps its row. It returns the earliest day recounted, the complete days left for a later count, and
// the days left until their blocks are complete.
func recountDays(ctx context.Context, tx pgx.Tx, late *lateDays, watermark int64, days []int64) (from *int64, later, pending []int64, err error) {
	var partlyPruned *int64
	if err := tx.QueryRow(ctx, `
		SELECT CASE WHEN pruned_through() > 0
		            THEN (SELECT timestamp / 86400 FROM blocks WHERE number >= history_from() ORDER BY number LIMIT 1) END`).
		Scan(&partlyPruned); err != nil {
		return nil, nil, nil, err
	}
	if partlyPruned != nil {
		days = slices.DeleteFunc(slices.Clone(days), func(day int64) bool { return day <= *partlyPruned })
	}
	if len(days) == 0 {
		return nil, nil, nil, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT d.day, s.n > 0 AND s.n = s.hi - s.lo + 1
		              AND (s.lo = (SELECT min(number) FROM blocks) OR EXISTS (SELECT 1 FROM blocks WHERE number = s.lo - 1))
		              AND (s.hi = $2 OR EXISTS (SELECT 1 FROM blocks WHERE number = s.hi + 1)),
		       s.n = 0, (SELECT timestamp / 86400 FROM blocks WHERE number = s.hi + 1)
		FROM (SELECT DISTINCT unnest($1::BIGINT[]) AS day) d
		CROSS JOIN LATERAL (
			SELECT min(number) AS lo, max(number) AS hi, count(*) AS n FROM blocks
			WHERE timestamp >= d.day * 86400 AND timestamp < (d.day + 1) * 86400 AND number <= $2
		) s
		ORDER BY d.day`, days, watermark)
	if err != nil {
		return nil, nil, nil, err
	}
	var complete []int64
	next := map[int64]int64{}
	for rows.Next() {
		var day int64
		var whole, empty bool
		var after *int64
		if err := rows.Scan(&day, &whole, &empty, &after); err != nil {
			rows.Close()
			return nil, nil, nil, err
		}
		switch {
		case whole:
			complete = append(complete, day)
			if after != nil && *after != day {
				next[day] = *after
			}
		case !empty:
			pending = append(pending, day)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(complete) == 0 {
		return nil, nil, pending, err
	}
	first := complete[0]
	group := map[int64]bool{first: true}
	var firstAggregated *int64
	if err := tx.QueryRow(ctx, `SELECT min(date) - DATE '1970-01-01' FROM fold_acc`).Scan(&firstAggregated); err != nil {
		return nil, nil, nil, err
	}
	if firstAggregated != nil {
		for day := first; day < *firstAggregated; day++ {
			group[day] = true
		}
	}
	for grew := true; grew; {
		grew = false
		for day := range group {
			if after, ok := next[day]; ok && !group[after] {
				group[after], grew = true, true
			}
		}
	}
	recounted := slices.Sorted(maps.Keys(group))
	for _, day := range recounted {
		if err := recountDay(ctx, tx, watermark, day); err != nil {
			return nil, nil, nil, err
		}
		late.recounts.Add(1)
	}
	for _, day := range complete {
		if !group[day] {
			later = append(later, day)
		}
	}
	return &first, later, pending, nil
}

// firstOfSeveralDays is the day of block first when blocks first to last span more than one day: 012 sets a new
// day's count of earlier transactions from the day before only if that day had one before the advance, so the second
// of two days one advance creates misses the first's transactions.
func firstOfSeveralDays(ctx context.Context, tx pgx.Tx, first, last int64) (*int64, error) {
	var firstDay, lastDay *int64
	if err := tx.QueryRow(ctx, `
		SELECT (SELECT timestamp / 86400 FROM blocks WHERE number = $1), (SELECT timestamp / 86400 FROM blocks WHERE number = $2)`,
		first, last).Scan(&firstDay, &lastDay); err != nil {
		return nil, err
	}
	if firstDay == nil || lastDay == nil || *firstDay == *lastDay {
		return nil, nil
	}
	return firstDay, nil
}

// writeDaysSQL writes daily_stats (%[2]s) from the aggregate (%[1]s) for the days the condition (%[3]s) selects:
// the block and transaction columns from the day's sums, and the address columns from address_stats, counted from
// the day's first block up to the next aggregated day's first, or to the watermark ($2) for the newest.
const writeDaysSQL = `
	INSERT INTO %[2]s AS d (date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
		successful_txs, failed_txs, active_addresses, new_addresses, avg_block_time, avg_block_size,
		new_contracts, token_transfer_count, cumulative_transactions, cumulative_addresses, cumulative_contracts)
	SELECT a.date, a.blocks, a.txs, a.gas_used::BIGINT,
	       COALESCE((a.gas_price_sum / NULLIF(a.txs, 0))::BIGINT, 0),
	       a.successful_txs, a.failed_txs, a.active_addresses, s.new_addresses,
	       COALESCE((a.block_time_sum / NULLIF(a.block_time_n, 0))::FLOAT, 0),
	       COALESCE((a.size_sum / NULLIF(a.size_n, 0))::BIGINT, 0),
	       s.new_contracts, a.token_transfers, (a.txs_before + a.txs)::BIGINT,
	       s.cumulative_addresses, s.cumulative_contracts
	FROM %[1]s a
	CROSS JOIN LATERAL (
		SELECT a.first_block AS lo,
		       COALESCE((SELECT n.first_block FROM %[1]s n WHERE n.date > a.date ORDER BY n.date LIMIT 1), $2 + 1) AS hi
	) span
	CROSS JOIN LATERAL (
		SELECT count(*) FILTER (WHERE first_seen >= span.lo AND first_seen < span.hi) AS new_addresses,
		       count(*) FILTER (WHERE is_contract = true AND first_seen >= span.lo AND first_seen < span.hi) AS new_contracts,
		       count(*) FILTER (WHERE first_seen < span.hi) AS cumulative_addresses,
		       count(*) FILTER (WHERE is_contract = true AND first_seen < span.hi) AS cumulative_contracts
		FROM address_stats
	) s
	WHERE %[3]s
	ON CONFLICT (date) DO UPDATE SET
		total_blocks = EXCLUDED.total_blocks,
		total_transactions = EXCLUDED.total_transactions,
		total_gas_used = EXCLUDED.total_gas_used,
		avg_gas_price = EXCLUDED.avg_gas_price,
		successful_txs = EXCLUDED.successful_txs,
		failed_txs = EXCLUDED.failed_txs,
		active_addresses = EXCLUDED.active_addresses,
		new_addresses = EXCLUDED.new_addresses,
		avg_block_time = EXCLUDED.avg_block_time,
		avg_block_size = EXCLUDED.avg_block_size,
		new_contracts = EXCLUDED.new_contracts,
		token_transfer_count = EXCLUDED.token_transfer_count,
		cumulative_transactions = EXCLUDED.cumulative_transactions,
		cumulative_addresses = EXCLUDED.cumulative_addresses,
		cumulative_contracts = EXCLUDED.cumulative_contracts`

// rewriteFrom recounts each aggregated day's transactions before it from the first given day on, and rewrites those
// days' daily_stats rows and the aggregated day's before them, whose addresses end at the first day's first block.
func rewriteFrom(ctx context.Context, tx pgx.Tx, t dailyTables, day, watermark int64) error {
	from := dayDate(day)
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		WITH counted AS (
			SELECT COALESCE(
				(SELECT p.txs_before + p.txs FROM %[1]s p WHERE p.date < $1::DATE ORDER BY p.date DESC LIMIT 1),
				(SELECT count(*) FROM transactions t JOIN blocks b ON t.block_number = b.number
				 WHERE b.timestamp < daily_stats_epoch($1::DATE) AND t.block_number >= history_from())
				+ prune_offset('chain', 'transactions_total')) AS before
		), running AS (
			SELECT date, (SELECT before FROM counted)
			             + COALESCE(sum(txs) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS before
			FROM %[1]s WHERE date >= $1::DATE
		)
		UPDATE %[1]s a SET txs_before = r.before FROM running r WHERE a.date = r.date`, t.acc), from); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(writeDaysSQL, t.acc, t.stats,
		fmt.Sprintf("a.date >= COALESCE((SELECT max(date) FROM %[1]s WHERE date < $1::DATE), $1::DATE)", t.acc)), from, watermark)
	return err
}

// recountDay stages a day's aggregate and address set from its blocks up to the watermark, and notes the day in
// fold_reset for stageRecountedSets.
func recountDay(ctx context.Context, tx pgx.Tx, watermark, day int64) error {
	date := dayDate(day)
	if _, err := tx.Exec(ctx, `DELETE FROM fold_active WHERE date = $1::DATE`, date); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fold_reset (date) VALUES ($1::DATE) ON CONFLICT DO NOTHING`, date); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fold_active (date, address)
		SELECT DISTINCT $1::DATE, x.address
		FROM transactions t JOIN blocks b ON b.number = t.block_number
		CROSS JOIN LATERAL (VALUES (t.from_address), (t.to_address)) AS x(address)
		WHERE b.timestamp >= daily_stats_epoch($1::DATE) AND b.timestamp < daily_stats_epoch($1::DATE + 1) AND b.number <= $2
		  AND x.address IS NOT NULL`, date, watermark); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		WITH day_blocks AS (
			SELECT b.number, b.timestamp, b.gas_used, b.size, b.timestamp - p.timestamp AS block_time, p.number AS previous
			FROM blocks b LEFT JOIN blocks p ON p.number = b.number - 1
			WHERE b.timestamp >= daily_stats_epoch($1::DATE) AND b.timestamp < daily_stats_epoch($1::DATE + 1) AND b.number <= $2
		), day_txs AS (
			SELECT count(*) AS txs, COALESCE(sum(t.gas_price), 0) AS gas_price_sum,
			       count(*) FILTER (WHERE t.status = 1) AS successful_txs, count(*) FILTER (WHERE t.status = 0) AS failed_txs
			FROM transactions t JOIN day_blocks b ON b.number = t.block_number
		)
		INSERT INTO fold_acc AS a (date, first_block, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n,
			txs, gas_price_sum, successful_txs, failed_txs, active_addresses, token_transfers, txs_before)
		SELECT $1::DATE, min(b.number), count(*), COALESCE(sum(b.gas_used), 0), COALESCE(sum(b.size), 0), count(b.size),
		       COALESCE(sum(b.block_time), 0), count(b.previous), x.txs, x.gas_price_sum, x.successful_txs, x.failed_txs,
		       (SELECT count(*) FROM fold_active WHERE date = $1::DATE),
		       (SELECT count(*) FROM token_transfers tt JOIN day_blocks d ON d.number = tt.block_number), NULL
		FROM day_blocks b CROSS JOIN day_txs x
		GROUP BY x.txs, x.gas_price_sum, x.successful_txs, x.failed_txs
		HAVING count(*) > 0
		ON CONFLICT (date) DO UPDATE SET
			first_block = EXCLUDED.first_block, blocks = EXCLUDED.blocks, gas_used = EXCLUDED.gas_used, size_sum = EXCLUDED.size_sum, size_n = EXCLUDED.size_n,
			block_time_sum = EXCLUDED.block_time_sum, block_time_n = EXCLUDED.block_time_n, txs = EXCLUDED.txs,
			gas_price_sum = EXCLUDED.gas_price_sum, successful_txs = EXCLUDED.successful_txs,
			failed_txs = EXCLUDED.failed_txs, active_addresses = EXCLUDED.active_addresses,
			token_transfers = EXCLUDED.token_transfers, txs_before = NULL`, date, watermark)
	return err
}

func dayDate(day int64) time.Time {
	return time.Unix(day*86400, 0).UTC()
}
