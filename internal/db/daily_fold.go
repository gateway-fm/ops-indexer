package db

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// lateDays remembers the days that may hold a block stored below the daily_stats watermark (migration 012), which
// daily_stats_advance never reaches: on a fresh sync realtime stores the head first and the first advance starts at
// the head's day, and catch-up stores the earlier blocks afterwards; an invalidated day restarts at its first stored
// block. Every block write notes its day, and the next day for its block-time pair, unless the block is above the
// watermark the last advance returned, where the advance itself counts it. A restart forgets the notes, so the first
// advance after one looks for such days in the stored blocks instead.
type lateDays struct {
	mu        sync.Mutex
	watermark *int64
	days      map[int64]struct{}
	searched  bool
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
	day := int64(timestamp) / 86400
	l.days[day] = struct{}{}
	l.days[day+1] = struct{}{}
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

// AdvanceDailyStats folds the blocks indexed since the last call into daily_stats (migration 012) and returns the
// block it aggregated through. Then, under the same lock, it recounts every day that took a block below that block
// (lateDays) from the stored blocks, so each block counts once whatever order it was stored in, and carries the
// count of earlier transactions through every later day.
func (d *DB) AdvanceDailyStats(ctx context.Context) (uint64, error) {
	days, search := d.lateDays.take()
	var upto *int64
	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT daily_stats_advance()`).Scan(&upto); err != nil {
			return err
		}
		if upto == nil {
			return nil
		}
		if search {
			found, err := searchLateDays(ctx, tx, *upto)
			if err != nil {
				return err
			}
			days = append(days, found...)
		}
		from, err := recountDays(ctx, tx, *upto, days)
		if err != nil {
			return err
		}
		if from == nil {
			return nil
		}
		return rewriteFrom(ctx, tx, *from)
	})
	if err != nil {
		d.lateDays.giveBack(days, search)
		return 0, err
	}
	if upto == nil {
		d.lateDays.giveBack(days, search)
		return 0, nil
	}
	d.lateDays.advanced(*upto, search)
	return uint64(*upto), nil
}

// searchLateDays lists the days whose blocks up to the watermark are not all counted: a day aggregated with fewer
// blocks or block-time pairs than are stored, or a day with stored blocks and no row at all. A day without an
// aggregate whose row counts its stored blocks was written before the incremental path and is left alone.
func searchLateDays(ctx context.Context, tx pgx.Tx, watermark int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `
		WITH stored AS (
			SELECT daily_stats_day(b.timestamp) AS date, count(*) AS blocks, count(p.number) AS pairs
			FROM blocks b LEFT JOIN blocks p ON p.number = b.number - 1
			WHERE b.number <= $1
			GROUP BY 1
		)
		SELECT s.date - DATE '1970-01-01'
		FROM stored s LEFT JOIN daily_stats_acc a USING (date) LEFT JOIN daily_stats d USING (date)
		WHERE CASE WHEN a.date IS NOT NULL THEN a.blocks <> s.blocks OR a.block_time_n <> s.pairs
		           ELSE d.date IS NULL OR d.total_blocks <> s.blocks END`, watermark)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// recountDays rewrites each given day's aggregate from its stored blocks up to the watermark, the sums
// daily_stats_advance would have built had every block arrived in order, and its address set, and returns the
// earliest day. A day older than every aggregated day takes the days up to the first aggregated one with it, so that
// each day's count of earlier transactions follows from the aggregated day before it.
func recountDays(ctx context.Context, tx pgx.Tx, watermark int64, days []int64) (*int64, error) {
	if len(days) == 0 {
		return nil, nil
	}
	sort.Slice(days, func(i, j int) bool { return days[i] < days[j] })
	var firstAggregated *int64
	if err := tx.QueryRow(ctx, `SELECT min(date) - DATE '1970-01-01' FROM daily_stats_acc`).Scan(&firstAggregated); err != nil {
		return nil, err
	}
	if firstAggregated != nil {
		for day := days[0]; day < *firstAggregated; day++ {
			days = append(days, day)
		}
	}
	recounted := map[int64]bool{}
	for _, day := range days {
		if recounted[day] {
			continue
		}
		recounted[day] = true
		if err := recountDay(ctx, tx, watermark, day); err != nil {
			return nil, err
		}
	}
	return &days[0], nil
}

// rewriteFrom recounts each aggregated day's transactions before it from the first given day on, and rewrites those
// days' daily_stats rows.
func rewriteFrom(ctx context.Context, tx pgx.Tx, day int64) error {
	from := dayDate(day)
	if _, err := tx.Exec(ctx, `
		WITH counted AS (
			SELECT COALESCE(
				(SELECT p.txs_before + p.txs FROM daily_stats_acc p WHERE p.date < $1::DATE ORDER BY p.date DESC LIMIT 1),
				(SELECT count(*) FROM transactions t JOIN blocks b ON t.block_number = b.number
				 WHERE b.timestamp < daily_stats_epoch($1::DATE))) AS before
		), running AS (
			SELECT date, (SELECT before FROM counted)
			             + COALESCE(sum(txs) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS before
			FROM daily_stats_acc WHERE date >= $1::DATE
		)
		UPDATE daily_stats_acc a SET txs_before = r.before FROM running r WHERE a.date = r.date`, from); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO daily_stats AS d (date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
			successful_txs, failed_txs, active_addresses, new_addresses, avg_block_time, avg_block_size,
			new_contracts, token_transfer_count, cumulative_transactions, cumulative_addresses, cumulative_contracts)
		SELECT a.date, a.blocks, a.txs, a.gas_used::BIGINT,
		       COALESCE((a.gas_price_sum / NULLIF(a.txs, 0))::BIGINT, 0),
		       a.successful_txs, a.failed_txs, a.active_addresses, s.new_addresses,
		       COALESCE((a.block_time_sum / NULLIF(a.block_time_n, 0))::FLOAT, 0),
		       COALESCE((a.size_sum / NULLIF(a.size_n, 0))::BIGINT, 0),
		       s.new_contracts, a.token_transfers, (a.txs_before + a.txs)::BIGINT,
		       s.cumulative_addresses, s.cumulative_contracts
		FROM daily_stats_acc a
		CROSS JOIN LATERAL (
			SELECT count(*) FILTER (WHERE first_seen >= daily_stats_epoch(a.date) AND first_seen < daily_stats_epoch(a.date + 1)) AS new_addresses,
			       count(*) FILTER (WHERE is_contract = true AND first_seen >= daily_stats_epoch(a.date)
			                          AND first_seen < daily_stats_epoch(a.date + 1)) AS new_contracts,
			       count(*) FILTER (WHERE first_seen < daily_stats_epoch(a.date + 1)) AS cumulative_addresses,
			       count(*) FILTER (WHERE is_contract = true AND first_seen < daily_stats_epoch(a.date + 1)) AS cumulative_contracts
			FROM address_stats
		) s
		WHERE a.date >= $1::DATE
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
			cumulative_contracts = EXCLUDED.cumulative_contracts`, from)
	return err
}

func recountDay(ctx context.Context, tx pgx.Tx, watermark, day int64) error {
	date := dayDate(day)
	if _, err := tx.Exec(ctx, `DELETE FROM daily_active_addresses WHERE date = $1::DATE`, date); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO daily_active_addresses (date, address)
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
		INSERT INTO daily_stats_acc AS a (date, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n, txs,
			gas_price_sum, successful_txs, failed_txs, active_addresses, token_transfers, txs_before)
		SELECT $1::DATE, count(*), COALESCE(sum(b.gas_used), 0), COALESCE(sum(b.size), 0), count(b.size),
		       COALESCE(sum(b.block_time), 0), count(b.previous), x.txs, x.gas_price_sum, x.successful_txs, x.failed_txs,
		       (SELECT count(*) FROM daily_active_addresses WHERE date = $1::DATE),
		       (SELECT count(*) FROM token_transfers tt JOIN day_blocks d ON d.number = tt.block_number), NULL
		FROM day_blocks b CROSS JOIN day_txs x
		GROUP BY x.txs, x.gas_price_sum, x.successful_txs, x.failed_txs
		HAVING count(*) > 0
		ON CONFLICT (date) DO UPDATE SET
			blocks = EXCLUDED.blocks, gas_used = EXCLUDED.gas_used, size_sum = EXCLUDED.size_sum, size_n = EXCLUDED.size_n,
			block_time_sum = EXCLUDED.block_time_sum, block_time_n = EXCLUDED.block_time_n, txs = EXCLUDED.txs,
			gas_price_sum = EXCLUDED.gas_price_sum, successful_txs = EXCLUDED.successful_txs,
			failed_txs = EXCLUDED.failed_txs, active_addresses = EXCLUDED.active_addresses,
			token_transfers = EXCLUDED.token_transfers, txs_before = NULL`, date, watermark)
	return err
}

func dayDate(day int64) time.Time {
	return time.Unix(day*86400, 0).UTC()
}
