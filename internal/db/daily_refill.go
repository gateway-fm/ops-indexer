package db

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// RefillDay is one day's block-level sums, counted from the node, the way daily_stats_advance counts stored blocks.
// The optional parts are nil when the refill did not fetch what they need: they then keep the stored values.
type RefillDay struct {
	Date                     time.Time
	FirstBlock, LastBlock    uint64
	Blocks, Txs              int64
	GasUsed, SizeSum         int64
	SizeN                    int64
	BlockTimeSum, BlockTimeN int64
	GasPriceSum              *big.Int
	ActiveAddresses          *int64
	SuccessfulTxs, FailedTxs *int64
	TokenTransfers           *int64
}

// DailyStatsRefillTable keeps the days a refill has counted, so a stopped refill resumes without the node.
const DailyStatsRefillTable = "daily_stats_refill"

const refillColumns = `date DATE PRIMARY KEY, first_block BIGINT NOT NULL, last_block BIGINT NOT NULL,
	blocks BIGINT NOT NULL, txs BIGINT NOT NULL, gas_used BIGINT NOT NULL, size_sum BIGINT NOT NULL, size_n BIGINT NOT NULL,
	block_time_sum BIGINT NOT NULL, block_time_n BIGINT NOT NULL, gas_price_sum NUMERIC, active_addresses BIGINT,
	successful_txs BIGINT, failed_txs BIGINT, token_transfers BIGINT`

func refillArgs(r RefillDay) []any {
	var gasPrice *string
	if r.GasPriceSum != nil {
		s := r.GasPriceSum.String()
		gasPrice = &s
	}
	return []any{r.Date, int64(r.FirstBlock), int64(r.LastBlock), r.Blocks, r.Txs, r.GasUsed, r.SizeSum, r.SizeN,
		r.BlockTimeSum, r.BlockTimeN, gasPrice, r.ActiveAddresses, r.SuccessfulTxs, r.FailedTxs, r.TokenTransfers}
}

const insertRefillSQL = ` (date, first_block, last_block, blocks, txs, gas_used, size_sum, size_n, block_time_sum,
	block_time_n, gas_price_sum, active_addresses, successful_txs, failed_txs, token_transfers)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::NUMERIC, $12, $13, $14, $15)`

// SaveRefillDay checkpoints a counted day.
func (d *DB) SaveRefillDay(ctx context.Context, r RefillDay) error {
	if _, err := d.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+DailyStatsRefillTable+` (`+refillColumns+`)`); err != nil {
		return err
	}
	_, err := d.pool.Exec(ctx, `DELETE FROM `+DailyStatsRefillTable+` WHERE date = $1`, r.Date)
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx, `INSERT INTO `+DailyStatsRefillTable+insertRefillSQL, refillArgs(r)...)
	return err
}

// SavedRefillDay is a checkpointed day, if any; full reports whether it has every optional part.
func (d *DB) SavedRefillDay(ctx context.Context, date time.Time) (saved, bodies, receipts bool, err error) {
	var exists bool
	if err := d.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, DailyStatsRefillTable).Scan(&exists); err != nil || !exists {
		return false, false, false, err
	}
	err = d.pool.QueryRow(ctx, `SELECT true, gas_price_sum IS NOT NULL, successful_txs IS NOT NULL FROM `+DailyStatsRefillTable+` WHERE date = $1`, date).
		Scan(&saved, &bodies, &receipts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, false, nil
	}
	return saved, bodies, receipts, err
}

// SavedRefillDays are the checkpointed days, in date order.
func (d *DB) SavedRefillDays(ctx context.Context) ([]RefillDay, error) {
	var exists bool
	if err := d.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, DailyStatsRefillTable).Scan(&exists); err != nil || !exists {
		return nil, err
	}
	rows, err := d.pool.Query(ctx, `SELECT date, first_block, last_block, blocks, txs, gas_used, size_sum, size_n, block_time_sum,
		block_time_n, gas_price_sum::TEXT, active_addresses, successful_txs, failed_txs, token_transfers FROM `+DailyStatsRefillTable+` ORDER BY date`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RefillDay
	for rows.Next() {
		var r RefillDay
		var first, last int64
		var gasPrice *string
		if err := rows.Scan(&r.Date, &first, &last, &r.Blocks, &r.Txs, &r.GasUsed, &r.SizeSum, &r.SizeN, &r.BlockTimeSum,
			&r.BlockTimeN, &gasPrice, &r.ActiveAddresses, &r.SuccessfulTxs, &r.FailedTxs, &r.TokenTransfers); err != nil {
			return nil, err
		}
		r.FirstBlock, r.LastBlock = uint64(first), uint64(last)
		if gasPrice != nil {
			r.GasPriceSum, _ = new(big.Int).SetString(*gasPrice, 10)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

var errRefillDryRun = errors.New("dry run")

// WriteRefilledDailyStats writes the refilled days into daily_stats, under daily_stats_advance's lock, in one
// transaction, and returns every daily_stats row from the first refilled day on as written. Dry, it rolls back.
//
//   - A refilled day takes its block-level columns from the node's counts, with daily_stats_advance's formulas. A
//     part the refill did not fetch (gas price, active addresses, success/failure, token transfers) keeps the stored
//     value.
//   - A refilled day's sums and first block also go into daily_stats_acc, which records every day's first block from
//     then on (a day the first run cut held a wrong one there).
//   - Every day in daily_stats_acc gets its address and contract columns from address_stats by its first block, as
//     daily_stats_advance does: new_* in [first, the next recorded day's first), cumulative_* below that; the
//     newest day ends at the watermark.
//   - cumulative_transactions runs backwards from the transactions_total counter: the newest aggregated day ends at
//     the counter minus the stored transactions above the watermark, and each earlier consecutive day at the next
//     one's minus its transactions. daily_stats_acc.txs_before follows, so later advances continue from it.
func (d *DB) WriteRefilledDailyStats(ctx context.Context, days []RefillDay, dry bool) ([]types.DailyStats, error) {
	var out []types.DailyStats
	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE refill_days (`+refillColumns+`) ON COMMIT DROP`); err != nil {
			return err
		}
		for _, r := range days {
			if _, err := tx.Exec(ctx, `INSERT INTO refill_days`+insertRefillSQL, refillArgs(r)...); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO daily_stats AS s (date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
				successful_txs, failed_txs, active_addresses, avg_block_time, avg_block_size, token_transfer_count)
			SELECT r.date, r.blocks, r.txs, r.gas_used,
			       COALESCE((r.gas_price_sum / NULLIF(r.txs, 0))::BIGINT, CASE WHEN r.gas_price_sum IS NOT NULL THEN 0 END, d.avg_gas_price, 0),
			       COALESCE(r.successful_txs, d.successful_txs, 0), COALESCE(r.failed_txs, d.failed_txs, 0),
			       COALESCE(r.active_addresses, d.active_addresses, 0),
			       COALESCE((r.block_time_sum::NUMERIC / NULLIF(r.block_time_n, 0))::FLOAT, 0),
			       COALESCE((r.size_sum::NUMERIC / NULLIF(r.size_n, 0))::BIGINT, 0),
			       COALESCE(r.token_transfers, d.token_transfer_count, 0)
			FROM refill_days r LEFT JOIN daily_stats d USING (date)
			ON CONFLICT (date) DO UPDATE SET
				total_blocks = EXCLUDED.total_blocks, total_transactions = EXCLUDED.total_transactions,
				total_gas_used = EXCLUDED.total_gas_used, avg_gas_price = EXCLUDED.avg_gas_price,
				successful_txs = EXCLUDED.successful_txs, failed_txs = EXCLUDED.failed_txs,
				active_addresses = EXCLUDED.active_addresses, avg_block_time = EXCLUDED.avg_block_time,
				avg_block_size = EXCLUDED.avg_block_size, token_transfer_count = EXCLUDED.token_transfer_count`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO daily_stats_acc AS a (date, first_block, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n,
				txs, gas_price_sum, successful_txs, failed_txs, active_addresses, token_transfers)
			SELECT r.date, r.first_block, r.blocks, r.gas_used, r.size_sum, r.size_n, r.block_time_sum, r.block_time_n, r.txs,
			       COALESCE(r.gas_price_sum, o.gas_price_sum, 0), COALESCE(r.successful_txs, o.successful_txs, 0),
			       COALESCE(r.failed_txs, o.failed_txs, 0), COALESCE(r.active_addresses, o.active_addresses, 0),
			       COALESCE(r.token_transfers, o.token_transfers, 0)
			FROM refill_days r LEFT JOIN daily_stats_acc o USING (date)
			ON CONFLICT (date) DO UPDATE SET
				first_block = EXCLUDED.first_block, blocks = EXCLUDED.blocks, gas_used = EXCLUDED.gas_used,
				size_sum = EXCLUDED.size_sum, size_n = EXCLUDED.size_n, block_time_sum = EXCLUDED.block_time_sum,
				block_time_n = EXCLUDED.block_time_n, txs = EXCLUDED.txs, gas_price_sum = EXCLUDED.gas_price_sum,
				successful_txs = EXCLUDED.successful_txs, failed_txs = EXCLUDED.failed_txs,
				active_addresses = EXCLUDED.active_addresses, token_transfers = EXCLUDED.token_transfers`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			WITH firsts AS (
				SELECT date, first_block FROM daily_stats_acc WHERE first_block IS NOT NULL
			), spans AS (
				SELECT date, first_block AS lo,
				       COALESCE(lead(first_block) OVER (ORDER BY date), (SELECT last_block + 1 FROM daily_stats_watermark WHERE id = 1)) AS hi
				FROM firsts
			), counted AS (
				SELECT sp.date, c.*
				FROM spans sp
				CROSS JOIN LATERAL (
					SELECT count(*) FILTER (WHERE first_seen >= sp.lo AND first_seen < sp.hi) AS new_addresses,
					       count(*) FILTER (WHERE is_contract AND first_seen >= sp.lo AND first_seen < sp.hi) AS new_contracts,
					       count(*) FILTER (WHERE first_seen < sp.hi) AS cumulative_addresses,
					       count(*) FILTER (WHERE is_contract AND first_seen < sp.hi) AS cumulative_contracts
					FROM address_stats
				) c
				WHERE sp.hi IS NOT NULL
			)
			UPDATE daily_stats s SET new_addresses = c.new_addresses, new_contracts = c.new_contracts,
				cumulative_addresses = c.cumulative_addresses, cumulative_contracts = c.cumulative_contracts
			FROM counted c WHERE s.date = c.date`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			CREATE TEMP TABLE refill_ends ON COMMIT DROP AS
			WITH RECURSIVE ends AS (
				SELECT s.date, s.total_transactions,
				       (SELECT count FROM chain_counters WHERE name = 'transactions_total')
				       - (SELECT count(*) FROM transactions
				          WHERE block_number > COALESCE((SELECT last_block FROM daily_stats_watermark WHERE id = 1), -1)) AS cumulative
				FROM daily_stats s WHERE s.date = (SELECT max(date) FROM daily_stats)
				UNION ALL
				SELECT p.date, p.total_transactions, e.cumulative - e.total_transactions
				FROM ends e JOIN daily_stats p ON p.date = e.date - 1
			)
			SELECT date, total_transactions, cumulative FROM ends`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE daily_stats s SET cumulative_transactions = e.cumulative FROM refill_ends e WHERE s.date = e.date`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE daily_stats_acc a SET txs_before = e.cumulative - e.total_transactions FROM refill_ends e WHERE a.date = e.date`); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT date, total_blocks, total_transactions, total_gas_used, avg_gas_price, successful_txs, failed_txs,
			       active_addresses, new_addresses, avg_block_time, avg_block_size, new_contracts, token_transfer_count,
			       cumulative_transactions, cumulative_addresses, cumulative_contracts
			FROM daily_stats WHERE date >= (SELECT min(date) FROM refill_days) ORDER BY date`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var s types.DailyStats
			var date time.Time
			if err := rows.Scan(&date, &s.TotalBlocks, &s.TotalTransactions, &s.TotalGasUsed, &s.AvgGasPrice, &s.SuccessfulTxs,
				&s.FailedTxs, &s.ActiveAddresses, &s.NewAddresses, &s.AvgBlockTime, &s.AvgBlockSize, &s.NewContracts,
				&s.TokenTransferCount, &s.CumulativeTransactions, &s.CumulativeAddresses, &s.CumulativeContracts); err != nil {
				return err
			}
			s.Date = date.Format("2006-01-02")
			out = append(out, s)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if dry {
			return errRefillDryRun
		}
		return nil
	})
	if errors.Is(err, errRefillDryRun) {
		err = nil
	}
	return out, err
}
