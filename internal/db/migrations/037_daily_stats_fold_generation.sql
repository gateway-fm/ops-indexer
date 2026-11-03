-- The daily-stats fold counts without (4670, 1) (PRST-5627). It counts in one snapshot into its session's staging
-- tables, then takes the lock only to write them, and only if no writer of the daily tables committed since its
-- snapshot: every transaction that writes daily_stats, daily_stats_acc, daily_active_addresses or the watermark
-- under (4670, 1) moves the generation. WipeAllData's truncate sets it back to 0, which no fold can see: the wipe
-- runs at start, before the fold.

ALTER TABLE daily_stats_watermark ADD COLUMN generation BIGINT NOT NULL DEFAULT 0;

-- 015's, moving the generation first: a revert of a block above the watermark returns early, and the fold may have
-- counted that block.
CREATE OR REPLACE FUNCTION daily_stats_invalidate(from_block BIGINT) RETURNS VOID LANGUAGE plpgsql AS $f$
DECLARE
    d DATE;
BEGIN
    PERFORM pg_advisory_xact_lock(4670, 1);
    UPDATE daily_stats_watermark SET generation = generation + 1 WHERE id = 1;
    IF NOT EXISTS (SELECT 1 FROM daily_stats_watermark WHERE id = 1 AND last_block >= from_block) THEN
        RETURN;
    END IF;
    SELECT daily_stats_day(timestamp) INTO d FROM blocks WHERE number = from_block;
    IF d IS NULL THEN
        RETURN;
    END IF;
    IF daily_stats_epoch(d) <= (SELECT pruned_through_ts FROM prune_state WHERE id = 1) THEN
        RAISE EXCEPTION 'daily_stats_invalidate: block % is on % whose first blocks are pruned', from_block, d;
    END IF;
    DELETE FROM daily_stats_acc WHERE date >= d;
    DELETE FROM daily_active_addresses WHERE date >= d;
    UPDATE daily_stats_watermark
    SET last_block = (SELECT min(number) - 1 FROM blocks WHERE timestamp >= daily_stats_epoch(d))
    WHERE id = 1;
END
$f$;

-- Writes a fold staged in this session's fold_acc, fold_active, fold_reset and fold_stats, if the watermark, the
-- generation, the pending first_seen moves and the pruned height are still those its snapshot read. The address
-- sets of days before kept_from are dropped, as daily_stats_advance drops them; kept_from is NULL when the
-- watermark does not move.
CREATE FUNCTION daily_stats_fold_apply(wm BIGINT, gen BIGINT, moves BIGINT, pruned BIGINT, upto BIGINT, kept_from DATE)
RETURNS BOOLEAN LANGUAGE plpgsql AS $f$
BEGIN
    PERFORM pg_advisory_xact_lock(4670, 1);
    IF NOT EXISTS (SELECT 1 FROM daily_stats_watermark WHERE id = 1 AND last_block = wm AND generation = gen)
       OR (SELECT value FROM ledger_verify_state WHERE name = 'address_days_recount') IS DISTINCT FROM moves
       OR pruned_through() <> pruned THEN
        RETURN false;
    END IF;

    INSERT INTO daily_stats_acc AS a (date, first_block, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n,
        txs, gas_price_sum, successful_txs, failed_txs, active_addresses, token_transfers, txs_before)
    SELECT f.date, f.first_block, f.blocks, f.gas_used, f.size_sum, f.size_n, f.block_time_sum, f.block_time_n,
           f.txs, f.gas_price_sum, f.successful_txs, f.failed_txs, f.active_addresses, f.token_transfers, f.txs_before
    FROM fold_acc f
    WHERE NOT EXISTS (
        SELECT 1 FROM daily_stats_acc l
        WHERE l.date = f.date
          AND (l.first_block, l.blocks, l.gas_used, l.size_sum, l.size_n, l.block_time_sum, l.block_time_n, l.txs,
               l.gas_price_sum, l.successful_txs, l.failed_txs, l.active_addresses, l.token_transfers, l.txs_before)
              IS NOT DISTINCT FROM
              (f.first_block, f.blocks, f.gas_used, f.size_sum, f.size_n, f.block_time_sum, f.block_time_n, f.txs,
               f.gas_price_sum, f.successful_txs, f.failed_txs, f.active_addresses, f.token_transfers, f.txs_before))
    ON CONFLICT (date) DO UPDATE SET
        first_block = EXCLUDED.first_block, blocks = EXCLUDED.blocks, gas_used = EXCLUDED.gas_used,
        size_sum = EXCLUDED.size_sum, size_n = EXCLUDED.size_n, block_time_sum = EXCLUDED.block_time_sum,
        block_time_n = EXCLUDED.block_time_n, txs = EXCLUDED.txs, gas_price_sum = EXCLUDED.gas_price_sum,
        successful_txs = EXCLUDED.successful_txs, failed_txs = EXCLUDED.failed_txs,
        active_addresses = EXCLUDED.active_addresses, token_transfers = EXCLUDED.token_transfers,
        txs_before = EXCLUDED.txs_before;

    DELETE FROM daily_active_addresses WHERE date IN (SELECT date FROM fold_reset) OR date < kept_from;
    INSERT INTO daily_active_addresses (date, address)
    SELECT date, address FROM fold_active WHERE kept_from IS NULL OR date >= kept_from
    ON CONFLICT DO NOTHING;

    INSERT INTO daily_stats AS d (date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
        successful_txs, failed_txs, active_addresses, new_addresses, avg_block_time, avg_block_size,
        new_contracts, token_transfer_count, cumulative_transactions, cumulative_addresses, cumulative_contracts)
    SELECT date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
           successful_txs, failed_txs, active_addresses, new_addresses, avg_block_time, avg_block_size,
           new_contracts, token_transfer_count, cumulative_transactions, cumulative_addresses, cumulative_contracts
    FROM fold_stats
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
        cumulative_contracts = EXCLUDED.cumulative_contracts;

    UPDATE daily_stats_watermark SET last_block = upto, generation = generation + 1 WHERE id = 1;
    RETURN true;
END
$f$;

---- create above / drop below ----

-- Each down runs in its own transaction, so a rollback past an unfinished stats backfill is refused here, at the
-- newest migration's down, before any down drops what the backfill needs (028's down repeats the check). A newer
-- migration takes this guard into its own down.
DO $guard$
BEGIN
    IF EXISTS (SELECT 1 FROM stats_backfill WHERE completed_at IS NULL) OR EXISTS (SELECT 1 FROM stats_backfill_balance_keys) THEN
        RAISE EXCEPTION 'schema rollback: the stats backfill has not finished (stats_backfill.completed_at is NULL, or balances it touched are still to be read: stats_backfill_balance_keys has rows), and the downs would drop its state, its span in progress and its balance keys. Let the backfill finish on this release and roll back then, or restore the pre-upgrade backup.';
    END IF;
END
$guard$;

DROP FUNCTION daily_stats_fold_apply(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, DATE);

CREATE OR REPLACE FUNCTION daily_stats_invalidate(from_block BIGINT) RETURNS VOID LANGUAGE plpgsql AS $f$
DECLARE
    d DATE;
BEGIN
    PERFORM pg_advisory_xact_lock(4670, 1);
    IF NOT EXISTS (SELECT 1 FROM daily_stats_watermark WHERE id = 1 AND last_block >= from_block) THEN
        RETURN;
    END IF;
    SELECT daily_stats_day(timestamp) INTO d FROM blocks WHERE number = from_block;
    IF d IS NULL THEN
        RETURN;
    END IF;
    IF daily_stats_epoch(d) <= (SELECT pruned_through_ts FROM prune_state WHERE id = 1) THEN
        RAISE EXCEPTION 'daily_stats_invalidate: block % is on % whose first blocks are pruned', from_block, d;
    END IF;
    DELETE FROM daily_stats_acc WHERE date >= d;
    DELETE FROM daily_active_addresses WHERE date >= d;
    UPDATE daily_stats_watermark
    SET last_block = (SELECT min(number) - 1 FROM blocks WHERE timestamp >= daily_stats_epoch(d))
    WHERE id = 1;
END
$f$;

ALTER TABLE daily_stats_watermark DROP COLUMN generation;
