-- daily_stats counts an address on the day of the block it was first seen in.
-- address_stats.first_seen is a block number; the daily columns compared it with epoch seconds, so new_addresses and
-- new_contracts were 0 on every day and cumulative_* counted every address. A day's addresses are those first seen
-- from its first block up to the next day's first block (daily_stats_acc.first_block, recorded while the blocks
-- exist). Existing rows are corrected from the stored blocks, so this refuses once anything is pruned.

DO $m$
BEGIN
    IF pruned_through() > 0 THEN
        RAISE EXCEPTION 'migration 021 (daily_stats address columns) needs every block, but history at or below block % is pruned', pruned_through();
    END IF;
END
$m$;

ALTER TABLE daily_stats_acc ADD COLUMN first_block BIGINT;

CREATE TEMP TABLE daily_spans ON COMMIT DROP AS
SELECT date, lo, COALESCE(lead(lo) OVER (ORDER BY date), (SELECT max(number) + 1 FROM blocks)) AS hi
FROM (SELECT daily_stats_day(timestamp) AS date, min(number) AS lo FROM blocks GROUP BY 1) f;
CREATE UNIQUE INDEX ON daily_spans (lo);

UPDATE daily_stats_acc a SET first_block = s.lo FROM daily_spans s WHERE s.date = a.date;

WITH firsts AS (
    SELECT s.date, count(*) AS n, count(*) FILTER (WHERE a.is_contract) AS c
    FROM address_stats a
    CROSS JOIN LATERAL (SELECT date FROM daily_spans WHERE lo <= a.first_seen ORDER BY lo DESC LIMIT 1) s
    GROUP BY s.date
), per_day AS (
    SELECT sp.date, COALESCE(f.n, 0) AS n, COALESCE(f.c, 0) AS c,
           sum(COALESCE(f.n, 0)) OVER (ORDER BY sp.date) AS cn, sum(COALESCE(f.c, 0)) OVER (ORDER BY sp.date) AS cc
    FROM daily_spans sp LEFT JOIN firsts f USING (date)
)
UPDATE daily_stats d SET new_addresses = p.n, new_contracts = p.c, cumulative_addresses = p.cn, cumulative_contracts = p.cc
FROM per_day p WHERE p.date = d.date;

CREATE OR REPLACE FUNCTION daily_stats_advance(max_blocks BIGINT DEFAULT 200000) RETURNS BIGINT LANGUAGE plpgsql AS $f$
DECLARE
    wm BIGINT;
    upto BIGINT;
    newest DATE;
    first_day DATE;
BEGIN
    PERFORM pg_advisory_xact_lock(4670, 1);

    SELECT last_block INTO wm FROM daily_stats_watermark WHERE id = 1;
    IF wm IS NULL THEN
        SELECT daily_stats_day(timestamp) INTO newest FROM blocks ORDER BY number DESC LIMIT 1;
        IF newest IS NULL THEN
            RETURN NULL;
        END IF;
        SELECT min(number) - 1 INTO wm FROM blocks WHERE timestamp >= daily_stats_epoch(newest);
        INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, wm);
    END IF;

    SELECT COALESCE(max(number), wm) INTO upto FROM (
        SELECT number, row_number() OVER (ORDER BY number) AS rn
        FROM blocks WHERE number > wm AND number <= wm + max_blocks
    ) s WHERE number = wm + rn;
    IF upto <= wm THEN
        RETURN wm;
    END IF;
    SELECT daily_stats_day(timestamp) INTO first_day FROM blocks WHERE number = wm + 1;

    INSERT INTO daily_stats_acc AS a (date, first_block, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n)
    SELECT daily_stats_day(b.timestamp), min(b.number), count(*), sum(b.gas_used), COALESCE(sum(b.size), 0), count(b.size),
           COALESCE(sum(b.timestamp - p.timestamp), 0), count(p.number)
    FROM blocks b LEFT JOIN blocks p ON p.number = b.number - 1
    WHERE b.number > wm AND b.number <= upto
    GROUP BY 1
    ON CONFLICT (date) DO UPDATE SET
        first_block = LEAST(a.first_block, EXCLUDED.first_block),
        blocks = a.blocks + EXCLUDED.blocks,
        gas_used = a.gas_used + EXCLUDED.gas_used,
        size_sum = a.size_sum + EXCLUDED.size_sum,
        size_n = a.size_n + EXCLUDED.size_n,
        block_time_sum = a.block_time_sum + EXCLUDED.block_time_sum,
        block_time_n = a.block_time_n + EXCLUDED.block_time_n;

    INSERT INTO daily_stats_acc AS a (date, txs, gas_price_sum, successful_txs, failed_txs)
    SELECT daily_stats_day(b.timestamp), count(*), sum(t.gas_price),
           count(*) FILTER (WHERE t.status = 1), count(*) FILTER (WHERE t.status = 0)
    FROM transactions t JOIN blocks b ON b.number = t.block_number
    WHERE t.block_number > wm AND t.block_number <= upto
    GROUP BY 1
    ON CONFLICT (date) DO UPDATE SET
        txs = a.txs + EXCLUDED.txs,
        gas_price_sum = a.gas_price_sum + EXCLUDED.gas_price_sum,
        successful_txs = a.successful_txs + EXCLUDED.successful_txs,
        failed_txs = a.failed_txs + EXCLUDED.failed_txs;

    WITH fresh AS (
        INSERT INTO daily_active_addresses (date, address)
        SELECT DISTINCT daily_stats_day(b.timestamp), x.address
        FROM transactions t JOIN blocks b ON b.number = t.block_number
        CROSS JOIN LATERAL (VALUES (t.from_address), (t.to_address)) AS x(address)
        WHERE t.block_number > wm AND t.block_number <= upto AND x.address IS NOT NULL
        ON CONFLICT DO NOTHING
        RETURNING date
    )
    INSERT INTO daily_stats_acc AS a (date, active_addresses)
    SELECT date, count(*) FROM fresh GROUP BY 1
    ON CONFLICT (date) DO UPDATE SET active_addresses = a.active_addresses + EXCLUDED.active_addresses;

    INSERT INTO daily_stats_acc AS a (date, token_transfers)
    SELECT daily_stats_day(b.timestamp), count(*)
    FROM token_transfers tt JOIN blocks b ON b.number = tt.block_number
    WHERE tt.block_number > wm AND tt.block_number <= upto
    GROUP BY 1
    ON CONFLICT (date) DO UPDATE SET token_transfers = a.token_transfers + EXCLUDED.token_transfers;

    -- Transactions before a day: the previous aggregated day's before + its own, or, for the first day this
    -- database aggregates, one count over everything before it.
    UPDATE daily_stats_acc a SET txs_before = COALESCE(
        (SELECT p.txs_before + p.txs FROM daily_stats_acc p
         WHERE p.date < a.date AND p.txs_before IS NOT NULL ORDER BY p.date DESC LIMIT 1),
        (SELECT count(*) FROM transactions t JOIN blocks b ON t.block_number = b.number
         WHERE b.timestamp < daily_stats_epoch(a.date) AND t.block_number >= (SELECT history_from()))
        + prune_offset('chain', 'transactions_total'))
    WHERE a.txs_before IS NULL;

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
        SELECT a.first_block AS lo,
               COALESCE((SELECT n.first_block FROM daily_stats_acc n WHERE n.date > a.date ORDER BY n.date LIMIT 1), upto + 1) AS hi
    ) span
    CROSS JOIN LATERAL (
        SELECT count(*) FILTER (WHERE first_seen >= span.lo AND first_seen < span.hi) AS new_addresses,
               count(*) FILTER (WHERE is_contract = true AND first_seen >= span.lo AND first_seen < span.hi) AS new_contracts,
               count(*) FILTER (WHERE first_seen < span.hi) AS cumulative_addresses,
               count(*) FILTER (WHERE is_contract = true AND first_seen < span.hi) AS cumulative_contracts
        FROM address_stats
    ) s
    WHERE a.date >= first_day
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

    UPDATE daily_stats_watermark SET last_block = upto WHERE id = 1;
    -- A day's address set is only needed while its blocks can still arrive or be reorged.
    DELETE FROM daily_active_addresses
    WHERE date < (SELECT daily_stats_day(timestamp) FROM blocks WHERE number = upto) - 1;
    RETURN upto;
END
$f$;

---- create above / drop below ----

CREATE OR REPLACE FUNCTION daily_stats_advance(max_blocks BIGINT DEFAULT 200000) RETURNS BIGINT LANGUAGE plpgsql AS $f$
DECLARE
    wm BIGINT;
    upto BIGINT;
    newest DATE;
    first_day DATE;
BEGIN
    PERFORM pg_advisory_xact_lock(4670, 1);

    SELECT last_block INTO wm FROM daily_stats_watermark WHERE id = 1;
    IF wm IS NULL THEN
        SELECT daily_stats_day(timestamp) INTO newest FROM blocks ORDER BY number DESC LIMIT 1;
        IF newest IS NULL THEN
            RETURN NULL;
        END IF;
        SELECT min(number) - 1 INTO wm FROM blocks WHERE timestamp >= daily_stats_epoch(newest);
        INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, wm);
    END IF;

    SELECT COALESCE(max(number), wm) INTO upto FROM (
        SELECT number, row_number() OVER (ORDER BY number) AS rn
        FROM blocks WHERE number > wm AND number <= wm + max_blocks
    ) s WHERE number = wm + rn;
    IF upto <= wm THEN
        RETURN wm;
    END IF;
    SELECT daily_stats_day(timestamp) INTO first_day FROM blocks WHERE number = wm + 1;

    INSERT INTO daily_stats_acc AS a (date, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n)
    SELECT daily_stats_day(b.timestamp), count(*), sum(b.gas_used), COALESCE(sum(b.size), 0), count(b.size),
           COALESCE(sum(b.timestamp - p.timestamp), 0), count(p.number)
    FROM blocks b LEFT JOIN blocks p ON p.number = b.number - 1
    WHERE b.number > wm AND b.number <= upto
    GROUP BY 1
    ON CONFLICT (date) DO UPDATE SET
        blocks = a.blocks + EXCLUDED.blocks,
        gas_used = a.gas_used + EXCLUDED.gas_used,
        size_sum = a.size_sum + EXCLUDED.size_sum,
        size_n = a.size_n + EXCLUDED.size_n,
        block_time_sum = a.block_time_sum + EXCLUDED.block_time_sum,
        block_time_n = a.block_time_n + EXCLUDED.block_time_n;

    INSERT INTO daily_stats_acc AS a (date, txs, gas_price_sum, successful_txs, failed_txs)
    SELECT daily_stats_day(b.timestamp), count(*), sum(t.gas_price),
           count(*) FILTER (WHERE t.status = 1), count(*) FILTER (WHERE t.status = 0)
    FROM transactions t JOIN blocks b ON b.number = t.block_number
    WHERE t.block_number > wm AND t.block_number <= upto
    GROUP BY 1
    ON CONFLICT (date) DO UPDATE SET
        txs = a.txs + EXCLUDED.txs,
        gas_price_sum = a.gas_price_sum + EXCLUDED.gas_price_sum,
        successful_txs = a.successful_txs + EXCLUDED.successful_txs,
        failed_txs = a.failed_txs + EXCLUDED.failed_txs;

    WITH fresh AS (
        INSERT INTO daily_active_addresses (date, address)
        SELECT DISTINCT daily_stats_day(b.timestamp), x.address
        FROM transactions t JOIN blocks b ON b.number = t.block_number
        CROSS JOIN LATERAL (VALUES (t.from_address), (t.to_address)) AS x(address)
        WHERE t.block_number > wm AND t.block_number <= upto AND x.address IS NOT NULL
        ON CONFLICT DO NOTHING
        RETURNING date
    )
    INSERT INTO daily_stats_acc AS a (date, active_addresses)
    SELECT date, count(*) FROM fresh GROUP BY 1
    ON CONFLICT (date) DO UPDATE SET active_addresses = a.active_addresses + EXCLUDED.active_addresses;

    INSERT INTO daily_stats_acc AS a (date, token_transfers)
    SELECT daily_stats_day(b.timestamp), count(*)
    FROM token_transfers tt JOIN blocks b ON b.number = tt.block_number
    WHERE tt.block_number > wm AND tt.block_number <= upto
    GROUP BY 1
    ON CONFLICT (date) DO UPDATE SET token_transfers = a.token_transfers + EXCLUDED.token_transfers;

    -- Transactions before a day: the previous aggregated day's before + its own, or, for the first day this
    -- database aggregates, one count over everything before it.
    UPDATE daily_stats_acc a SET txs_before = COALESCE(
        (SELECT p.txs_before + p.txs FROM daily_stats_acc p
         WHERE p.date < a.date AND p.txs_before IS NOT NULL ORDER BY p.date DESC LIMIT 1),
        (SELECT count(*) FROM transactions t JOIN blocks b ON t.block_number = b.number
         WHERE b.timestamp < daily_stats_epoch(a.date) AND t.block_number >= (SELECT history_from()))
        + prune_offset('chain', 'transactions_total'))
    WHERE a.txs_before IS NULL;

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
    WHERE a.date >= first_day
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

    UPDATE daily_stats_watermark SET last_block = upto WHERE id = 1;
    -- A day's address set is only needed while its blocks can still arrive or be reorged.
    DELETE FROM daily_active_addresses
    WHERE date < (SELECT daily_stats_day(timestamp) FROM blocks WHERE number = upto) - 1;
    RETURN upto;
END
$f$;

ALTER TABLE daily_stats_acc DROP COLUMN first_block;
