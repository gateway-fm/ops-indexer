-- daily_stats under the stats backfill. The backfill keeps its own
-- per-day parts (daily_stats_base, daily_active_base): a reorg's daily_stats_invalidate never touches them. When it
-- completes, daily_stats_backfill_complete() writes every day the head path's aggregate (daily_stats_acc) does not
-- cover: the backfill's parts, plus for the days from START_BLOCK up to the aggregate's first day what their retained
-- blocks hold. It then recomputes, for every day, what depends on all of history: cumulative_transactions (and the
-- aggregate's txs_before, so later advances continue from it), and the address columns from each day's first block.

ALTER TABLE stats_backfill ADD COLUMN last_ts BIGINT, ADD COLUMN daily_done BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE daily_stats_base (
    date DATE PRIMARY KEY,
    first_block BIGINT NOT NULL,
    blocks BIGINT NOT NULL DEFAULT 0,
    gas_used NUMERIC NOT NULL DEFAULT 0,
    size_sum NUMERIC NOT NULL DEFAULT 0,
    size_n BIGINT NOT NULL DEFAULT 0,
    block_time_sum NUMERIC NOT NULL DEFAULT 0,
    block_time_n BIGINT NOT NULL DEFAULT 0,
    txs BIGINT NOT NULL DEFAULT 0,
    gas_price_sum NUMERIC NOT NULL DEFAULT 0,
    successful_txs BIGINT NOT NULL DEFAULT 0,
    failed_txs BIGINT NOT NULL DEFAULT 0,
    token_transfers BIGINT NOT NULL DEFAULT 0,
    active_addresses BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE daily_active_base (
    date DATE NOT NULL,
    address TEXT NOT NULL,
    PRIMARY KEY (date, address)
);

CREATE FUNCTION daily_stats_backfill_complete() RETURNS VOID LANGUAGE plpgsql AS $f$
DECLARE
    start_block BIGINT;
    start_ts BIGINT;
    last_ts_ BIGINT;
    boundary DATE;
    acc_from BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(4670, 1);
    SELECT target + 1, last_ts INTO start_block, last_ts_ FROM stats_backfill WHERE id = 1;
    SELECT timestamp INTO start_ts FROM blocks WHERE number = start_block;
    IF start_ts IS NULL THEN
        RAISE EXCEPTION 'daily_stats_backfill_complete: block % (START_BLOCK) is not stored yet', start_block;
    END IF;
    boundary := daily_stats_day(start_ts);
    IF EXISTS (SELECT 1 FROM daily_stats_acc WHERE date <= boundary) THEN
        RAISE EXCEPTION 'daily_stats_backfill_complete: the incremental aggregate covers % (START_BLOCK''s day); it was started on a later day', boundary;
    END IF;
    SELECT COALESCE(min(first_block), (SELECT max(number) + 1 FROM blocks)) INTO acc_from FROM daily_stats_acc;

    -- The pair (START_BLOCK - 1, START_BLOCK): the backfill holds the first timestamp, the blocks table the second.
    IF last_ts_ IS NOT NULL THEN
        INSERT INTO daily_stats_base AS b (date, first_block, block_time_sum, block_time_n)
        VALUES (boundary, start_block, start_ts - last_ts_, 1)
        ON CONFLICT (date) DO UPDATE SET block_time_sum = b.block_time_sum + EXCLUDED.block_time_sum,
                                         block_time_n = b.block_time_n + 1;
    END IF;

    CREATE TEMP TABLE daily_parts ON COMMIT DROP AS
    SELECT date, first_block, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n, txs, gas_price_sum,
           successful_txs, failed_txs, token_transfers, active_addresses
    FROM daily_stats_base;

    INSERT INTO daily_parts AS d (date, first_block, blocks, gas_used, size_sum, size_n, block_time_sum, block_time_n,
                                  txs, gas_price_sum, successful_txs, failed_txs, token_transfers, active_addresses)
    SELECT daily_stats_day(b.timestamp), min(b.number), count(*), sum(b.gas_used), COALESCE(sum(b.size), 0), count(b.size),
           COALESCE(sum(b.timestamp - p.timestamp), 0), count(p.number), 0, 0, 0, 0, 0, 0
    FROM blocks b LEFT JOIN blocks p ON p.number = b.number - 1
    WHERE b.number >= start_block AND b.number < acc_from
    GROUP BY 1;
    CREATE TEMP TABLE retained_txs ON COMMIT DROP AS
    SELECT daily_stats_day(b.timestamp) AS date, count(*) AS txs, sum(t.gas_price) AS gas_price_sum,
           count(*) FILTER (WHERE t.status = 1) AS ok, count(*) FILTER (WHERE t.status = 0) AS failed
    FROM transactions t JOIN blocks b ON b.number = t.block_number
    WHERE t.block_number >= start_block AND t.block_number < acc_from
    GROUP BY 1;
    CREATE TEMP TABLE retained_transfers ON COMMIT DROP AS
    SELECT daily_stats_day(b.timestamp) AS date, count(*) AS n
    FROM token_transfers tt JOIN blocks b ON b.number = tt.block_number
    WHERE tt.block_number >= start_block AND tt.block_number < acc_from
    GROUP BY 1;
    CREATE TEMP TABLE retained_active ON COMMIT DROP AS
    SELECT DISTINCT daily_stats_day(b.timestamp) AS date, x.address
    FROM transactions t JOIN blocks b ON b.number = t.block_number
    CROSS JOIN LATERAL (VALUES (t.from_address), (t.to_address)) AS x(address)
    WHERE t.block_number >= start_block AND t.block_number < acc_from AND x.address IS NOT NULL;

    -- Each day appears once: a day before START_BLOCK's only in the base, a later one only from its blocks; the
    -- boundary day in both, so its rows add and its active addresses are the union.
    CREATE TEMP TABLE days ON COMMIT DROP AS
    SELECT p.date, min(p.first_block) AS first_block, sum(p.blocks) AS blocks, sum(p.gas_used) AS gas_used,
           sum(p.size_sum) AS size_sum, sum(p.size_n) AS size_n, sum(p.block_time_sum) AS block_time_sum,
           sum(p.block_time_n) AS block_time_n,
           sum(p.txs) + COALESCE(max(r.txs), 0) AS txs, sum(p.gas_price_sum) + COALESCE(max(r.gas_price_sum), 0) AS gas_price_sum,
           sum(p.successful_txs) + COALESCE(max(r.ok), 0) AS successful_txs,
           sum(p.failed_txs) + COALESCE(max(r.failed), 0) AS failed_txs,
           sum(p.token_transfers) + COALESCE(max(tr.n), 0) AS token_transfers,
           (SELECT count(*) FROM (SELECT address FROM daily_active_base a WHERE a.date = p.date
                                  UNION SELECT address FROM retained_active ra WHERE ra.date = p.date) u) AS active_addresses
    FROM daily_parts p
    LEFT JOIN retained_txs r ON r.date = p.date
    LEFT JOIN retained_transfers tr ON tr.date = p.date
    GROUP BY p.date;

    INSERT INTO daily_stats AS d (date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
        successful_txs, failed_txs, active_addresses, new_addresses, avg_block_time, avg_block_size,
        new_contracts, token_transfer_count, cumulative_transactions, cumulative_addresses, cumulative_contracts)
    SELECT date, blocks, txs, gas_used::BIGINT, COALESCE((gas_price_sum / NULLIF(txs, 0))::BIGINT, 0),
           successful_txs, failed_txs, active_addresses, 0,
           COALESCE((block_time_sum / NULLIF(block_time_n, 0))::FLOAT, 0), COALESCE((size_sum / NULLIF(size_n, 0))::BIGINT, 0),
           0, token_transfers, 0, 0, 0
    FROM days
    ON CONFLICT (date) DO UPDATE SET
        total_blocks = EXCLUDED.total_blocks, total_transactions = EXCLUDED.total_transactions,
        total_gas_used = EXCLUDED.total_gas_used, avg_gas_price = EXCLUDED.avg_gas_price,
        successful_txs = EXCLUDED.successful_txs, failed_txs = EXCLUDED.failed_txs,
        active_addresses = EXCLUDED.active_addresses, avg_block_time = EXCLUDED.avg_block_time,
        avg_block_size = EXCLUDED.avg_block_size, token_transfer_count = EXCLUDED.token_transfer_count;

    -- What depends on all of history, for every day: transactions through the day, and addresses by the day of
    -- their first block (each day's first block: the base's, the retained blocks', the aggregate's).
    WITH sums AS (
        SELECT date, sum(total_transactions) OVER (ORDER BY date) AS through FROM daily_stats
    )
    UPDATE daily_stats d SET cumulative_transactions = s.through FROM sums s WHERE s.date = d.date;
    UPDATE daily_stats_acc a SET txs_before = d.cumulative_transactions - a.txs
    FROM daily_stats d WHERE d.date = a.date;

    CREATE TEMP TABLE day_spans ON COMMIT DROP AS
    SELECT date, lo FROM (
        SELECT date, first_block AS lo FROM days
        UNION ALL SELECT date, first_block FROM daily_stats_acc WHERE first_block IS NOT NULL
    ) x;
    CREATE UNIQUE INDEX ON day_spans (lo);
    WITH firsts AS (
        SELECT s.date, count(*) AS n, count(*) FILTER (WHERE a.is_contract) AS c
        FROM address_stats a
        CROSS JOIN LATERAL (SELECT date FROM day_spans WHERE lo <= a.first_seen ORDER BY lo DESC LIMIT 1) s
        GROUP BY s.date
    ), per_day AS (
        SELECT d.date, COALESCE(f.n, 0) AS n, COALESCE(f.c, 0) AS c,
               sum(COALESCE(f.n, 0)) OVER (ORDER BY d.date) AS cn, sum(COALESCE(f.c, 0)) OVER (ORDER BY d.date) AS cc
        FROM daily_stats d LEFT JOIN firsts f USING (date)
    )
    UPDATE daily_stats d SET new_addresses = p.n, new_contracts = p.c, cumulative_addresses = p.cn, cumulative_contracts = p.cc
    FROM per_day p WHERE p.date = d.date;

    UPDATE stats_backfill SET daily_done = true WHERE id = 1;
END
$f$;

---- create above / drop below ----

DROP FUNCTION daily_stats_backfill_complete();
DROP TABLE daily_active_base;
DROP TABLE daily_stats_base;
ALTER TABLE stats_backfill DROP COLUMN daily_done, DROP COLUMN last_ts;
