-- The running diff of one address bucket (028) read every history row above the closed spans and kept the bucket's
-- 1/256 after the union, and read ledger_address from the bucket to the end of the table: about 10 minutes a bucket
-- on a 6.4M-transaction open tail, for the reconciliation's 256 buckets and every tick of the running check.
-- The bucket's address range now bounds each history table's address index scan, and ledger_address's
-- and address_stats' at both ends. Same rows, same result.

CREATE FUNCTION address_bucket_lo(bucket INT) RETURNS TEXT LANGUAGE sql IMMUTABLE AS $f$
    SELECT '0x' || lpad(to_hex(bucket), 2, '0')
$f$;

-- The first address past the bucket; past the last bucket, '0y', above every '0x' address.
CREATE FUNCTION address_bucket_hi(bucket INT) RETURNS TEXT LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE WHEN bucket < 255 THEN '0x' || lpad(to_hex(bucket + 1), 2, '0') ELSE '0y' END
$f$;

-- ledger_address_contributions from block p_lo up, for the addresses in [p_from, p_to), through each table's address
-- index.
CREATE FUNCTION ledger_address_contributions_in(p_lo BIGINT, p_from TEXT, p_to TEXT)
RETURNS TABLE (address TEXT, tx TEXT, block BIGINT, sent INT, internal INT, transfers INT) LANGUAGE sql STABLE AS $f$
    SELECT from_address, hash, block_number, CASE WHEN tx_type BETWEEN 0 AND 4 THEN 1 ELSE 0 END, 0, 0
    FROM transactions WHERE from_address >= p_from AND from_address < p_to AND block_number >= p_lo
    UNION ALL
    SELECT to_address, hash, block_number, 0, 0, 0
    FROM transactions WHERE to_address >= p_from AND to_address < p_to AND block_number >= p_lo
    UNION ALL
    SELECT from_address, tx_hash, block_number, 0, 0, 1
    FROM token_transfers WHERE from_address >= p_from AND from_address < p_to AND block_number >= p_lo
      AND from_address <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT to_address, tx_hash, block_number, 0, 0, 1
    FROM token_transfers WHERE to_address >= p_from AND to_address < p_to AND block_number >= p_lo
      AND to_address <> '0x0000000000000000000000000000000000000000' AND to_address <> from_address
    UNION ALL
    SELECT from_address, NULL, block_number, 0, 1, 0
    FROM internal_transactions WHERE from_address >= p_from AND from_address < p_to AND block_number >= p_lo
    UNION ALL
    SELECT to_address, NULL, block_number, 0, 1, 0
    FROM internal_transactions WHERE to_address >= p_from AND to_address < p_to AND block_number >= p_lo
      AND to_address <> from_address
    UNION ALL
    SELECT address, NULL, block_number, 0, 0, 0
    FROM contracts WHERE address >= p_from AND address < p_to AND block_number >= p_lo
$f$;

CREATE OR REPLACE FUNCTION ledger_running_diff_address(bucket INT)
RETURNS TABLE (family TEXT, key TEXT, counter TEXT, running NUMERIC, expected NUMERIC) LANGUAGE sql STABLE AS $f$
    WITH open_rows AS (
        SELECT x.address, count(DISTINCT x.tx) AS txs, sum(x.sent) AS sent, sum(x.internal) AS internal,
               sum(x.transfers) AS transfers, min(x.block) AS first_seen, max(x.block) AS last_seen
        FROM ledger_address_contributions_in(ledger_closed_through(), address_bucket_lo(bucket), address_bucket_hi(bucket)) x
        GROUP BY x.address
    ),
    expected AS (
        SELECT address, sum(txs) AS txs, sum(sent) AS sent, sum(internal) AS internal, sum(transfers) AS transfers,
               min(first_seen) AS first_seen, max(last_seen) AS last_seen
        FROM (
            SELECT l.address, l.txs, l.sent, l.internal, l.transfers, l.first_seen, l.last_seen
            FROM ledger_address l
            WHERE l.address >= address_bucket_lo(bucket) AND l.address < address_bucket_hi(bucket)
            UNION ALL
            SELECT l.address, l.txs, l.sent, l.internal, l.transfers, l.first_seen, l.last_seen
            FROM ledger_stage_address l
            WHERE l.lo = stats_backfill_open_span()
              AND l.address >= address_bucket_lo(bucket) AND l.address < address_bucket_hi(bucket)
            UNION ALL SELECT * FROM open_rows
        ) u GROUP BY address
    ),
    running AS (
        SELECT a.* FROM address_stats a
        WHERE a.address >= address_bucket_lo(bucket) AND a.address < address_bucket_hi(bucket)
    )
    SELECT 'address', COALESCE(r.address, e.address), x.counter, x.running, x.expected
    FROM running r FULL JOIN expected e ON e.address = r.address
    CROSS JOIN LATERAL (VALUES ('txs', r.tx_count::NUMERIC, e.txs), ('sent', r.sent_count::NUMERIC, e.sent),
                               ('internal', r.internal_tx_count::NUMERIC, e.internal),
                               ('transfers', r.token_transfer_count::NUMERIC, e.transfers),
                               ('first_seen', r.first_seen::NUMERIC, e.first_seen::NUMERIC),
                               ('last_seen', r.last_seen::NUMERIC, e.last_seen::NUMERIC))
        AS x(counter, running, expected)
    WHERE COALESCE(x.running, 0) IS DISTINCT FROM COALESCE(x.expected, 0)
      AND COALESCE(r.address, e.address) <> '0x0000000000000000000000000000000000000000'
$f$;

---- create above / drop below ----

CREATE OR REPLACE FUNCTION ledger_running_diff_address(bucket INT)
RETURNS TABLE (family TEXT, key TEXT, counter TEXT, running NUMERIC, expected NUMERIC) LANGUAGE sql STABLE AS $f$
    WITH bounds AS (
        SELECT '0x' || lpad(to_hex(bucket), 2, '0') AS lo,
               CASE WHEN bucket < 255 THEN '0x' || lpad(to_hex(bucket + 1), 2, '0') END AS hi,
               ledger_closed_through() AS open_lo
    ),
    open_rows AS (
        SELECT x.address, count(DISTINCT x.tx) AS txs, sum(x.sent) AS sent, sum(x.internal) AS internal,
               sum(x.transfers) AS transfers, min(x.block) AS first_seen, max(x.block) AS last_seen
        FROM bounds, ledger_address_contributions(bounds.open_lo, 9223372036854775807) x
        WHERE x.address >= bounds.lo AND (bounds.hi IS NULL OR x.address < bounds.hi)
        GROUP BY x.address
    ),
    expected AS (
        SELECT address, sum(txs) AS txs, sum(sent) AS sent, sum(internal) AS internal, sum(transfers) AS transfers,
               min(first_seen) AS first_seen, max(last_seen) AS last_seen
        FROM (
            SELECT l.address, l.txs, l.sent, l.internal, l.transfers, l.first_seen, l.last_seen
            FROM ledger_address l, bounds WHERE l.address >= bounds.lo AND (bounds.hi IS NULL OR l.address < bounds.hi)
            UNION ALL
            SELECT l.address, l.txs, l.sent, l.internal, l.transfers, l.first_seen, l.last_seen
            FROM ledger_stage_address l, bounds
            WHERE l.lo = stats_backfill_open_span() AND l.address >= bounds.lo AND (bounds.hi IS NULL OR l.address < bounds.hi)
            UNION ALL SELECT * FROM open_rows
        ) u GROUP BY address
    ),
    running AS (
        SELECT a.* FROM address_stats a, bounds WHERE a.address >= bounds.lo AND (bounds.hi IS NULL OR a.address < bounds.hi)
    )
    SELECT 'address', COALESCE(r.address, e.address), x.counter, x.running, x.expected
    FROM running r FULL JOIN expected e ON e.address = r.address
    CROSS JOIN LATERAL (VALUES ('txs', r.tx_count::NUMERIC, e.txs), ('sent', r.sent_count::NUMERIC, e.sent),
                               ('internal', r.internal_tx_count::NUMERIC, e.internal),
                               ('transfers', r.token_transfer_count::NUMERIC, e.transfers),
                               ('first_seen', r.first_seen::NUMERIC, e.first_seen::NUMERIC),
                               ('last_seen', r.last_seen::NUMERIC, e.last_seen::NUMERIC))
        AS x(counter, running, expected)
    WHERE COALESCE(x.running, 0) IS DISTINCT FROM COALESCE(x.expected, 0)
      AND COALESCE(r.address, e.address) <> '0x0000000000000000000000000000000000000000'
$f$;
DROP FUNCTION ledger_address_contributions_in(BIGINT, TEXT, TEXT);
DROP FUNCTION address_bucket_hi(INT);
DROP FUNCTION address_bucket_lo(INT);
