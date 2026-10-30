-- The zero address in the running diff of an address bucket. It has an address_stats row like any address: ingest
-- and the ledger both count the transactions sent to it and the internal calls to and from it, and neither counts
-- token transfers for it. The diff left it out, so the running check, the one-time reconciliation and
-- repair-address-stats never compared or repaired it; only a span close set it. It is in bucket 0 ('0x00' to '0x01').
-- Same body as 033 otherwise.
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
$f$;

---- create above / drop below ----

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
