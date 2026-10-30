-- The stats backfill: a deployment that starts at START_BLOCK > 0
-- with retention walks [0, START_BLOCK) on the node and writes only counters, state and the ledger, never history
-- rows. Everything below START_BLOCK is pruned from day one. The span in progress accumulates in the stage tables,
-- which become logged so a crash cannot empty them, and moves into the ledger in the batch that completes it; until
-- then every recount (prune_offsets, the running check, a token's seed) counts it too. Spans from START_BLOCK upwards
-- close while the backfill runs.

CREATE TABLE stats_backfill (
    id INT PRIMARY KEY CHECK (id = 1),
    target BIGINT NOT NULL,
    through BIGINT NOT NULL,
    span_lo BIGINT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- (token, address) keys the backfill touched: each one's balance is read once, at latest.
CREATE TABLE stats_backfill_balance_keys (
    token_address TEXT NOT NULL,
    address TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    PRIMARY KEY (token_address, address)
);

ALTER TABLE ledger_stage_chain SET LOGGED;
ALTER TABLE ledger_stage_token SET LOGGED;
ALTER TABLE ledger_stage_address SET LOGGED;
-- ledger_token's token index is built CONCURRENTLY after startup (EnsureLedgerIndexes), as ledger_address's is.

-- The lo of the backfill's span in progress; NULL when no backfill runs.
CREATE FUNCTION stats_backfill_open_span() RETURNS BIGINT LANGUAGE sql STABLE AS $f$
    SELECT span_lo FROM stats_backfill WHERE completed_at IS NULL
$f$;

CREATE OR REPLACE VIEW prune_offsets AS
SELECT 'chain'::TEXT AS kind, c.name AS key, sum(c.value)::BIGINT AS count, 0::NUMERIC AS supply
FROM (SELECT c.name, c.value FROM ledger_chain c JOIN ledger_spans s USING (lo) WHERE s.hi <= (SELECT history_from())
      UNION ALL SELECT name, value FROM ledger_stage_chain WHERE lo = (SELECT stats_backfill_open_span())) c
GROUP BY c.name
UNION ALL
SELECT 'token', t.token, sum(t.transfers)::BIGINT, sum(t.supply)
FROM (SELECT t.token, t.transfers, t.supply FROM ledger_token t JOIN ledger_spans s USING (lo) WHERE s.hi <= (SELECT history_from())
      UNION ALL SELECT token, transfers, supply FROM ledger_stage_token WHERE lo = (SELECT stats_backfill_open_span())) t
GROUP BY t.token;

-- What a token's counters hold when its row is created: its retained transfers, the ledger rows of pruned spans, and
-- the backfill's span in progress.
CREATE FUNCTION token_counted(p_token TEXT, OUT cnt BIGINT, OUT supply NUMERIC) LANGUAGE sql STABLE AS $f$
    SELECT COALESCE(sum(n), 0)::BIGINT, COALESCE(sum(s), 0) FROM (
        SELECT count(*) AS n,
               COALESCE(SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
                      - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END), 0) AS s
        FROM token_transfers WHERE token_address = p_token AND block_number >= (SELECT history_from())
        UNION ALL
        SELECT t.transfers, t.supply FROM ledger_token t JOIN ledger_spans sp USING (lo)
        WHERE t.token = p_token AND sp.hi <= (SELECT history_from())
        UNION ALL
        SELECT transfers, supply FROM ledger_stage_token WHERE token = p_token AND lo = (SELECT stats_backfill_open_span())
    ) u
$f$;

CREATE OR REPLACE FUNCTION ledger_next_span(span_blocks BIGINT, first_block BIGINT, OUT lo BIGINT, OUT hi BIGINT) LANGUAGE plpgsql STABLE AS $f$
BEGIN
    lo := GREATEST(COALESCE((SELECT max(s.hi) FROM ledger_spans s), first_block), history_from());
    hi := LEAST((lo / span_blocks + 1) * span_blocks,
                (SELECT min(b) FROM (SELECT p.lo AS b FROM history_partitions p UNION ALL SELECT p.hi FROM history_partitions p) x
                 WHERE b > lo));
END
$f$;

CREATE OR REPLACE FUNCTION ledger_close_span(p_lo BIGINT, p_hi BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql AS $f$
DECLARE
    head BIGINT;
    stored BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(4672, 1);
    IF p_lo IS DISTINCT FROM GREATEST(COALESCE((SELECT max(hi) FROM ledger_spans), p_lo), (SELECT history_from())) THEN
        RAISE EXCEPTION 'ledger_close_span: [%, %) is not the next span', p_lo, p_hi;
    END IF;
    SELECT max(number) INTO head FROM blocks;
    IF head IS NULL OR p_hi - 1 + prune_floor_gap() > head THEN
        RETURN false;
    END IF;
    IF EXISTS (SELECT 1 FROM missing_block_ranges WHERE from_number < p_hi AND to_number >= p_lo) THEN
        RETURN false;
    END IF;
    SELECT count(*) INTO stored FROM blocks WHERE number >= GREATEST(p_lo, 1) AND number < p_hi;
    IF stored <> p_hi - GREATEST(p_lo, 1) THEN
        RETURN false;
    END IF;
    INSERT INTO ledger_spans (lo, hi, source) VALUES (p_lo, p_hi, 'rows');
    PERFORM ledger_stage_from_rows(p_lo, p_hi);
    PERFORM ledger_stage_commit(p_lo);
    RETURN true;
END
$f$;

CREATE OR REPLACE FUNCTION ledger_running_diff_chain_token()
RETURNS TABLE (family TEXT, key TEXT, counter TEXT, running NUMERIC, expected NUMERIC) LANGUAGE sql STABLE AS $f$
    WITH open AS (SELECT ledger_closed_through() AS lo),
    chain AS (
        SELECT name, sum(value) AS v FROM (
            SELECT name, value FROM ledger_chain
            UNION ALL SELECT name, value FROM ledger_stage_chain WHERE lo = stats_backfill_open_span()
            UNION ALL SELECT 'blocks_total', count(*) FROM blocks, open WHERE number >= open.lo
            UNION ALL SELECT 'transactions_total', count(*) FROM transactions, open WHERE block_number >= open.lo
            UNION ALL SELECT 'transfers_total', count(*) FROM token_transfers, open WHERE block_number >= open.lo
        ) u GROUP BY name
    ),
    token AS (
        SELECT token, sum(transfers) AS transfers, sum(supply) AS supply FROM (
            SELECT token, transfers, supply FROM ledger_token
            UNION ALL SELECT token, transfers, supply FROM ledger_stage_token WHERE lo = stats_backfill_open_span()
            UNION ALL
            SELECT token_address, count(*),
                   COALESCE(SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
                          - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END), 0)
            FROM token_transfers, open WHERE block_number >= open.lo GROUP BY token_address
        ) u GROUP BY token
    )
    SELECT 'chain', c.name, 'count', cc.count::NUMERIC, c.v
    FROM chain c JOIN chain_counters cc ON cc.name = c.name
    WHERE cc.count IS DISTINCT FROM c.v
    UNION ALL
    SELECT 'token', t.address, x.counter, x.running, x.expected
    FROM tokens t
    LEFT JOIN token e ON e.token = t.address
    CROSS JOIN LATERAL (VALUES ('transfers', COALESCE(t.transfer_count, 0)::NUMERIC, COALESCE(e.transfers, 0)),
                               ('supply', CASE WHEN t.token_type = 'ERC20' THEN COALESCE(t.total_supply, 0) END,
                                          CASE WHEN t.token_type = 'ERC20' THEN COALESCE(e.supply, 0) END))
        AS x(counter, running, expected)
    WHERE x.running IS DISTINCT FROM x.expected
$f$;

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

---- create above / drop below ----

CREATE OR REPLACE FUNCTION ledger_running_diff_chain_token()
RETURNS TABLE (family TEXT, key TEXT, counter TEXT, running NUMERIC, expected NUMERIC) LANGUAGE sql STABLE AS $f$
    WITH open AS (SELECT ledger_closed_through() AS lo),
    chain AS (
        SELECT name, sum(value) AS v FROM (
            SELECT name, value FROM ledger_chain
            UNION ALL SELECT 'blocks_total', count(*) FROM blocks, open WHERE number >= open.lo
            UNION ALL SELECT 'transactions_total', count(*) FROM transactions, open WHERE block_number >= open.lo
            UNION ALL SELECT 'transfers_total', count(*) FROM token_transfers, open WHERE block_number >= open.lo
        ) u GROUP BY name
    ),
    token AS (
        SELECT token, sum(transfers) AS transfers, sum(supply) AS supply FROM (
            SELECT token, transfers, supply FROM ledger_token
            UNION ALL
            SELECT token_address, count(*),
                   COALESCE(SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
                          - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END), 0)
            FROM token_transfers, open WHERE block_number >= open.lo GROUP BY token_address
        ) u GROUP BY token
    )
    SELECT 'chain', c.name, 'count', cc.count::NUMERIC, c.v
    FROM chain c JOIN chain_counters cc ON cc.name = c.name
    WHERE cc.count IS DISTINCT FROM c.v
    UNION ALL
    SELECT 'token', t.address, x.counter, x.running, x.expected
    FROM tokens t
    LEFT JOIN token e ON e.token = t.address
    CROSS JOIN LATERAL (VALUES ('transfers', COALESCE(t.transfer_count, 0)::NUMERIC, COALESCE(e.transfers, 0)),
                               ('supply', CASE WHEN t.token_type = 'ERC20' THEN COALESCE(t.total_supply, 0) END,
                                          CASE WHEN t.token_type = 'ERC20' THEN COALESCE(e.supply, 0) END))
        AS x(counter, running, expected)
    WHERE x.running IS DISTINCT FROM x.expected
$f$;

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

CREATE OR REPLACE FUNCTION ledger_close_span(p_lo BIGINT, p_hi BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql AS $f$
DECLARE
    head BIGINT;
    stored BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(4672, 1);
    IF p_lo IS DISTINCT FROM COALESCE((SELECT max(hi) FROM ledger_spans), p_lo) THEN
        RAISE EXCEPTION 'ledger_close_span: [%, %) is not the next span', p_lo, p_hi;
    END IF;
    SELECT max(number) INTO head FROM blocks;
    IF head IS NULL OR p_hi - 1 + prune_floor_gap() > head THEN
        RETURN false;
    END IF;
    IF EXISTS (SELECT 1 FROM missing_block_ranges WHERE from_number < p_hi AND to_number >= p_lo) THEN
        RETURN false;
    END IF;
    SELECT count(*) INTO stored FROM blocks WHERE number >= GREATEST(p_lo, 1) AND number < p_hi;
    IF stored <> p_hi - GREATEST(p_lo, 1) THEN
        RETURN false;
    END IF;
    INSERT INTO ledger_spans (lo, hi, source) VALUES (p_lo, p_hi, 'rows');
    PERFORM ledger_stage_from_rows(p_lo, p_hi);
    PERFORM ledger_stage_commit(p_lo);
    RETURN true;
END
$f$;

CREATE OR REPLACE FUNCTION ledger_next_span(span_blocks BIGINT, first_block BIGINT, OUT lo BIGINT, OUT hi BIGINT) LANGUAGE plpgsql STABLE AS $f$
BEGIN
    lo := COALESCE((SELECT max(s.hi) FROM ledger_spans s), GREATEST(first_block, history_from()));
    hi := LEAST((lo / span_blocks + 1) * span_blocks,
                (SELECT min(b) FROM (SELECT p.lo AS b FROM history_partitions p UNION ALL SELECT p.hi FROM history_partitions p) x
                 WHERE b > lo));
END
$f$;

DROP FUNCTION token_counted(TEXT);
CREATE OR REPLACE VIEW prune_offsets AS
SELECT 'chain'::TEXT AS kind, c.name AS key, sum(c.value)::BIGINT AS count, 0::NUMERIC AS supply
FROM ledger_chain c JOIN ledger_spans s USING (lo)
WHERE s.hi <= (SELECT history_from())
GROUP BY c.name
UNION ALL
SELECT 'token', t.token, sum(t.transfers)::BIGINT, sum(t.supply)
FROM ledger_token t JOIN ledger_spans s USING (lo)
WHERE s.hi <= (SELECT history_from())
GROUP BY t.token;
DROP FUNCTION stats_backfill_open_span();
DROP INDEX IF EXISTS ledger_token_token;
ALTER TABLE ledger_stage_address SET UNLOGGED;
ALTER TABLE ledger_stage_token SET UNLOGGED;
ALTER TABLE ledger_stage_chain SET UNLOGGED;
DROP TABLE stats_backfill_balance_keys;
DROP TABLE stats_backfill;
