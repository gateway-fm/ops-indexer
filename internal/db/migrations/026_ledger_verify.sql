-- Verification and repair of the ledger.
-- A span's rows are computed into stage tables, from its history rows (closing, the retained recount) or from the
-- node (the pruned recount, written by the indexer); the same diff compares them with the stored rows and the same
-- repair replaces the stored rows. Repairing a span touches only the ledger: the running counters are checked
-- against "closed ledger rows + open history rows" separately, and repaired there.

ALTER TABLE ledger_spans
    ADD COLUMN checked_at TIMESTAMPTZ,
    ADD COLUMN check_state TEXT NOT NULL DEFAULT 'unchecked'
        CHECK (check_state IN ('unchecked', 'ok', 'drift', 'repaired', 'quarantined')),
    ADD COLUMN repairs INT NOT NULL DEFAULT 0;
CREATE INDEX ledger_spans_checked ON ledger_spans (checked_at NULLS FIRST, lo);
CREATE INDEX ledger_address_address ON ledger_address (address);

CREATE UNLOGGED TABLE ledger_stage_chain (LIKE ledger_chain INCLUDING ALL);
CREATE UNLOGGED TABLE ledger_stage_token (LIKE ledger_token INCLUDING ALL);
CREATE UNLOGGED TABLE ledger_stage_address (LIKE ledger_address INCLUDING ALL);

-- Every repair, per span or running key, with what it replaced.
CREATE TABLE ledger_repairs (
    at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lo BIGINT,
    family TEXT NOT NULL,
    key TEXT NOT NULL,
    counter TEXT NOT NULL,
    stored NUMERIC,
    repaired NUMERIC
);
CREATE INDEX ledger_repairs_key ON ledger_repairs (family, key, counter);

-- Running keys whose drift came back after a repair: no longer repaired, alerted.
CREATE TABLE ledger_quarantine (
    family TEXT NOT NULL,
    key TEXT NOT NULL,
    since TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (family, key)
);

CREATE FUNCTION ledger_stage_clear(p_lo BIGINT) RETURNS VOID LANGUAGE sql AS $f$
    DELETE FROM ledger_stage_chain WHERE lo = p_lo;
    DELETE FROM ledger_stage_token WHERE lo = p_lo;
    DELETE FROM ledger_stage_address WHERE lo = p_lo;
$f$;

CREATE FUNCTION ledger_stage_from_rows(p_lo BIGINT, p_hi BIGINT) RETURNS VOID LANGUAGE plpgsql AS $f$
BEGIN
    PERFORM ledger_stage_clear(p_lo);
    INSERT INTO ledger_stage_chain (lo, name, value)
    VALUES (p_lo, 'blocks_total', (SELECT count(*) FROM blocks WHERE number >= p_lo AND number < p_hi)),
           (p_lo, 'transactions_total', (SELECT count(*) FROM transactions WHERE block_number >= p_lo AND block_number < p_hi)),
           (p_lo, 'transfers_total', (SELECT count(*) FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi));
    INSERT INTO ledger_stage_token (lo, token, transfers, supply)
    SELECT p_lo, token_address, count(*),
           COALESCE(SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
                  - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END), 0)
    FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi
    GROUP BY token_address;
    INSERT INTO ledger_stage_address (lo, address, txs, sent, internal, transfers, first_seen, last_seen)
    SELECT p_lo, address, count(DISTINCT tx), sum(sent), sum(internal), sum(transfers), min(block), max(block)
    FROM ledger_address_contributions(p_lo, p_hi) x
    GROUP BY address;
END
$f$;

-- What each history row of [p_lo, p_hi) contributes to an address's counters, as the writer counts it.
CREATE FUNCTION ledger_address_contributions(p_lo BIGINT, p_hi BIGINT)
RETURNS TABLE (address TEXT, tx TEXT, block BIGINT, sent INT, internal INT, transfers INT) LANGUAGE sql STABLE AS $f$
    SELECT from_address, hash, block_number, CASE WHEN tx_type BETWEEN 0 AND 4 THEN 1 ELSE 0 END, 0, 0
    FROM transactions WHERE block_number >= p_lo AND block_number < p_hi
    UNION ALL
    SELECT to_address, hash, block_number, 0, 0, 0
    FROM transactions WHERE block_number >= p_lo AND block_number < p_hi AND to_address IS NOT NULL
    UNION ALL
    SELECT from_address, tx_hash, block_number, 0, 0, 1
    FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi
      AND from_address <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT to_address, tx_hash, block_number, 0, 0, 1
    FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi
      AND to_address <> '0x0000000000000000000000000000000000000000' AND to_address <> from_address
    UNION ALL
    SELECT from_address, NULL, block_number, 0, 1, 0
    FROM internal_transactions WHERE block_number >= p_lo AND block_number < p_hi
    UNION ALL
    SELECT to_address, NULL, block_number, 0, 1, 0
    FROM internal_transactions WHERE block_number >= p_lo AND block_number < p_hi
      AND to_address IS NOT NULL AND to_address <> from_address
    UNION ALL
    SELECT address, NULL, block_number, 0, 0, 0
    FROM contracts WHERE block_number >= p_lo AND block_number < p_hi
$f$;

-- Moves a span's staged rows into the ledger, replacing what it held.
CREATE FUNCTION ledger_stage_commit(p_lo BIGINT) RETURNS VOID LANGUAGE plpgsql AS $f$
BEGIN
    DELETE FROM ledger_chain WHERE lo = p_lo;
    DELETE FROM ledger_token WHERE lo = p_lo;
    DELETE FROM ledger_address WHERE lo = p_lo;
    INSERT INTO ledger_chain SELECT * FROM ledger_stage_chain WHERE lo = p_lo;
    INSERT INTO ledger_token SELECT * FROM ledger_stage_token WHERE lo = p_lo;
    INSERT INTO ledger_address SELECT * FROM ledger_stage_address WHERE lo = p_lo;
    PERFORM ledger_stage_clear(p_lo);
END
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
    SELECT count(*) INTO stored FROM blocks WHERE number >= p_lo AND number < p_hi;
    IF stored <> p_hi - p_lo THEN
        RETURN false;
    END IF;
    INSERT INTO ledger_spans (lo, hi, source) VALUES (p_lo, p_hi, 'rows');
    PERFORM ledger_stage_from_rows(p_lo, p_hi);
    PERFORM ledger_stage_commit(p_lo);
    RETURN true;
END
$f$;

-- Stored rows against staged rows, one row per differing (family, key, counter).
CREATE FUNCTION ledger_span_diff(p_lo BIGINT)
RETURNS TABLE (family TEXT, key TEXT, counter TEXT, stored NUMERIC, recount NUMERIC) LANGUAGE sql STABLE AS $f$
    SELECT 'chain', COALESCE(s.name, r.name), 'value', s.value::NUMERIC, r.value::NUMERIC
    FROM (SELECT * FROM ledger_chain WHERE lo = p_lo) s
    FULL JOIN (SELECT * FROM ledger_stage_chain WHERE lo = p_lo) r ON r.name = s.name
    WHERE s.value IS DISTINCT FROM r.value
    UNION ALL
    SELECT 'token', COALESCE(s.token, r.token), c.counter, c.stored, c.recount
    FROM (SELECT * FROM ledger_token WHERE lo = p_lo) s
    FULL JOIN (SELECT * FROM ledger_stage_token WHERE lo = p_lo) r ON r.token = s.token
    CROSS JOIN LATERAL (VALUES ('transfers', s.transfers::NUMERIC, r.transfers::NUMERIC), ('supply', s.supply, r.supply))
        AS c(counter, stored, recount)
    WHERE c.stored IS DISTINCT FROM c.recount
    UNION ALL
    SELECT 'address', COALESCE(s.address, r.address), c.counter, c.stored, c.recount
    FROM (SELECT * FROM ledger_address WHERE lo = p_lo) s
    FULL JOIN (SELECT * FROM ledger_stage_address WHERE lo = p_lo) r ON r.address = s.address
    CROSS JOIN LATERAL (VALUES ('txs', s.txs::NUMERIC, r.txs::NUMERIC), ('sent', s.sent, r.sent),
                               ('internal', s.internal, r.internal), ('transfers', s.transfers, r.transfers),
                               ('first_seen', s.first_seen, r.first_seen), ('last_seen', s.last_seen, r.last_seen))
        AS c(counter, stored, recount)
    WHERE c.stored IS DISTINCT FROM c.recount
$f$;

-- Records the staged recount of a closed span as a check: ok when it matches; otherwise, with repair on and the span
-- not repaired before, replaces the stored rows (logged per key in ledger_repairs) and marks it repaired; a span that
-- drifts again after a repair is quarantined and keeps its rows. Returns the number of differing counters.
CREATE FUNCTION ledger_span_settle(p_lo BIGINT, repair BOOLEAN) RETURNS INT LANGUAGE plpgsql AS $f$
DECLARE
    drifted INT;
    prior TEXT;
BEGIN
    SELECT check_state INTO prior FROM ledger_spans WHERE lo = p_lo FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ledger_span_settle: no closed span at %', p_lo;
    END IF;
    SELECT count(*) INTO drifted FROM ledger_span_diff(p_lo);
    IF drifted = 0 THEN
        UPDATE ledger_spans SET checked_at = now(),
                                check_state = CASE WHEN prior IN ('repaired', 'quarantined') THEN prior ELSE 'ok' END
        WHERE lo = p_lo;
    ELSIF prior IN ('repaired', 'quarantined') OR NOT repair THEN
        UPDATE ledger_spans SET checked_at = now(),
                                check_state = CASE WHEN prior IN ('repaired', 'quarantined') THEN 'quarantined' ELSE 'drift' END
        WHERE lo = p_lo;
    ELSE
        INSERT INTO ledger_repairs (lo, family, key, counter, stored, repaired)
        SELECT p_lo, family, key, counter, stored, recount FROM ledger_span_diff(p_lo);
        PERFORM ledger_stage_commit(p_lo);
        UPDATE ledger_spans SET checked_at = now(), check_state = 'repaired', repairs = repairs + 1 WHERE lo = p_lo;
    END IF;
    PERFORM ledger_stage_clear(p_lo);
    RETURN drifted;
END
$f$;

-- The end of the closed prefix: every block below it is in a closed span.
CREATE FUNCTION ledger_closed_through() RETURNS BIGINT LANGUAGE sql STABLE AS $f$
    SELECT COALESCE(max(hi), 0) FROM ledger_spans
$f$;

-- Running chain and token counters against closed ledger rows + the open history rows at or above the closed prefix.
CREATE FUNCTION ledger_running_diff_chain_token()
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

-- Running address counters of one bucket (the address's first hex byte, 0-255) against closed ledger rows + open rows.
CREATE FUNCTION ledger_running_diff_address(bucket INT)
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

---- create above / drop below ----

DROP FUNCTION ledger_running_diff_address(INT);
DROP FUNCTION ledger_running_diff_chain_token();
DROP FUNCTION ledger_closed_through();
DROP FUNCTION ledger_span_settle(BIGINT, BOOLEAN);
DROP FUNCTION ledger_span_diff(BIGINT);
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
    SELECT count(*) INTO stored FROM blocks WHERE number >= p_lo AND number < p_hi;
    IF stored <> p_hi - p_lo THEN
        RETURN false;
    END IF;

    INSERT INTO ledger_spans (lo, hi, source) VALUES (p_lo, p_hi, 'rows');
    INSERT INTO ledger_chain (lo, name, value)
    VALUES (p_lo, 'blocks_total', stored),
           (p_lo, 'transactions_total', (SELECT count(*) FROM transactions WHERE block_number >= p_lo AND block_number < p_hi)),
           (p_lo, 'transfers_total', (SELECT count(*) FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi));
    INSERT INTO ledger_token (lo, token, transfers, supply)
    SELECT p_lo, token_address, count(*),
           COALESCE(SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
                  - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END), 0)
    FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi
    GROUP BY token_address;
    INSERT INTO ledger_address (lo, address, txs, sent, internal, transfers, first_seen, last_seen)
    SELECT p_lo, address, count(DISTINCT tx), sum(sent), sum(internal), sum(transfers), min(block), max(block)
    FROM (
        SELECT from_address AS address, hash AS tx, block_number AS block,
               CASE WHEN tx_type BETWEEN 0 AND 4 THEN 1 ELSE 0 END AS sent, 0 AS internal, 0 AS transfers
        FROM transactions WHERE block_number >= p_lo AND block_number < p_hi
        UNION ALL
        SELECT to_address, hash, block_number, 0, 0, 0
        FROM transactions WHERE block_number >= p_lo AND block_number < p_hi AND to_address IS NOT NULL
        UNION ALL
        SELECT from_address, tx_hash, block_number, 0, 0, 1
        FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi
          AND from_address <> '0x0000000000000000000000000000000000000000'
        UNION ALL
        SELECT to_address, tx_hash, block_number, 0, 0, 1
        FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi
          AND to_address <> '0x0000000000000000000000000000000000000000' AND to_address <> from_address
        UNION ALL
        SELECT from_address, NULL, block_number, 0, 1, 0
        FROM internal_transactions WHERE block_number >= p_lo AND block_number < p_hi
        UNION ALL
        SELECT to_address, NULL, block_number, 0, 1, 0
        FROM internal_transactions WHERE block_number >= p_lo AND block_number < p_hi
          AND to_address IS NOT NULL AND to_address <> from_address
        UNION ALL
        SELECT address, NULL, block_number, 0, 0, 0
        FROM contracts WHERE block_number >= p_lo AND block_number < p_hi
    ) x
    GROUP BY address;
    RETURN true;
END
$f$;
DROP FUNCTION ledger_stage_commit(BIGINT);
DROP FUNCTION ledger_stage_from_rows(BIGINT, BIGINT);
DROP FUNCTION ledger_address_contributions(BIGINT, BIGINT);
DROP FUNCTION ledger_stage_clear(BIGINT);
DROP TABLE ledger_quarantine;
DROP TABLE ledger_repairs;
DROP TABLE ledger_stage_address;
DROP TABLE ledger_stage_token;
DROP TABLE ledger_stage_chain;
DROP INDEX ledger_address_address;
DROP INDEX ledger_spans_checked;
ALTER TABLE ledger_spans DROP COLUMN repairs, DROP COLUMN check_state, DROP COLUMN checked_at;
