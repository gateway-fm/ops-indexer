-- Closing a ledger span reads each history table once. On a production deployment
-- 019's version took 40+ min on an 18.5M-transaction span: transactions read twice through index scans visiting the
-- heap per row, and count(DISTINCT tx) per address sorting every (address, transaction) pair to disk.
--
-- The rows are 019's. A transaction counts once for its sender, once for its recipient when that is another
-- address, and once for each Transfer participant that is neither (the participants grouped per transaction
-- first), so the per-address grouping is plain sums: a hash aggregate over the span's addresses, no sort. The span's
-- transfers are read once into a temporary table for the chain count, the token rows and the address rows. Plain
-- index scans are off for the closer, so heap reads are bitmap scans in block order, and it runs on one worker; the
-- caller sets work_mem (LEDGER_WORK_MEM).
--
-- Genesis is optional: ingest that starts on an empty database never stores block 0, so
-- completeness counts from block 1; a stored block 0 is counted in blocks_total.

CREATE OR REPLACE FUNCTION history_range_complete(p_suffix TEXT) RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $f$
DECLARE
    p_lo BIGINT;
    p_hi BIGINT;
    first_block BIGINT;
    stored BIGINT;
BEGIN
    SELECT lo, hi INTO p_lo, p_hi FROM history_partitions WHERE suffix = p_suffix;
    IF p_hi IS NULL THEN
        RETURN false;
    END IF;
    first_block := GREATEST(COALESCE(p_lo, 0), history_from(), 1);
    IF EXISTS (SELECT 1 FROM missing_block_ranges WHERE from_number < p_hi AND to_number >= first_block) THEN
        RETURN false;
    END IF;
    EXECUTE format('SELECT count(*) FROM %I WHERE number >= $1', history_relname('blocks', p_suffix))
        INTO stored USING first_block;
    RETURN stored = p_hi - first_block;
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
    SELECT count(*) INTO stored FROM blocks WHERE number >= GREATEST(p_lo, 1) AND number < p_hi;
    IF stored <> p_hi - GREATEST(p_lo, 1) THEN
        RETURN false;
    END IF;

    PERFORM set_config('enable_indexscan', 'off', true);
    PERFORM set_config('max_parallel_workers_per_gather', '0', true);

    CREATE TEMP TABLE span_transfers ON COMMIT DROP AS
    SELECT tx_hash, token_address, token_type, from_address, to_address, value, block_number
    FROM token_transfers WHERE block_number >= p_lo AND block_number < p_hi;
    ANALYZE span_transfers;

    INSERT INTO ledger_spans (lo, hi, source) VALUES (p_lo, p_hi, 'rows');
    INSERT INTO ledger_chain (lo, name, value)
    VALUES (p_lo, 'blocks_total', (SELECT count(*) FROM blocks WHERE number >= p_lo AND number < p_hi)),
           (p_lo, 'transactions_total', (SELECT count(*) FROM transactions WHERE block_number >= p_lo AND block_number < p_hi)),
           (p_lo, 'transfers_total', (SELECT count(*) FROM span_transfers));
    INSERT INTO ledger_token (lo, token, transfers, supply)
    SELECT p_lo, token_address, count(*),
           COALESCE(SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
                  - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END), 0)
    FROM span_transfers
    GROUP BY token_address;

    INSERT INTO ledger_address (lo, address, txs, sent, internal, transfers, first_seen, last_seen)
    SELECT p_lo, address, sum(txs), sum(sent), sum(internal), sum(transfers), min(block), max(block)
    FROM (
        SELECT x.address, 1 AS txs, x.sent, 0 AS internal, 0 AS transfers, t.block_number AS block
        FROM transactions t
        LEFT JOIN (
            SELECT tx_hash, array_agg(DISTINCT p.address) AS addresses
            FROM span_transfers CROSS JOIN LATERAL (VALUES (from_address), (to_address)) AS p(address)
            WHERE p.address <> '0x0000000000000000000000000000000000000000'
            GROUP BY tx_hash
        ) part ON part.tx_hash = t.hash
        CROSS JOIN LATERAL (
            SELECT t.from_address AS address, CASE WHEN t.tx_type BETWEEN 0 AND 4 THEN 1 ELSE 0 END AS sent
            UNION ALL
            SELECT t.to_address, 0 WHERE t.to_address IS NOT NULL AND t.to_address <> t.from_address
            UNION ALL
            SELECT a, 0 FROM unnest(part.addresses) AS a WHERE a <> t.from_address AND a IS DISTINCT FROM t.to_address
        ) x
        WHERE t.block_number >= p_lo AND t.block_number < p_hi
        UNION ALL
        SELECT x.address, 0, 0, 0, 1, s.block_number
        FROM span_transfers s CROSS JOIN LATERAL (VALUES (s.from_address, true), (s.to_address, false)) AS x(address, sender)
        WHERE x.address <> '0x0000000000000000000000000000000000000000' AND (x.sender OR s.to_address <> s.from_address)
        UNION ALL
        SELECT x.address, 0, 0, 1, 0, i.block_number
        FROM internal_transactions i CROSS JOIN LATERAL (VALUES (i.from_address, true), (i.to_address, false)) AS x(address, sender)
        WHERE i.block_number >= p_lo AND i.block_number < p_hi
          AND x.address IS NOT NULL AND (x.sender OR i.to_address <> i.from_address)
        UNION ALL
        SELECT address, 0, 0, 0, 0, block_number FROM contracts WHERE block_number >= p_lo AND block_number < p_hi
    ) u
    GROUP BY address;
    RETURN true;
END
$f$;

---- create above / drop below ----

CREATE OR REPLACE FUNCTION history_range_complete(p_suffix TEXT) RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $f$
DECLARE
    p_lo BIGINT;
    p_hi BIGINT;
    first_block BIGINT;
    stored BIGINT;
BEGIN
    SELECT lo, hi INTO p_lo, p_hi FROM history_partitions WHERE suffix = p_suffix;
    IF p_hi IS NULL THEN
        RETURN false;
    END IF;
    first_block := GREATEST(COALESCE(p_lo, 0), history_from());
    IF EXISTS (SELECT 1 FROM missing_block_ranges WHERE from_number < p_hi AND to_number >= first_block) THEN
        RETURN false;
    END IF;
    EXECUTE format('SELECT count(*) FROM %I WHERE number >= $1', history_relname('blocks', p_suffix))
        INTO stored USING first_block;
    RETURN stored = p_hi - first_block;
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
