-- Totals as a ledger: what each closed span of blocks contributed to
-- the chain and token counters, one row per span and key. A span is final and complete when it closes, and closes
-- in block order, so the closed spans are a prefix of the chain. Pruned history is the spans at or below the
-- watermark; prune_offsets becomes their sum, so every recount stays "retained rows + offsets".
-- Replaces the folded prune_offsets and prune_pending: refuses a database that holds folded offsets.

DO $m$
BEGIN
    IF EXISTS (SELECT 1 FROM prune_offsets) OR EXISTS (SELECT 1 FROM prune_pending) THEN
        RAISE EXCEPTION 'migration 019 (ledger) cannot convert folded prune_offsets: this database was pruned by a build without the ledger; restore an unpruned copy';
    END IF;
END
$m$;

CREATE TABLE ledger_spans (
    lo BIGINT PRIMARY KEY,
    hi BIGINT NOT NULL UNIQUE CHECK (hi > lo),
    source TEXT NOT NULL CHECK (source IN ('rows', 'node')),
    closed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger_chain (
    lo BIGINT NOT NULL REFERENCES ledger_spans (lo) ON DELETE CASCADE,
    name TEXT NOT NULL,
    value BIGINT NOT NULL,
    PRIMARY KEY (lo, name)
);

CREATE TABLE ledger_token (
    lo BIGINT NOT NULL REFERENCES ledger_spans (lo) ON DELETE CASCADE,
    token TEXT NOT NULL,
    transfers BIGINT NOT NULL,
    supply NUMERIC NOT NULL,
    PRIMARY KEY (lo, token)
);

DROP FUNCTION prune_prepare(TEXT);
DROP TABLE prune_pending;
DROP TABLE prune_offsets;

CREATE VIEW prune_offsets AS
SELECT 'chain'::TEXT AS kind, c.name AS key, sum(c.value)::BIGINT AS count, 0::NUMERIC AS supply
FROM ledger_chain c JOIN ledger_spans s USING (lo)
WHERE s.hi <= history_from()
GROUP BY c.name
UNION ALL
SELECT 'token', t.token, sum(t.transfers)::BIGINT, sum(t.supply)
FROM ledger_token t JOIN ledger_spans s USING (lo)
WHERE s.hi <= history_from()
GROUP BY t.token;

-- The span after the closed prefix: from the prefix's end (or first_block) to the next grid point or partition bound.
CREATE FUNCTION ledger_next_span(span_blocks BIGINT, first_block BIGINT, OUT lo BIGINT, OUT hi BIGINT) LANGUAGE plpgsql STABLE AS $f$
BEGIN
    lo := COALESCE((SELECT max(s.hi) FROM ledger_spans s), GREATEST(first_block, history_from()));
    hi := LEAST((lo / span_blocks + 1) * span_blocks,
                (SELECT min(b) FROM (SELECT p.lo AS b FROM history_partitions p UNION ALL SELECT p.hi FROM history_partitions p) x
                 WHERE b > lo));
END
$f$;

-- Closes [p_lo, p_hi) from its rows if it is the next span, below the reorg floor and complete; reports whether it did.
CREATE FUNCTION ledger_close_span(p_lo BIGINT, p_hi BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql AS $f$
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
    RETURN true;
END
$f$;

-- Moves the watermark past the oldest attached range [lo, hi) and marks it detaching. Refuses unless it is the
-- oldest range, entirely below the reorg floor and the daily_stats watermark, complete, and closed in the ledger.
CREATE OR REPLACE FUNCTION prune_advance(p_suffix TEXT) RETURNS BIGINT LANGUAGE plpgsql AS $f$
DECLARE
    p_hi BIGINT;
    head BIGINT;
    dsw BIGINT;
    ts BIGINT;
BEGIN
    IF NOT pg_try_advisory_xact_lock(4671, 1) THEN
        RAISE EXCEPTION 'prune_advance: another pruner holds the lock';
    END IF;
    SELECT hi INTO p_hi FROM history_partitions WHERE suffix = p_suffix AND state = 'attached';
    IF p_hi IS NULL THEN
        RAISE EXCEPTION 'prune_advance: % is not an attached bounded range', p_suffix;
    END IF;
    IF EXISTS (SELECT 1 FROM history_partitions WHERE state = 'attached' AND COALESCE(lo, -1) < COALESCE((SELECT lo FROM history_partitions WHERE suffix = p_suffix), -1)) THEN
        RAISE EXCEPTION 'prune_advance: % is not the oldest attached range', p_suffix;
    END IF;
    SELECT max(number) INTO head FROM blocks;
    SELECT last_block INTO dsw FROM daily_stats_watermark WHERE id = 1;
    IF head IS NULL OR dsw IS NULL OR p_hi - 1 > LEAST(head - prune_floor_gap(), dsw) THEN
        RAISE EXCEPTION 'prune_advance: % (below %) is not entirely below the reorg floor and the daily_stats watermark', p_suffix, p_hi;
    END IF;
    IF NOT history_range_complete(p_suffix) THEN
        RAISE EXCEPTION 'prune_advance: % (below %) is missing blocks', p_suffix, p_hi;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM ledger_spans WHERE hi = p_hi) THEN
        RAISE EXCEPTION 'prune_advance: % (below %) is not closed in the ledger', p_suffix, p_hi;
    END IF;
    EXECUTE format('SELECT max(timestamp) FROM %I', history_relname('blocks', p_suffix)) INTO ts;

    DELETE FROM missing_block_ranges WHERE to_number < p_hi;
    UPDATE missing_block_ranges SET from_number = p_hi WHERE from_number < p_hi;
    UPDATE history_partitions SET state = 'detaching' WHERE suffix = p_suffix;
    UPDATE prune_state SET pruned_through = GREATEST(pruned_through, p_hi - 1),
                           pruned_through_ts = GREATEST(pruned_through_ts, COALESCE(ts, 0)), updated_at = now()
    WHERE id = 1;
    RETURN p_hi - 1;
END
$f$;

---- create above / drop below ----

DO $m$
BEGIN
    IF pruned_through() > 0 THEN
        RAISE EXCEPTION 'migration 019 cannot be reverted: history at or below block % is pruned and its offsets live in the ledger', pruned_through();
    END IF;
END
$m$;

DROP FUNCTION ledger_close_span(BIGINT, BIGINT);
DROP FUNCTION ledger_next_span(BIGINT, BIGINT);
DROP VIEW prune_offsets;
DROP TABLE ledger_token;
DROP TABLE ledger_chain;
DROP TABLE ledger_spans;

CREATE TABLE prune_offsets (
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    supply NUMERIC NOT NULL DEFAULT 0,
    PRIMARY KEY (kind, key)
);
CREATE TABLE prune_pending (
    suffix TEXT NOT NULL,
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    count BIGINT NOT NULL,
    supply NUMERIC NOT NULL DEFAULT 0,
    PRIMARY KEY (suffix, kind, key)
);
CREATE FUNCTION prune_prepare(p_suffix TEXT) RETURNS VOID LANGUAGE plpgsql AS $f$
BEGIN
    DELETE FROM prune_pending WHERE suffix = p_suffix;
    EXECUTE format($q$
        INSERT INTO prune_pending (suffix, kind, key, count, supply)
        SELECT %L, 'token', token_address, count(*),
               COALESCE(SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
                      - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END), 0)
        FROM %I GROUP BY token_address$q$, p_suffix, history_relname('token_transfers', p_suffix));
    EXECUTE format($q$
        INSERT INTO prune_pending (suffix, kind, key, count)
        VALUES (%1$L, 'chain', 'blocks_total', (SELECT count(*) FROM %2$I)),
               (%1$L, 'chain', 'transactions_total', (SELECT count(*) FROM %3$I)),
               (%1$L, 'chain', 'transfers_total', (SELECT count(*) FROM %4$I))$q$,
        p_suffix, history_relname('blocks', p_suffix), history_relname('transactions', p_suffix),
        history_relname('token_transfers', p_suffix));
END
$f$;
CREATE OR REPLACE FUNCTION prune_advance(p_suffix TEXT) RETURNS BIGINT LANGUAGE plpgsql AS $f$
DECLARE
    p_hi BIGINT;
    head BIGINT;
    dsw BIGINT;
    ts BIGINT;
BEGIN
    IF NOT pg_try_advisory_xact_lock(4671, 1) THEN
        RAISE EXCEPTION 'prune_advance: another pruner holds the lock';
    END IF;
    SELECT hi INTO p_hi FROM history_partitions WHERE suffix = p_suffix AND state = 'attached';
    IF p_hi IS NULL THEN
        RAISE EXCEPTION 'prune_advance: % is not an attached bounded range', p_suffix;
    END IF;
    IF EXISTS (SELECT 1 FROM history_partitions WHERE state = 'attached' AND COALESCE(lo, -1) < COALESCE((SELECT lo FROM history_partitions WHERE suffix = p_suffix), -1)) THEN
        RAISE EXCEPTION 'prune_advance: % is not the oldest attached range', p_suffix;
    END IF;
    SELECT max(number) INTO head FROM blocks;
    SELECT last_block INTO dsw FROM daily_stats_watermark WHERE id = 1;
    IF head IS NULL OR dsw IS NULL OR p_hi - 1 > LEAST(head - prune_floor_gap(), dsw) THEN
        RAISE EXCEPTION 'prune_advance: % (below %) is not entirely below the reorg floor and the daily_stats watermark', p_suffix, p_hi;
    END IF;
    IF NOT history_range_complete(p_suffix) THEN
        RAISE EXCEPTION 'prune_advance: % (below %) is missing blocks', p_suffix, p_hi;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM prune_pending WHERE suffix = p_suffix AND kind = 'chain') THEN
        RAISE EXCEPTION 'prune_advance: offsets for % not prepared', p_suffix;
    END IF;
    EXECUTE format('SELECT max(timestamp) FROM %I', history_relname('blocks', p_suffix)) INTO ts;

    INSERT INTO prune_offsets AS o (kind, key, count, supply)
    SELECT kind, key, count, supply FROM prune_pending WHERE suffix = p_suffix
    ON CONFLICT (kind, key) DO UPDATE SET count = o.count + EXCLUDED.count, supply = o.supply + EXCLUDED.supply;
    DELETE FROM prune_pending WHERE suffix = p_suffix;

    DELETE FROM missing_block_ranges WHERE to_number < p_hi;
    UPDATE missing_block_ranges SET from_number = p_hi WHERE from_number < p_hi;
    UPDATE history_partitions SET state = 'detaching' WHERE suffix = p_suffix;
    UPDATE prune_state SET pruned_through = GREATEST(pruned_through, p_hi - 1),
                           pruned_through_ts = GREATEST(pruned_through_ts, COALESCE(ts, 0)), updated_at = now()
    WHERE id = 1;
    RETURN p_hi - 1;
END
$f$;
