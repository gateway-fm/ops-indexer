-- Ledger compaction (available, off by default). Pruned spans that the node recount
-- verified can be merged, a run of n consecutive ones into one span: its rows are their sums, so totals do not move,
-- and the merged span is recounted from the node like any other, over its larger range. Per-address rows shrink
-- where an address is active in several of the merged spans.

ALTER TABLE ledger_spans DROP CONSTRAINT IF EXISTS ledger_spans_source_check;
ALTER TABLE ledger_spans ADD CONSTRAINT ledger_spans_source_check CHECK (source IN ('rows', 'node', 'merged'));

-- Merges the oldest run of n consecutive pruned spans that are verified (ok or repaired, checked after their last
-- change) and not already merged to n or more; returns the merged span's lo, or NULL when no run qualifies.
CREATE FUNCTION ledger_compact(n INT) RETURNS BIGINT LANGUAGE plpgsql AS $f$
DECLARE
    run_lo BIGINT;
    run_hi BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(4672, 1);
    SELECT lo, hi INTO run_lo, run_hi FROM (
        SELECT s.lo, lead(s.hi, n - 1) OVER w AS hi,
               bool_and(s.check_state IN ('ok', 'repaired') AND s.source <> 'merged' AND s.hi <= (SELECT history_from()))
                   OVER (w ROWS BETWEEN CURRENT ROW AND n - 1 FOLLOWING) AS verified,
               count(*) OVER (w ROWS BETWEEN CURRENT ROW AND n - 1 FOLLOWING) AS spans,
               lead(s.hi, n - 1) OVER w - s.lo AS width,
               sum(s.hi - s.lo) OVER (w ROWS BETWEEN CURRENT ROW AND n - 1 FOLLOWING) AS covered
        FROM ledger_spans s
        WINDOW w AS (ORDER BY s.lo)
    ) r
    WHERE verified AND spans = n AND width = covered
    ORDER BY lo LIMIT 1;
    IF run_lo IS NULL OR n < 2 THEN
        RETURN NULL;
    END IF;

    CREATE TEMP TABLE merged_chain ON COMMIT DROP AS
    SELECT name, sum(value)::BIGINT AS value FROM ledger_chain WHERE lo >= run_lo AND lo < run_hi GROUP BY name;
    CREATE TEMP TABLE merged_token ON COMMIT DROP AS
    SELECT token, sum(transfers)::BIGINT AS transfers, sum(supply) AS supply
    FROM ledger_token WHERE lo >= run_lo AND lo < run_hi GROUP BY token;
    CREATE TEMP TABLE merged_address ON COMMIT DROP AS
    SELECT address, sum(txs)::BIGINT AS txs, sum(sent)::BIGINT AS sent, sum(internal)::BIGINT AS internal,
           sum(transfers)::BIGINT AS transfers, min(first_seen) AS first_seen, max(last_seen) AS last_seen
    FROM ledger_address WHERE lo >= run_lo AND lo < run_hi GROUP BY address;

    DELETE FROM ledger_spans WHERE lo >= run_lo AND lo < run_hi;
    INSERT INTO ledger_spans (lo, hi, source) VALUES (run_lo, run_hi, 'merged');
    INSERT INTO ledger_chain (lo, name, value) SELECT run_lo, name, value FROM merged_chain;
    INSERT INTO ledger_token (lo, token, transfers, supply) SELECT run_lo, token, transfers, supply FROM merged_token;
    INSERT INTO ledger_address (lo, address, txs, sent, internal, transfers, first_seen, last_seen)
    SELECT run_lo, address, txs, sent, internal, transfers, first_seen, last_seen FROM merged_address;
    RETURN run_lo;
END
$f$;

---- create above / drop below ----

DROP FUNCTION ledger_compact(INT);
DO $m$
BEGIN
    IF EXISTS (SELECT 1 FROM ledger_spans WHERE source = 'merged') THEN
        RAISE EXCEPTION 'migration 028 cannot be reverted: the ledger holds merged spans';
    END IF;
END
$m$;
ALTER TABLE ledger_spans DROP CONSTRAINT ledger_spans_source_check;
ALTER TABLE ledger_spans ADD CONSTRAINT ledger_spans_source_check CHECK (source IN ('rows', 'node'));
