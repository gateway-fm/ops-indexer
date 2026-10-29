-- A reverted block's UTC day is recounted from its first block (daily_stats_invalidate), which refuses a day whose
-- first blocks are pruned. The block-count floor kept the blocks a reorg can reach but not the rest of their day: a
-- window shorter than a day pruned into the head's own day, and a one-block reorg at the head then stopped ingest.
-- A range is now pruned only when its newest block is older than the UTC day of the floor block, so every day a
-- reorg can reach is kept whole.

-- The start of the UTC day of the newest stored block at or below the floor (head - prune_floor_gap()); NULL when
-- there is none. A block a reorg can reach is newer, so its day starts no earlier.
CREATE OR REPLACE FUNCTION prune_day_floor() RETURNS BIGINT LANGUAGE sql STABLE AS $f$
    SELECT daily_stats_epoch(daily_stats_day(timestamp)) FROM blocks
    WHERE number <= (SELECT max(number) FROM blocks) - prune_floor_gap()
    ORDER BY number DESC LIMIT 1
$f$;

CREATE OR REPLACE FUNCTION history_range_newest_ts(p_suffix TEXT) RETURNS BIGINT LANGUAGE plpgsql STABLE AS $f$
DECLARE
    ts BIGINT;
BEGIN
    EXECUTE format('SELECT max(timestamp) FROM %I', history_relname('blocks', p_suffix)) INTO ts;
    RETURN ts;
END
$f$;

-- Whether a range lies wholly before the UTC day a reorg can reach.
CREATE OR REPLACE FUNCTION history_range_before_reorg_day(p_suffix TEXT) RETURNS BOOLEAN LANGUAGE sql STABLE AS $f$
    SELECT COALESCE(COALESCE(history_range_newest_ts(p_suffix), -1) < prune_day_floor(), false)
$f$;

-- Moves the watermark past the oldest attached range [lo, hi) and marks it detaching. Refuses unless it is the
-- oldest range, entirely below the reorg floor, its UTC day and the daily_stats watermark, complete, and closed in
-- the ledger.
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
    IF NOT history_range_before_reorg_day(p_suffix) THEN
        RAISE EXCEPTION 'prune_advance: % (below %) reaches the UTC day a reorg can revert into', p_suffix, p_hi;
    END IF;
    IF NOT history_range_complete(p_suffix) THEN
        RAISE EXCEPTION 'prune_advance: % (below %) is missing blocks', p_suffix, p_hi;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM ledger_spans WHERE hi = p_hi) THEN
        RAISE EXCEPTION 'prune_advance: % (below %) is not closed in the ledger', p_suffix, p_hi;
    END IF;
    ts := history_range_newest_ts(p_suffix);

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
DROP FUNCTION IF EXISTS history_range_before_reorg_day(TEXT);
DROP FUNCTION IF EXISTS history_range_newest_ts(TEXT);
DROP FUNCTION IF EXISTS prune_day_floor();
