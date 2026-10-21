-- A range is pruned only once it holds every block: the daily_stats watermark starts at the newest day's first
-- stored block, so during a catch-up from genesis it sits above ranges catch-up has not filled yet.

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

DROP FUNCTION IF EXISTS history_range_complete(TEXT);
