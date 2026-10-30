-- prune_day_floor() read the day from the newest stored block at or below the floor (head - prune_floor_gap()). When
-- an earlier build had pruned through the floor or past it, no stored block was there and it returned NULL: the
-- start did not warn about a partly pruned day the reorg window reaches, and pruning, which treats NULL as "not
-- before the reorg day", only stopped. With nothing stored at or below the floor, the floor's day is now taken from
-- the pruned watermark's block: a block between the watermark and the floor is no older, so its day starts no
-- earlier, and every check that compares with the floor's day stays as strict.
CREATE OR REPLACE FUNCTION prune_day_floor() RETURNS BIGINT LANGUAGE sql STABLE AS $f$
    SELECT COALESCE(
        (SELECT daily_stats_epoch(daily_stats_day(timestamp)) FROM blocks
         WHERE number <= (SELECT max(number) FROM blocks) - prune_floor_gap()
         ORDER BY number DESC LIMIT 1),
        (SELECT daily_stats_epoch(daily_stats_day(pruned_through_ts)) FROM prune_state
         WHERE id = 1 AND pruned_through > 0 AND EXISTS (SELECT 1 FROM blocks)))
$f$;

---- create above / drop below ----

CREATE OR REPLACE FUNCTION prune_day_floor() RETURNS BIGINT LANGUAGE sql STABLE AS $f$
    SELECT daily_stats_epoch(daily_stats_day(timestamp)) FROM blocks
    WHERE number <= (SELECT max(number) FROM blocks) - prune_floor_gap()
    ORDER BY number DESC LIMIT 1
$f$;
