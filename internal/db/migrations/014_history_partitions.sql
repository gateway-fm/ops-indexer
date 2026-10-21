-- History tables range-partitioned by block, so retention drops whole partitions instead of deleting rows.
--
-- Each existing table becomes the first partition of a new parent, <table>_legacy, for every block
-- (MINVALUE..MAXVALUE). Its partition constraint is then only "key IS NOT NULL", which the column's NOT NULL
-- already proves, so ATTACH reads no rows. The parent gets the legacy table's indexes under their own names and
-- ATTACH adopts the legacy copies; nothing is rewritten or rebuilt.
--
-- A partitioned table's unique keys must contain the partition key. Those that do (blocks, logs,
-- token_transfers, balances) move to the parent. Those that do not (transactions.hash, blocks.hash,
-- internal_transactions (tx_hash, trace_address), the serial ids) stay on each partition, so they are unique per
-- partition; inserts into those tables use ON CONFLICT DO NOTHING without a target.
--
-- With retention off nothing more happens: the legacy partition takes every block. Retention bounds it once
-- (history_cut_*), then keeps fixed-size partitions ahead of the head and drops whole ones below the window.
--
-- Foreign keys into transactions and blocks go: a dropped partition must not cascade into tokens or contracts,
-- and DeleteBlock removes a reverted block's rows from every table itself.

ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_block_number_fkey;
ALTER TABLE internal_transactions DROP CONSTRAINT IF EXISTS internal_transactions_tx_hash_fkey;
ALTER TABLE op_deposits DROP CONSTRAINT IF EXISTS op_deposits_l2_tx_hash_fkey;
ALTER TABLE tokens DROP CONSTRAINT IF EXISTS tokens_creation_tx_fkey;
ALTER TABLE contracts DROP CONSTRAINT IF EXISTS contracts_creation_tx_fkey;

CREATE OR REPLACE FUNCTION history_tables() RETURNS TEXT[] LANGUAGE sql IMMUTABLE AS $f$
    SELECT ARRAY['blocks', 'transactions', 'logs', 'token_transfers', 'internal_transactions', 'balances']
$f$;

CREATE OR REPLACE FUNCTION history_key(t TEXT) RETURNS TEXT LANGUAGE sql IMMUTABLE AS $f$
    SELECT CASE WHEN t = 'blocks' THEN 'number' ELSE 'block_number' END
$f$;

DO $m$
DECLARE
    t TEXT;
    r RECORD;
BEGIN
    FOREACH t IN ARRAY history_tables() LOOP
        EXECUTE format('ALTER TABLE %I RENAME TO %I', t, t || '_legacy');
        EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING DEFAULTS INCLUDING GENERATED) PARTITION BY RANGE (%I)',
                       t, t || '_legacy', history_key(t));
        FOR r IN
            SELECT c.relname AS name, pg_get_indexdef(i.indexrelid) AS def, i.indisunique AS uniq,
                   con.conname AS constraint_name, con.contype,
                   history_key(t) = ANY (ARRAY(SELECT a.attname::TEXT FROM pg_attribute a
                                               WHERE a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey))) AS has_key
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid AND con.conrelid = i.indrelid
            WHERE i.indrelid = (t || '_legacy')::regclass
        LOOP
            CONTINUE WHEN r.uniq AND NOT r.has_key;
            IF r.constraint_name IS NOT NULL THEN
                EXECUTE format('ALTER TABLE %I RENAME CONSTRAINT %I TO %I', t || '_legacy', r.constraint_name, r.constraint_name || '_legacy');
                EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s', t, r.constraint_name,
                               pg_get_constraintdef((SELECT oid FROM pg_constraint
                                                     WHERE conrelid = (t || '_legacy')::regclass AND conname = r.constraint_name || '_legacy')));
            ELSE
                EXECUTE format('ALTER INDEX %I RENAME TO %I', r.name, r.name || '_legacy');
                EXECUTE replace(r.def, ' ON public.' || t || '_legacy ', ' ON public.' || t || ' ');
            END IF;
        END LOOP;
        FOR r IN SELECT s.relname AS seq, a.attname AS col
                 FROM pg_depend d
                 JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
                 JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
                 WHERE d.refobjid = (t || '_legacy')::regclass AND d.deptype = 'a' LOOP
            EXECUTE format('ALTER SEQUENCE %I OWNED BY %I.%I', r.seq, t, r.col);
        END LOOP;
        EXECUTE format('ALTER TABLE %I ATTACH PARTITION %I FOR VALUES FROM (MINVALUE) TO (MAXVALUE)', t, t || '_legacy');
    END LOOP;
END
$m$;

-- One row per block range; every history table has a partition for each. lo IS NULL is the legacy partition
-- (from MINVALUE); hi IS NULL means unbounded (the legacy partition before the cut).
CREATE TABLE IF NOT EXISTS history_partitions (
    lo BIGINT UNIQUE,
    hi BIGINT,
    suffix TEXT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'attached' CHECK (state IN ('attached', 'detaching', 'detached')),
    detached_at TIMESTAMPTZ
);
INSERT INTO history_partitions (lo, hi, suffix) VALUES (NULL, NULL, 'legacy') ON CONFLICT DO NOTHING;

-- An archived detached partition: the indexer streamed it out before the drop.
CREATE TABLE IF NOT EXISTS history_archives (
    suffix TEXT NOT NULL,
    tbl TEXT NOT NULL,
    location TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    bytes BIGINT NOT NULL,
    sha256 TEXT NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (suffix, tbl)
);

-- pruned_through: every block at or below it is gone on purpose; reads below it answer "pruned".
-- pruned_through_ts: the newest pruned block's timestamp; days up to it are never recomputed.
CREATE TABLE IF NOT EXISTS prune_state (
    id INT PRIMARY KEY CHECK (id = 1),
    pruned_through BIGINT NOT NULL DEFAULT 0,
    pruned_through_ts BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO prune_state (id) VALUES (1) ON CONFLICT DO NOTHING;

-- What pruned rows contributed to the cumulative counters, which are never decremented. Anything that recounts
-- history (the startup re-seeds, daily_stats' first day, the gates) counts the rows above pruned_through and adds
-- these.
CREATE TABLE IF NOT EXISTS prune_offsets (
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    supply NUMERIC NOT NULL DEFAULT 0,
    PRIMARY KEY (kind, key)
);

-- Offsets of one range, computed from its partitions before the watermark moves (a scan, no lock on a parent).
CREATE TABLE IF NOT EXISTS prune_pending (
    suffix TEXT NOT NULL,
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    count BIGINT NOT NULL,
    supply NUMERIC NOT NULL DEFAULT 0,
    PRIMARY KEY (suffix, kind, key)
);

CREATE OR REPLACE FUNCTION pruned_through() RETURNS BIGINT LANGUAGE sql STABLE AS $f$
    SELECT COALESCE((SELECT pruned_through FROM prune_state WHERE id = 1), 0)
$f$;

-- The first retained block: rows below it may still be attached while their range is detaching.
CREATE OR REPLACE FUNCTION history_from() RETURNS BIGINT LANGUAGE sql STABLE AS $f$
    SELECT CASE WHEN pruned_through() > 0 THEN pruned_through() + 1 ELSE 0 END
$f$;

CREATE OR REPLACE FUNCTION prune_offset(k TEXT, key_ TEXT) RETURNS BIGINT LANGUAGE sql STABLE AS $f$
    SELECT COALESCE((SELECT count FROM prune_offsets WHERE kind = k AND key = key_), 0)
$f$;

-- Reorgs revert at most 128 blocks from the head (maxReorgDepth); nothing within 100x that is ever pruned.
CREATE OR REPLACE FUNCTION prune_floor_gap() RETURNS BIGINT LANGUAGE sql IMMUTABLE AS $f$ SELECT 12800::BIGINT $f$;

-- A new empty partition [lo, hi) for every history table, with the unique keys that cannot live on the parent.
-- ATTACH of an empty table takes SHARE UPDATE EXCLUSIVE on the parent, so ingest keeps running.
CREATE OR REPLACE FUNCTION history_partition_create(p_lo BIGINT, p_hi BIGINT) RETURNS VOID LANGUAGE plpgsql AS $f$
DECLARE
    t TEXT;
    p TEXT;
BEGIN
    FOREACH t IN ARRAY history_tables() LOOP
        p := t || '_p' || p_lo;
        EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING DEFAULTS INCLUDING GENERATED)', p, t);
        IF t = 'blocks' THEN
            EXECUTE format('ALTER TABLE %I ADD UNIQUE (hash)', p);
        ELSIF t = 'transactions' THEN
            EXECUTE format('ALTER TABLE %I ADD PRIMARY KEY (hash)', p);
        ELSIF t = 'internal_transactions' THEN
            EXECUTE format('ALTER TABLE %I ADD UNIQUE (tx_hash, trace_address)', p);
        END IF;
        EXECUTE format('ALTER TABLE %I ATTACH PARTITION %I FOR VALUES FROM (%s) TO (%s)', t, p, p_lo, p_hi);
    END LOOP;
    INSERT INTO history_partitions (lo, hi, suffix) VALUES (p_lo, p_hi, 'p' || p_lo);
END
$f$;

-- Partitions of size_blocks above the newest one until one starts beyond upto. A no-op before the cut.
-- size_blocks 0 repeats the newest partition's size (retention switched off after a cut).
CREATE OR REPLACE FUNCTION history_partitions_ensure(upto BIGINT, size_blocks BIGINT) RETURNS INT LANGUAGE plpgsql AS $f$
DECLARE
    top BIGINT;
    made INT := 0;
BEGIN
    SELECT max(hi) INTO top FROM history_partitions;
    IF top IS NULL OR EXISTS (SELECT 1 FROM history_partitions WHERE suffix = 'legacy' AND hi IS NULL) THEN
        RETURN 0;
    END IF;
    IF size_blocks <= 0 THEN
        SELECT hi - lo INTO size_blocks FROM history_partitions WHERE lo IS NOT NULL ORDER BY lo DESC LIMIT 1;
    END IF;
    WHILE top <= upto LOOP
        PERFORM history_partition_create(top, top + size_blocks);
        top := top + size_blocks;
        made := made + 1;
    END LOOP;
    RETURN made;
END
$f$;

-- The cut, step 1: no legacy row may reach cut_at. NOT VALID needs no scan; new rows are checked at once. The
-- caller then runs VALIDATE CONSTRAINT history_cut on each legacy table (a scan under SHARE UPDATE EXCLUSIVE).
CREATE OR REPLACE FUNCTION history_cut_constrain(cut_at BIGINT) RETURNS VOID LANGUAGE plpgsql AS $f$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY history_tables() LOOP
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS history_cut', t || '_legacy');
        EXECUTE format('ALTER TABLE %I ADD CONSTRAINT history_cut CHECK (%I < %s) NOT VALID', t || '_legacy', history_key(t), cut_at);
    END LOOP;
END
$f$;

-- The cut, step 2: re-attach the legacy partitions bounded at cut_at (the validated check proves the bound, so no
-- scan) and create the first partitions above. One transaction; the DETACH takes ACCESS EXCLUSIVE on each parent.
CREATE OR REPLACE FUNCTION history_cut_finish(cut_at BIGINT, size_blocks BIGINT) RETURNS VOID LANGUAGE plpgsql AS $f$
DECLARE
    t TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM history_partitions WHERE suffix = 'legacy' AND hi IS NULL) THEN
        RAISE EXCEPTION 'history_cut_finish: the legacy partitions are already bounded';
    END IF;
    FOREACH t IN ARRAY history_tables() LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = (t || '_legacy')::regclass
                       AND conname = 'history_cut' AND convalidated
                       AND pg_get_constraintdef(oid) = format('CHECK ((%s < %s))', history_key(t), cut_at)) THEN
            RAISE EXCEPTION 'history_cut_finish: %_legacy has no validated history_cut check at %', t, cut_at;
        END IF;
        EXECUTE format('ALTER TABLE %I DETACH PARTITION %I', t, t || '_legacy');
        EXECUTE format('ALTER TABLE %I ATTACH PARTITION %I FOR VALUES FROM (MINVALUE) TO (%s)', t, t || '_legacy', cut_at);
    END LOOP;
    UPDATE history_partitions SET hi = cut_at WHERE suffix = 'legacy';
    PERFORM history_partition_create(cut_at, cut_at + size_blocks);
END
$f$;

-- The table of history table t for a range suffix.
CREATE OR REPLACE FUNCTION history_relname(t TEXT, p_suffix TEXT) RETURNS TEXT LANGUAGE sql IMMUTABLE AS $f$
    SELECT t || '_' || p_suffix
$f$;

-- Counter offsets of the oldest attached range, from its own partitions.
CREATE OR REPLACE FUNCTION prune_prepare(p_suffix TEXT) RETURNS VOID LANGUAGE plpgsql AS $f$
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

-- Moves the watermark past the oldest attached range [lo, hi): applies its prepared offsets and marks it
-- detaching, in one transaction. From here reads below hi answer "pruned"; the caller detaches and drops it.
-- Refuses unless it is the oldest range, entirely below both the reorg floor and the daily_stats watermark, and
-- its offsets are prepared.
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

---- create above / drop below ----

-- Back to plain tables while the legacy partition holds all history: refuses once a range was pruned.
DO $m$
DECLARE
    t TEXT;
    r RECORD;
BEGIN
    IF EXISTS (SELECT 1 FROM prune_state WHERE pruned_through > 0) THEN
        RAISE EXCEPTION 'history was pruned: 014 cannot be reverted in place';
    END IF;
    FOREACH t IN ARRAY history_tables() LOOP
        EXECUTE format('ALTER TABLE %I DETACH PARTITION %I', t, t || '_legacy');
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS history_cut', t || '_legacy');
        EXECUTE format('INSERT INTO %I SELECT * FROM %I', t || '_legacy', t);
        FOR r IN SELECT s.relname AS seq, a.attname AS col
                 FROM pg_depend d
                 JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
                 JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
                 WHERE d.refobjid = t::regclass AND d.deptype = 'a' LOOP
            EXECUTE format('ALTER SEQUENCE %I OWNED BY %I.%I', r.seq, t || '_legacy', r.col);
        END LOOP;
        EXECUTE format('DROP TABLE %I CASCADE', t);
        EXECUTE format('ALTER TABLE %I RENAME TO %I', t || '_legacy', t);
        FOR r IN SELECT conname FROM pg_constraint WHERE conrelid = t::regclass AND conname LIKE '%\_legacy' LOOP
            EXECUTE format('ALTER TABLE %I RENAME CONSTRAINT %I TO %I', t, r.conname, left(r.conname, length(r.conname) - 7));
        END LOOP;
        FOR r IN SELECT c.relname AS name FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                 WHERE i.indrelid = t::regclass AND c.relname LIKE '%\_legacy' LOOP
            EXECUTE format('ALTER INDEX %I RENAME TO %I', r.name, left(r.name, length(r.name) - 7));
        END LOOP;
    END LOOP;
END
$m$;
DROP FUNCTION IF EXISTS prune_advance(TEXT);
DROP FUNCTION IF EXISTS prune_prepare(TEXT);
DROP FUNCTION IF EXISTS history_relname(TEXT, TEXT);
DROP FUNCTION IF EXISTS history_cut_finish(BIGINT, BIGINT);
DROP FUNCTION IF EXISTS history_cut_constrain(BIGINT);
DROP FUNCTION IF EXISTS history_partitions_ensure(BIGINT, BIGINT);
DROP FUNCTION IF EXISTS history_partition_create(BIGINT, BIGINT);
DROP FUNCTION IF EXISTS prune_floor_gap();
DROP FUNCTION IF EXISTS prune_offset(TEXT, TEXT);
DROP FUNCTION IF EXISTS history_from();
DROP FUNCTION IF EXISTS pruned_through();
DROP TABLE IF EXISTS prune_pending;
DROP TABLE IF EXISTS prune_offsets;
DROP TABLE IF EXISTS prune_state;
DROP TABLE IF EXISTS history_archives;
DROP TABLE IF EXISTS history_partitions;
DROP FUNCTION IF EXISTS history_key(TEXT);
DROP FUNCTION IF EXISTS history_tables();
ALTER TABLE transactions ADD CONSTRAINT transactions_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE NOT VALID;
ALTER TABLE internal_transactions ADD CONSTRAINT internal_transactions_tx_hash_fkey FOREIGN KEY (tx_hash) REFERENCES transactions(hash) ON DELETE CASCADE NOT VALID;
ALTER TABLE op_deposits ADD CONSTRAINT op_deposits_l2_tx_hash_fkey FOREIGN KEY (l2_tx_hash) REFERENCES transactions(hash) ON DELETE CASCADE NOT VALID;
ALTER TABLE tokens ADD CONSTRAINT tokens_creation_tx_fkey FOREIGN KEY (creation_tx) REFERENCES transactions(hash) ON DELETE CASCADE NOT VALID;
ALTER TABLE contracts ADD CONSTRAINT contracts_creation_tx_fkey FOREIGN KEY (creation_tx) REFERENCES transactions(hash) ON DELETE CASCADE NOT VALID;
