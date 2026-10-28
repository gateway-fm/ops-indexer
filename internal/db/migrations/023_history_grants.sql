-- 014 created each partitioned history table with CREATE TABLE (LIKE ...), which copies no privileges: what was
-- granted on a table before 014 stayed on <table>_legacy, and a role that could read the table got "permission
-- denied" on it. This copies those grants, exactly as they are on <table>_legacy, to the partitioned table and to
-- every partition made since, and history_partition_create gives each new partition its table's grants. Nothing is
-- granted that a table did not have. A deployment that already dropped <table>_legacy has nothing left to copy.

-- Grants src's privileges to every role src grants them to, besides its owner, on dst; with grant option where src
-- has it.
CREATE OR REPLACE FUNCTION history_copy_grants(src REGCLASS, dst REGCLASS) RETURNS VOID LANGUAGE plpgsql AS $f$
DECLARE
    g RECORD;
BEGIN
    FOR g IN
        SELECT a.privilege_type, a.is_grantable,
               CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END AS grantee
        FROM pg_class c, aclexplode(c.relacl) a
        WHERE c.oid = src AND a.grantee <> c.relowner
    LOOP
        EXECUTE format('GRANT %s ON %s TO %s%s', g.privilege_type, dst, g.grantee,
                       CASE WHEN g.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
    END LOOP;
END
$f$;

DO $m$
DECLARE
    t TEXT;
    p TEXT;
BEGIN
    FOREACH t IN ARRAY history_tables() LOOP
        IF to_regclass(t || '_legacy') IS NULL THEN
            RAISE NOTICE '%_legacy is dropped: no grants to copy to %', t, t;
            CONTINUE;
        END IF;
        PERFORM history_copy_grants((t || '_legacy')::regclass, t::regclass);
        FOR p IN SELECT history_relname(t, suffix) FROM history_partitions WHERE suffix <> 'legacy' LOOP
            IF to_regclass(p) IS NOT NULL THEN
                PERFORM history_copy_grants(t::regclass, p::regclass);
            END IF;
        END LOOP;
    END LOOP;
END
$m$;

-- 014's, plus the grants of each table on its new partition.
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
        PERFORM history_copy_grants(t::regclass, p::regclass);
        EXECUTE format('ALTER TABLE %I ATTACH PARTITION %I FOR VALUES FROM (%s) TO (%s)', t, p, p_lo, p_hi);
    END LOOP;
    INSERT INTO history_partitions (lo, hi, suffix) VALUES (p_lo, p_hi, 'p' || p_lo);
END
$f$;

---- create above / drop below ----

-- The grants stay: revoking them would take from the roles the access they had before 014.
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
DROP FUNCTION IF EXISTS history_copy_grants(REGCLASS, REGCLASS);
