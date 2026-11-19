WITH me AS (SELECT * FROM pg_catalog.pg_roles WHERE rolname=current_user),
 user_schemas AS (
  SELECT * FROM pg_catalog.pg_namespace WHERE nspname !~ '^pg_' AND nspname<>'information_schema'
 ), user_relations AS (
  SELECT c.*,n.nspname FROM pg_catalog.pg_class c JOIN user_schemas n ON n.oid=c.relnamespace
  WHERE c.relkind IN ('r','p','v','m','f','S')
 )
SELECT current_user::text=$1::text AND session_user::text=$1::text
 AND NOT (me.rolsuper OR me.rolcreaterole OR me.rolcreatedb OR me.rolreplication OR me.rolbypassrls)
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=me.oid)
 -- Ownership grants implicit ALTER/DROP authority even without CREATE or ACLs.
 -- Include all current-DB and shared objects (types/domains/collations too),
 -- rather than maintaining an incomplete relation/function ownership list.
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_shdepend dep
   WHERE dep.refclassid='pg_catalog.pg_authid'::regclass AND dep.refobjid=me.oid
     AND dep.deptype='o' AND (dep.dbid=0 OR dep.dbid=(
       SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())))
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_database d WHERE d.datname=current_database() AND d.datdba=me.oid)
 AND NOT pg_catalog.has_database_privilege(current_database(),'CREATE')
 AND pg_catalog.has_database_privilege(current_database(),'CONNECT')
 AND NOT EXISTS(SELECT 1 FROM user_schemas n WHERE n.nspowner=me.oid OR pg_catalog.has_schema_privilege(n.oid,'CREATE'))
 AND NOT EXISTS(SELECT 1 FROM user_relations c WHERE c.relowner=me.oid)
 AND NOT EXISTS(SELECT 1 FROM user_relations c WHERE CASE WHEN c.relkind='S'
   THEN pg_catalog.has_sequence_privilege(c.oid,'USAGE,SELECT,UPDATE') ELSE false END)
 AND NOT EXISTS(SELECT 1 FROM user_relations c WHERE CASE WHEN c.relkind<>'S' THEN (
   pg_catalog.has_table_privilege(c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
   OR pg_catalog.has_any_column_privilege(c.oid,'INSERT,UPDATE,REFERENCES')
   OR pg_catalog.has_table_privilege(c.oid,'SELECT WITH GRANT OPTION')
   OR EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
      AND pg_catalog.has_column_privilege(c.oid,a.attnum,'SELECT WITH GRANT OPTION'))
   OR ((c.nspname<>'public' OR NOT c.relname=ANY($2::text[]))
       AND (pg_catalog.has_table_privilege(c.oid,'SELECT') OR pg_catalog.has_any_column_privilege(c.oid,'SELECT')))
 ) ELSE false END)
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_proc p JOIN user_schemas n ON n.oid=p.pronamespace
   WHERE p.proowner=me.oid OR pg_catalog.has_function_privilege(p.oid,'EXECUTE'))
FROM me
