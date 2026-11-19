WITH selected AS (
 SELECT c.*
 FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relname=ANY($1::text[])
), objects AS (
 SELECT c.relname AS name, jsonb_build_object(
  'name',c.relname,'kind',c.relkind::text,'persistence',c.relpersistence::text,
  'row_security',c.relrowsecurity,'force_row_security',c.relforcerowsecurity,
  'inheritance',EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid),
  'rules',EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite rw WHERE rw.ev_class=c.oid),
  'user_triggers',EXISTS(SELECT 1 FROM pg_catalog.pg_trigger tr WHERE tr.tgrelid=c.oid AND NOT tr.tgisinternal),
  'columns',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'name',a.attname,'type_schema',tn.nspname,'type_oid',a.atttypid::bigint,
    'type_mod',a.atttypmod,'not_null',a.attnotnull,'generated',a.attgenerated::text,
    'identity',a.attidentity::text,'select',pg_catalog.has_column_privilege(c.oid,a.attnum,'SELECT')
   ) ORDER BY a.attname COLLATE "C")
   FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
   JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace
   WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),'[]'::jsonb),
  'keys',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'kind',k.contype::text,
    'columns',(SELECT jsonb_agg(a.attname ORDER BY u.ordinality)
      FROM unnest(k.conkey) WITH ORDINALITY u(attnum,ordinality)
      JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum=u.attnum),
    'ready',k.convalidated AND NOT k.condeferrable AND x.indisready AND x.indisvalid AND x.indislive
   ) ORDER BY k.conname COLLATE "C")
   FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_index x ON x.indexrelid=k.conindid
   WHERE k.conrelid=c.oid AND k.contype IN ('p','u')),'[]'::jsonb)
 ) AS object
 FROM selected c
), payload AS (
 SELECT COALESCE(jsonb_agg(object ORDER BY name COLLATE "C"),'[]'::jsonb)::text AS body FROM objects
)
SELECT CASE WHEN octet_length(body)<=$2::integer THEN body ELSE NULL END FROM payload
