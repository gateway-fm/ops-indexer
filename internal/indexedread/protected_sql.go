package indexedread

// SQL predicates are fixed and parameterized. No indexed receipt, calldata,
// ABI, value or contract metadata is transported as authorization evidence.
const protectedCandidatesSQL = `WITH candidates AS MATERIALIZED (
 SELECT t.hash,t.block_number,t.tx_index,t.from_address,t.to_address,
        b.hash AS block_hash,b.timestamp
 FROM public.transactions t JOIN public.blocks b ON b.number=t.block_number
 WHERE (t.from_address=ANY($1::text[]) OR t.to_address=ANY($1::text[]))
   AND b.timestamp BETWEEN $2::bigint AND $3::bigint
   AND t.block_number BETWEEN $4::bigint AND $5::bigint
)`

const protectedShapeSQL = `(hash ~ '^0x[0-9a-f]{64}$' AND octet_length(hash)=66
 AND block_hash ~ '^0x[0-9a-f]{64}$' AND octet_length(block_hash)=66
 AND from_address ~ '^0x[0-9a-f]{40}$' AND octet_length(from_address)=42
 AND (to_address IS NULL OR (to_address ~ '^0x[0-9a-f]{40}$' AND octet_length(to_address)=42))
 AND block_number>=0 AND tx_index>=0 AND timestamp>=0)`

const protectedDestinationBoundsSQL = protectedCandidatesSQL + `
 SELECT coalesce(bool_and(` + protectedShapeSQL + `),true),min(block_number),max(block_number)
 FROM candidates`

const protectedDestinationsSQL = protectedCandidatesSQL + `, destinations AS (
 SELECT to_address AS address FROM candidates WHERE to_address IS NOT NULL
 UNION
 SELECT c.address FROM candidates t JOIN public.contracts c
 ON c.creation_tx=t.hash AND c.block_number=t.block_number WHERE t.to_address IS NULL
 )
 SELECT CASE WHEN octet_length(address)=42 AND address ~ '^0x[0-9a-f]{40}$' THEN address END
 FROM destinations ORDER BY address COLLATE "C" LIMIT 4097`

const protectedEligibleSQL = protectedCandidatesSQL + `,
 intervals AS (SELECT * FROM pg_catalog.jsonb_to_recordset($6::jsonb) AS i(a text,n bigint,i bigint,z bigint,j bigint)),
 creations AS (SELECT * FROM pg_catalog.jsonb_to_recordset($7::jsonb) AS c(a text,h text,n bigint,i bigint)),
 eligible AS MATERIALIZED (
 SELECT t.* FROM candidates t WHERE
 EXISTS(SELECT 1 FROM intervals p WHERE t.to_address=p.a
   AND (t.block_number,t.tx_index)>=(p.n,p.i) AND (t.block_number,t.tx_index)<(p.z,p.j))
 OR (t.to_address IS NULL AND EXISTS(SELECT 1 FROM creations p
   WHERE t.hash=p.h AND t.block_number=p.n AND t.tx_index=p.i))
 )`

// Variable text is bounded by SQL before transport even for a malformed
// selected row. The separate validity column makes NULL guards fail closed.
const protectedProjectionSQL = `(` + protectedShapeSQL + `),
 CASE WHEN octet_length(address)=42 THEN address END,
 CASE WHEN octet_length(hash)=66 THEN hash END,
 CASE WHEN octet_length(block_hash)=66 THEN block_hash END,
 block_number,tx_index::bigint,timestamp,
 CASE WHEN octet_length(from_address)=42 THEN from_address END,
 CASE WHEN octet_length(to_address)=42 THEN to_address END`

const protectedWitnessesSQL = protectedEligibleSQL + `, witnesses AS (
 SELECT target.a AS address,t.* FROM pg_catalog.unnest($1::text[]) target(a)
 JOIN LATERAL (SELECT * FROM eligible e WHERE e.from_address=target.a OR e.to_address=target.a
 ORDER BY e.block_number,e.tx_index,e.hash COLLATE "C" LIMIT 1) t ON true
 ) SELECT ` + protectedProjectionSQL + ` FROM witnesses ORDER BY address COLLATE "C" LIMIT 4097`

const protectedTransactionsSQL = protectedEligibleSQL + `, selected AS (
 SELECT NULL::text AS address,t.* FROM eligible t
 WHERE ($8::text IS NULL OR t.from_address=$8 OR t.to_address=$8)
 AND ($9::text IS NULL OR t.hash=$9)
 AND (NOT $10::boolean OR (t.block_number,t.tx_index,t.hash COLLATE "C")>($11::bigint,$12::bigint,$13::text COLLATE "C"))
 ORDER BY t.block_number,t.tx_index,t.hash COLLATE "C" LIMIT $14::integer
 ) SELECT ` + protectedProjectionSQL + ` FROM selected ORDER BY block_number,tx_index,hash COLLATE "C"`
