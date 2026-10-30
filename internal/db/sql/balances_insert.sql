-- Append a flush's worth of balance history rows in ONE statement.
--
--   $1 address[]  $2 token_address[]  $3 block_number[]  $4 balance[]
--   Parallel arrays, one element per DISTINCT (address, token_address, block_number),
--   lowercased, sorted by (token_address, address, block_number) -- the caller collapses
--   duplicates keeping the last arrival, because ON CONFLICT DO UPDATE cannot touch the
--   same row twice in one statement.
--
-- Was one INSERT per row (0.12 ms x ~97 per block under a 500 tx/s load, 24.8% of database
-- time once holder_count was incremental). Semantics unchanged: last write wins for an identical key.
--
-- A row for a pruned block has no partition to go to; it is left out here, and its current balance is still
-- applied.
INSERT INTO balances (address, token_address, block_number, balance)
SELECT LOWER(a), LOWER(t), b, v
FROM unnest($1::text[], $2::text[], $3::bigint[], $4::numeric[]) AS r(a, t, b, v)
WHERE b >= (SELECT history_from())
ON CONFLICT (address, token_address, block_number) DO UPDATE SET balance = EXCLUDED.balance
