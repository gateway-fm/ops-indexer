-- Upsert a flush's worth of token_balances_current rows in ONE statement and return each
-- key's contribution to its token's holder_count.
--
--   $1 token_address[]  $2 address[]  $3 balance[]  $4 block_number[]
--   $5 old_pos[]: 1 if the key existed with balance > 0 before this transaction read it
--      (see tbc_old_pos.sql), else 0. Parallel arrays, one element per DISTINCT key,
--      already lowercased and sorted by (token_address, address).
--
-- Returns (token_address, delta) per key that was written: delta = (new balance > 0) -
-- old_pos, i.e. +1 when the address became a holder, -1 when it stopped being one, 0
-- otherwise. A key whose write the block guard rejects is not returned: the row did not
-- change, so neither did the holder count.
--
-- The guard is the one PRST-4508 settled on: highest block_number wins, >= so a
-- re-delivery of the same block is last-write-wins like the balances insert beside it.
--
-- Was one INSERT per key (0.06 ms x ~97 per block under a 500 tx/s load, 12.9% of database
-- time once holder_count was incremental). Row locks are taken in unnest order = the caller's sort order, the
-- same total order every writer uses (see InsertBalancesBatch).
WITH src AS (
    SELECT LOWER(t) AS token_address, LOWER(a) AS address, v AS balance, b AS block_number, o AS old_pos
    FROM unnest($1::text[], $2::text[], $3::numeric[], $4::bigint[], $5::int[]) AS r(t, a, v, b, o)
),
ins AS (
    INSERT INTO token_balances_current (token_address, address, balance, block_number)
    SELECT token_address, address, balance, block_number FROM src
    ON CONFLICT (token_address, address) DO UPDATE
        SET balance = EXCLUDED.balance,
            block_number = EXCLUDED.block_number
        WHERE EXCLUDED.block_number >= token_balances_current.block_number
    RETURNING token_address, address, (balance > 0)::int AS new_pos
)
SELECT ins.token_address, ins.new_pos - src.old_pos AS delta
FROM ins JOIN src USING (token_address, address)
