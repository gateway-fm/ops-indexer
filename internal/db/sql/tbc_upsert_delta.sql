-- Upsert one (token, address) row of token_balances_current and return this
-- write's contribution to that token's holder_count.
--
--   $1 token_address   $2 address   $3 balance   $4 block_number
--   $5 old_pos: 1 if the row existed with balance > 0 before this transaction
--      read it (see tbc_old_pos.sql), else 0
--
-- Returns (new balance > 0) - old_pos: +1 when the address became a holder,
-- -1 when it stopped being one, 0 otherwise. When the block guard rejects the
-- write no row is returned and the caller counts 0: the row did not change, so
-- neither did the holder count.
--
-- The guard is the same one PRST-4508 settled on: highest block_number wins,
-- >= so a re-delivery of the same block is last-write-wins like the balances
-- insert beside it.
--
-- This file is go:embed-ed by holder_count.go AND executed verbatim by
-- tools/gates.sh (functional gate): the text the binary runs is the text the
-- gate tests.
INSERT INTO token_balances_current (token_address, address, balance, block_number)
VALUES (LOWER($1), LOWER($2), $3, $4)
ON CONFLICT (token_address, address) DO UPDATE
    SET balance = EXCLUDED.balance,
        block_number = EXCLUDED.block_number
    WHERE EXCLUDED.block_number >= token_balances_current.block_number
RETURNING (balance > 0)::int - $5::int
