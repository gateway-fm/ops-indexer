-- Apply one token's holder_count movement from a balances flush, in the same
-- transaction as the token_balances_current writes that produced it.
--
--   $1 token_address (lowercased)   $2 delta (sum of tbc_upsert_delta.sql results)
--
-- One statement per token, in sorted token order (the caller sorts), never
-- UPDATE ... FROM over a set: that would take tokens row locks in plan order,
-- and every other writer of tokens rows takes them sorted (see DeleteBlock and
-- InsertBlockDataBatch), so this has to as well.
--
-- Floors at zero like transfer_count does: a negative holder count is an
-- accounting error made nonsensical; zero is the same error made visible and
-- sane. The gates compare exactly, so the floor cannot hide drift from them.
UPDATE tokens
SET holder_count = GREATEST(COALESCE(holder_count, 0) + $2::int, 0)
WHERE address = $1
