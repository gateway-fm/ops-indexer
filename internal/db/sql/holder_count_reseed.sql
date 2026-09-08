-- Startup re-seed of tokens.holder_count from token_balances_current, one
-- index-only count per token over idx_tbc_holders (partial: balance > 0).
-- Runs once per process start after reconcileTokenBalancesCurrent, alongside
-- the chain_counters and transfer-counter re-seeds that already happen there,
-- so a build without the incremental writer (or a crash mid-flush) is repaired
-- on the next start rather than never. Writes only rows that differ.
UPDATE tokens t
SET holder_count = c.n
FROM (
    SELECT t2.address,
           (SELECT COUNT(*) FROM token_balances_current c
             WHERE c.token_address = t2.address AND c.balance > 0)::int AS n
    FROM tokens t2
) AS c
WHERE c.address = t.address
  AND t.holder_count IS DISTINCT FROM c.n
