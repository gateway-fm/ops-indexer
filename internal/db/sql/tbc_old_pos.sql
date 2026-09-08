-- For a set of (token, address) keys, which ones currently hold a positive
-- balance. Keys absent from token_balances_current are simply not returned:
-- the caller treats them as old_pos = 0.
--
--   $1 token_address[]   $2 address[]   (parallel arrays, already lowercased)
--
-- Read under the per-key advisory locks of tbc_lock_keys.sql, so no other
-- balance writer can change these rows between this read and the upsert that
-- follows in the same transaction. Without that lock two flushes racing on a
-- key not yet in the table would both read "absent" and both count +1.
SELECT c.token_address, c.address, (c.balance > 0)
FROM token_balances_current c
JOIN unnest($1::text[], $2::text[]) AS k(token_address, address)
  ON c.token_address = k.token_address AND c.address = k.address
