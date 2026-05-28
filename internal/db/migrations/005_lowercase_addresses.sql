-- Canonicalise every address column to lowercase across the whole indexer
-- schema. The migration is the read-side companion to a writer-side change
-- (batch.go / queries.go INSERTs now wrap address parameters in LOWER()),
-- so once this migration has run the data is canonical and existing
-- LOWER()-on-read patches in the query layer (added in v0.2.0 as a quick
-- fix) can be reverted in a follow-up.
--
-- Background: v0.1.x stored from_address / to_address (and other address
-- columns) in EIP-55 checksum case, the format eth_getTransactionByHash
-- returns. SQL equality is case-sensitive, so every callsite that passes a
-- lowercased address (the industry-canonical wire format used by
-- Etherscan / Blockscout / The Graph / privacy-proxy) got zero rows back.
-- v0.2.0's fix wrapped the read side in LOWER(col) = LOWER($1) — correct
-- but expensive: it bypasses the btree indexes on these columns unless
-- functional indexes are added (which they aren't), and pays the
-- normalisation cost on every read instead of once on write.
--
-- 004_perf_hot_paths.sql already applied this principle to
-- contracts.address and address_stats.address. This migration extends it
-- to the remaining tables: transactions, internal_transactions,
-- token_transfers, logs, tokens, balances.
--
-- Idempotent and resumable. On very large chains operators may prefer to
-- run these UPDATEs in batches off-line before deploying.
--
-- Note for write-side: the corresponding code change wraps every address
-- parameter in INSERT statements with LOWER(...) so newly indexed rows
-- arrive canonical. See internal/db/batch.go and internal/db/queries.go.

UPDATE transactions
SET from_address = LOWER(from_address)
WHERE from_address != LOWER(from_address);

UPDATE transactions
SET to_address = LOWER(to_address)
WHERE to_address IS NOT NULL AND to_address != LOWER(to_address);

UPDATE internal_transactions
SET from_address = LOWER(from_address)
WHERE from_address != LOWER(from_address);

UPDATE internal_transactions
SET to_address = LOWER(to_address)
WHERE to_address IS NOT NULL AND to_address != LOWER(to_address);

UPDATE token_transfers
SET from_address = LOWER(from_address)
WHERE from_address != LOWER(from_address);

UPDATE token_transfers
SET to_address = LOWER(to_address)
WHERE to_address != LOWER(to_address);

UPDATE token_transfers
SET token_address = LOWER(token_address)
WHERE token_address != LOWER(token_address);

UPDATE logs
SET address = LOWER(address)
WHERE address != LOWER(address);

UPDATE tokens
SET address = LOWER(address)
WHERE address != LOWER(address);

-- balances.address is part of the primary key; lowering it can conflict
-- with an existing lowercase row for the same (token_address, block_number).
-- Drop the duplicate-by-case rows (keeping the lower-case one if both
-- exist) before normalising, so the UPDATE doesn't trip the PK constraint.
DELETE FROM balances b1
USING balances b2
WHERE b1.address != LOWER(b1.address)
  AND b2.address = LOWER(b1.address)
  AND b2.token_address = b1.token_address
  AND b2.block_number = b1.block_number;

UPDATE balances
SET address = LOWER(address)
WHERE address != LOWER(address);

UPDATE balances
SET token_address = LOWER(token_address)
WHERE token_address != LOWER(token_address);
