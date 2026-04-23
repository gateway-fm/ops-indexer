-- Add a materialized `categories` bitfield column on transactions.
--
-- Prior implementation computed 4 EXISTS subqueries per row at read time in
-- GetTransactionWithCategories (see RD-855 Phase 0 inventory, N+1 risk #4).
-- Materializing at index time turns a per-row 4-subquery pattern into a single
-- scan of one smallint column.
--
-- Bit layout:
--   bit 0 (1)  = coin_transfer      (native-value transfer)
--   bit 1 (2)  = contract_creation  (to IS NULL and code was deployed)
--   bit 2 (4)  = contract_call      (call to an address with code)
--   bit 3 (8)  = token_transfer     (emitted an ERC20/721/1155 Transfer log)
-- Future categories (e.g. native-coin burn, system tx) can be added without
-- migrating; just allocate a new bit.
--
-- The indexer populates this column when writing new rows (see
-- internal/indexer). This migration also backfills existing rows — a single
-- UPDATE with correlated EXISTS subqueries. On multi-million-row tables this
-- may take minutes; it runs once per deployment upgrade.

ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS categories SMALLINT NOT NULL DEFAULT 0;

-- Backfill in a single pass — safe to re-run because the bits are idempotent.
-- Skip rows already populated (categories != 0) to make re-runs cheap.
UPDATE transactions SET categories =
    -- coin_transfer: value > 0 AND input is empty or 0x
    (CASE WHEN value > 0 AND (input_data IS NULL OR input_data = '' OR input_data = '0x') THEN 1 ELSE 0 END)
    -- contract_creation: to is NULL and status = 1 (reverted deploys don't count)
    | (CASE WHEN to_address IS NULL AND status = 1 THEN 2 ELSE 0 END)
    -- contract_call: to is not null AND input has a function selector (>= 4 bytes)
    | (CASE WHEN to_address IS NOT NULL
            AND input_data IS NOT NULL
            AND length(input_data) >= 10  -- "0x" + 8 hex = 4 bytes
         THEN 4 ELSE 0 END)
    -- token_transfer: at least one token_transfers row for this tx
    | (CASE WHEN EXISTS (
            SELECT 1 FROM token_transfers tt WHERE tt.tx_hash = transactions.hash
         ) THEN 8 ELSE 0 END)
WHERE categories = 0;

-- Index for filtering by category. Partial indexes per bit could be added
-- later if a specific category becomes a hot filter; a plain BTREE on the
-- full value is fine for bitwise AND queries given the small cardinality.
CREATE INDEX IF NOT EXISTS idx_tx_categories ON transactions(categories);

---- create above / drop below ----

DROP INDEX IF EXISTS idx_tx_categories;
ALTER TABLE transactions DROP COLUMN IF EXISTS categories;
