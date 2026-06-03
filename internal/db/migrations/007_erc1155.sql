-- Per-(collection, token_id, owner) ERC-1155 balances, backing the multi-token
-- inventory view.
--
-- ERC-1155 differs from ERC-721 in that a single token id is fungible and held
-- by many owners at once, each with a quantity. The per-instance `nft_tokens`
-- table (one owner per id) and the aggregate `balances` table (no id dimension)
-- can express neither, so multi-token balances live here.
--
-- `balance` is maintained by accumulation: the indexer writes signed deltas
-- (credit the transfer's `to`, debit its `from`) and the upsert applies them to
-- the running total. `token_uri` is captured once at mint (uri(id)) and never
-- clobbered by a later, URI-less transfer.
CREATE TABLE IF NOT EXISTS erc1155_holdings (
    token_address TEXT NOT NULL,
    token_id      NUMERIC(78, 0) NOT NULL,
    owner         TEXT NOT NULL,
    balance       NUMERIC(78, 0) NOT NULL DEFAULT 0,
    token_uri     TEXT,
    block_number  BIGINT NOT NULL,
    updated_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (token_address, token_id, owner)
);

-- Inventory listing aggregates by (collection, token_id); the per-owner rows
-- with a positive balance are the live holders.
CREATE INDEX IF NOT EXISTS idx_erc1155_holdings_collection
    ON erc1155_holdings(token_address, token_id);

-- Widen token_transfers uniqueness to admit ERC-1155 TransferBatch rows.
--
-- An ERC-1155 TransferBatch is a single log (one log_index) that moves several
-- token ids at once. The indexer fans it out into one token_transfers row per
-- id, but the original UNIQUE(tx_hash, log_index) collapsed them to one row —
-- every id after the first was silently dropped by ON CONFLICT DO NOTHING.
--
-- Add token_id to the uniqueness so the per-id rows of a batch coexist. The
-- index is NULLS NOT DISTINCT so ERC-20 transfers (token_id IS NULL) still
-- dedupe correctly on re-index — without it each NULL would be treated as
-- unique and re-indexing would duplicate every fungible transfer.
ALTER TABLE token_transfers DROP CONSTRAINT IF EXISTS token_transfers_tx_hash_log_index_key;

CREATE UNIQUE INDEX IF NOT EXISTS token_transfers_tx_log_id_key
    ON token_transfers (tx_hash, log_index, token_id) NULLS NOT DISTINCT;
