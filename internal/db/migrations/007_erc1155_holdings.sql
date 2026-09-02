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
