-- Per-token-id NFT instances, backing the ERC-721 inventory view.
--
-- One row per (collection, token_id). `owner` advances to the `to` of the
-- latest transfer for that token (zero address = burned). `token_uri` is
-- captured once at mint and never clobbered (the URI is immutable for our
-- purposes). Aggregate balances live in `balances`; this table is the
-- per-instance truth that `balances` cannot express for NFTs.
CREATE TABLE IF NOT EXISTS nft_tokens (
    token_address TEXT NOT NULL,
    token_id      NUMERIC(78, 0) NOT NULL,
    owner         TEXT NOT NULL,
    token_uri     TEXT,
    block_number  BIGINT NOT NULL,
    updated_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (token_address, token_id)
);

-- Inventory listing: filter by collection, order by token_id, paginate.
CREATE INDEX IF NOT EXISTS idx_nft_tokens_collection ON nft_tokens(token_address, token_id);
