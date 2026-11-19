-- Stable seek keys for the existing V1 catalog and feed orderings. The
-- block-first history indexes propagate to range partitions.
CREATE INDEX IF NOT EXISTS idx_v1_tx_feed ON transactions(block_number DESC,tx_index DESC,hash DESC);
CREATE INDEX IF NOT EXISTS idx_v1_internal_feed ON internal_transactions(block_number DESC,id DESC,tx_hash DESC,trace_address DESC);
CREATE INDEX IF NOT EXISTS idx_v1_tokens_catalog ON tokens(holder_count DESC,address ASC);
CREATE INDEX IF NOT EXISTS idx_v1_addresses_catalog ON address_stats(tx_count DESC,address ASC);
CREATE INDEX IF NOT EXISTS idx_v1_holders_catalog ON token_balances_current(token_address,balance DESC,address ASC) WHERE balance>0;
CREATE INDEX IF NOT EXISTS idx_v1_contracts_catalog ON contracts(created_at DESC,address ASC) WHERE is_verified=true;
CREATE INDEX IF NOT EXISTS idx_v1_inventory_catalog ON nft_tokens(LOWER(token_address),token_id);
CREATE INDEX IF NOT EXISTS idx_v1_deposit_feed ON op_deposits(l1_block_number DESC,l2_tx_hash DESC);

---- create above / drop below ----
DROP INDEX IF EXISTS idx_v1_deposit_feed;
DROP INDEX IF EXISTS idx_v1_inventory_catalog;
DROP INDEX IF EXISTS idx_v1_contracts_catalog;
DROP INDEX IF EXISTS idx_v1_holders_catalog;
DROP INDEX IF EXISTS idx_v1_addresses_catalog;
DROP INDEX IF EXISTS idx_v1_tokens_catalog;
DROP INDEX IF EXISTS idx_v1_internal_feed;
DROP INDEX IF EXISTS idx_v1_tx_feed;
