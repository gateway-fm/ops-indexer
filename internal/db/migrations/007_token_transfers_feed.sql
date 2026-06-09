-- Indexes backing the global "Token Transfers" feed (ListAllTokenTransfers).
-- The feed orders every transfer chain-wide by (block_number DESC, log_index DESC)
-- with an optional token_type filter. Without these, the feed and its COUNT(*)
-- fall back to a full scan + sort of token_transfers on every page load.

-- Unfiltered feed ordering + count.
CREATE INDEX IF NOT EXISTS idx_transfer_block_log
    ON token_transfers (block_number DESC, log_index DESC);

-- token_type-filtered feed ordering + count (All / ERC20 / ERC721 / ERC1155).
CREATE INDEX IF NOT EXISTS idx_transfer_tokentype_block_log
    ON token_transfers (token_type, block_number DESC, log_index DESC);
