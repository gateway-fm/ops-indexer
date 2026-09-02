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
