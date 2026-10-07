-- logs and token_transfers: unique by position in the block, (block_number, log_index), instead of by
-- (tx_hash, log_index) (spike: append-ordered keys). A log's index is its position in the block, so the
-- new key is as unique as the old one, and every new row lands at the right edge of the index instead of
-- at a random page of a multi-GB btree: on the 85 GB rig state each of the two old keys cost ~1 buffer
-- read and ~1 full-page WAL image per ingested transaction.
--
-- The foreign keys to transactions(hash) go with them: ON DELETE CASCADE needs an index on the
-- referencing tx_hash, or every reverted transaction would scan the whole table. DeleteBlock removes a
-- block's logs and transfers by block_number itself; lookups by transaction resolve the hash to its block
-- through transactions_pkey first.
CREATE UNIQUE INDEX IF NOT EXISTS logs_block_number_log_index_key ON logs (block_number, log_index);
CREATE UNIQUE INDEX IF NOT EXISTS token_transfers_block_number_log_index_key ON token_transfers (block_number, log_index);
ALTER TABLE logs DROP CONSTRAINT IF EXISTS logs_tx_hash_fkey;
ALTER TABLE token_transfers DROP CONSTRAINT IF EXISTS token_transfers_tx_hash_fkey;
ALTER TABLE logs DROP CONSTRAINT IF EXISTS logs_tx_hash_log_index_key;
ALTER TABLE token_transfers DROP CONSTRAINT IF EXISTS token_transfers_tx_hash_log_index_key;

---- create above / drop below ----

ALTER TABLE logs ADD CONSTRAINT logs_tx_hash_log_index_key UNIQUE (tx_hash, log_index);
ALTER TABLE token_transfers ADD CONSTRAINT token_transfers_tx_hash_log_index_key UNIQUE (tx_hash, log_index);
ALTER TABLE logs ADD CONSTRAINT logs_tx_hash_fkey FOREIGN KEY (tx_hash) REFERENCES transactions(hash) ON DELETE CASCADE;
ALTER TABLE token_transfers ADD CONSTRAINT token_transfers_tx_hash_fkey FOREIGN KEY (tx_hash) REFERENCES transactions(hash) ON DELETE CASCADE;
DROP INDEX IF EXISTS logs_block_number_log_index_key;
DROP INDEX IF EXISTS token_transfers_block_number_log_index_key;
