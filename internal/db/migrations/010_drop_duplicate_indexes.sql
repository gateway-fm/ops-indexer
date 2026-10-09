-- Two indexes duplicate the leading column of a unique constraint on the same table, so every
-- query that could use them can use the constraint's index instead, and every insert paid for
-- both. On a 6.8M-transaction database idx_logs_tx_hash is 802 MB of randomly keyed btree that
-- one log row in every transaction has to descend into; it had 0 scans over the whole fill.
--
--   idx_logs_tx_hash (tx_hash)  ->  logs_tx_hash_log_index_key (tx_hash, log_index)
--   idx_blocks_hash  (hash)     ->  blocks_hash_key (hash)
--
-- Dropping an index is a catalog change; it does not rewrite the table.
DROP INDEX IF EXISTS idx_logs_tx_hash;
DROP INDEX IF EXISTS idx_blocks_hash;

---- create above / drop below ----

CREATE INDEX IF NOT EXISTS idx_logs_tx_hash ON logs(tx_hash);
CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash);
