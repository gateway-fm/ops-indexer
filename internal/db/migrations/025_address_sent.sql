-- address_stats.sent_count: transactions the address sent of nonce-bumping types (0-4), what eth_getTransactionCount
-- returns for an EOA indexed from genesis. GetAddress.tx_count_out serves it. Rows written before this start at 0
-- until the ledger reconciliation recounts them.

ALTER TABLE address_stats ADD COLUMN sent_count BIGINT NOT NULL DEFAULT 0;

---- create above / drop below ----

ALTER TABLE address_stats DROP COLUMN sent_count;
