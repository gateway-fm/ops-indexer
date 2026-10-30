-- address_stats.sent_count: transactions of types 0-4 sent by the address. On chains with system or deposit
-- transaction types (OP Stack 126, Arbitrum 100-106) it can be below eth_getTransactionCount, since some of those raise
-- the sender's nonce too. GetAddress.tx_count_out serves it. Rows written before this start at 0 until the ledger
-- reconciliation recounts them.

-- The ADD COLUMN needs ACCESS EXCLUSIVE on address_stats, and while it waits for a reader every later reader queues
-- behind it. It gives up after a second instead; the startup migration retries it (MigrateSchema).
SET LOCAL lock_timeout = '1s';
ALTER TABLE address_stats ADD COLUMN sent_count BIGINT NOT NULL DEFAULT 0;

---- create above / drop below ----

ALTER TABLE address_stats DROP COLUMN sent_count;
