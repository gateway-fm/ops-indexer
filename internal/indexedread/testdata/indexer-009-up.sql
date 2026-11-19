-- Exact producer009 UP from ops-indexer219515f184c61e85e57add79b01070063c7504d0.
-- Original full migration SHA256 42df518619e2ba99e5f06800844eaff71b961a8798608245eef915aa6412e2e1.
-- Owned integration fixture only; the reader never runs this migration.
-- Widen the counters that ingest increments inside the block transaction from INT to BIGINT.
-- At INT, the 2,147,483,648th transfer of one token (or transaction of one address) raises
-- "integer out of range" and every later block touching that row rolls back: ingest stops.
-- The gRPC fields are already uint64, and Go reads them into int (64-bit), so readers see no change.
-- tokens has one row per token (instant). address_stats is rewritten under an ACCESS EXCLUSIVE lock:
-- its runtime on a large database is in the PR text.
ALTER TABLE tokens
    ALTER COLUMN holder_count TYPE BIGINT,
    ALTER COLUMN transfer_count TYPE BIGINT;

ALTER TABLE address_stats
    ALTER COLUMN tx_count TYPE BIGINT,
    ALTER COLUMN internal_tx_count TYPE BIGINT,
    ALTER COLUMN token_transfer_count TYPE BIGINT;

