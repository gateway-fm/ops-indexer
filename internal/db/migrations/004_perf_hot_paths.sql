-- Hot-path schema changes from the EXPLAIN audit:
--   * lowercase contracts.address + address_stats.address so reads use the
--     PK index (was forcing seq scans via WHERE LOWER(address) = LOWER($1))
--   * index contracts.creation_tx so the tx-detail LEFT JOIN stops seq scanning
--   * denormalise blocks.timestamp onto transactions.block_timestamp so list
--     queries drop the JOIN blocks
--   * chain_counters: O(1) totals replacing 4× COUNT(*) in GetChainStats,
--     maintained transactionally by InsertBlockDataBatch

UPDATE contracts     SET address = LOWER(address) WHERE address != LOWER(address);
UPDATE address_stats SET address = LOWER(address) WHERE address != LOWER(address);

CREATE INDEX IF NOT EXISTS idx_contracts_creation_tx ON contracts(creation_tx);

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS block_timestamp BIGINT;

-- Idempotent / resumable; on very large chains operators may prefer running
-- this in batches off-line before deploying.
UPDATE transactions t
SET block_timestamp = b.timestamp
FROM blocks b
WHERE t.block_number = b.number
  AND t.block_timestamp IS NULL;

CREATE TABLE IF NOT EXISTS chain_counters (
    name TEXT PRIMARY KEY,
    count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO chain_counters (name, count) VALUES
    ('blocks_total',       (SELECT COUNT(*) FROM blocks)),
    ('transactions_total', (SELECT COUNT(*) FROM transactions)),
    ('addresses_total',    (SELECT COUNT(*) FROM address_stats)),
    ('tokens_total',       (SELECT COUNT(*) FROM tokens))
ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW();

---- create above / drop below ----

DROP TABLE IF EXISTS chain_counters;
DROP INDEX IF EXISTS idx_contracts_creation_tx;
ALTER TABLE transactions DROP COLUMN IF EXISTS block_timestamp;
-- Address-case normalisation is one-way; no down for the UPDATEs.
SELECT 1;
