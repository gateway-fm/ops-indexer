-- Hot-path schema changes from the EXPLAIN audit:
--   * lowercase contracts.address + address_stats.address so reads use the
--     PK index (was forcing seq scans via WHERE LOWER(address) = LOWER($1))
--   * index contracts.creation_tx so the tx-detail LEFT JOIN stops seq scanning
--   * denormalise blocks.timestamp onto transactions.block_timestamp so list
--     queries drop the JOIN blocks
--   * chain_counters: O(1) totals replacing 4× COUNT(*) in GetChainStats,
--     maintained transactionally by InsertBlockDataBatch

-- Normalise addresses to lowercase so reads hit the PK index. A blind
-- UPDATE ... LOWER(address) collides on the primary key whenever legacy
-- mixed-case rows (written by the pre-0.2.0 indexer) share a lowercase form
-- with an existing row, so we merge instead of rename.
--
-- address_stats is an aggregate table: fold each mixed-case row into its
-- lowercase canonical row, summing counts and widening the seen-range.
WITH merged AS (
    SELECT
        LOWER(address)            AS address,
        SUM(tx_count)             AS tx_count,
        SUM(internal_tx_count)    AS internal_tx_count,
        SUM(token_transfer_count) AS token_transfer_count,
        MIN(first_seen)           AS first_seen,
        MAX(last_seen)            AS last_seen,
        bool_or(is_contract)      AS is_contract
    FROM address_stats
    WHERE address != LOWER(address)
    GROUP BY LOWER(address)
)
INSERT INTO address_stats AS s
    (address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, updated_at)
SELECT address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, NOW()
FROM merged
ON CONFLICT (address) DO UPDATE SET
    tx_count             = s.tx_count + EXCLUDED.tx_count,
    internal_tx_count    = s.internal_tx_count + EXCLUDED.internal_tx_count,
    token_transfer_count = s.token_transfer_count + EXCLUDED.token_transfer_count,
    first_seen           = LEAST(s.first_seen, EXCLUDED.first_seen),
    last_seen            = GREATEST(s.last_seen, EXCLUDED.last_seen),
    is_contract          = s.is_contract OR EXCLUDED.is_contract,
    updated_at           = NOW();
DELETE FROM address_stats WHERE address != LOWER(address);

-- contracts is not aggregate: keep one canonical row per lowercase address
-- (prefer an already-lowercase row, then the earliest creation) and drop the
-- rest, then lowercase the survivor.
DELETE FROM contracts
WHERE ctid IN (
    SELECT ctid FROM (
        SELECT ctid, ROW_NUMBER() OVER (
            PARTITION BY LOWER(address)
            ORDER BY (address = LOWER(address)) DESC, block_number ASC, ctid
        ) AS rn
        FROM contracts
    ) ranked
    WHERE rn > 1
);
UPDATE contracts SET address = LOWER(address) WHERE address != LOWER(address);

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
