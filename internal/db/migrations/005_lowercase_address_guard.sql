-- Structural guarantee that contracts.address and address_stats.address are
-- always lowercase. Migration 004 normalises existing rows and the 0.2.2+
-- indexer writes lowercase on every insert, but nothing at the DB level
-- enforced it — a future code regression could silently reintroduce the
-- mixed-case duplicates that broke 004. The CHECK constraints below close that.
--
-- The defensive merge/dedup repeats 004's normalisation so this migration is
-- safe to apply on any DB regardless of how it reached this point: it is a
-- pure no-op on a DB that already crossed 004 (every row already lowercase),
-- and it cleans up any straggler before the CHECK is validated.

-- address_stats: fold mixed-case rows into their lowercase canonical row.
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

-- contracts: keep one canonical row per lowercase address, drop the rest.
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

ALTER TABLE address_stats ADD CONSTRAINT address_stats_address_lowercase_chk
    CHECK (address = LOWER(address));
ALTER TABLE contracts ADD CONSTRAINT contracts_address_lowercase_chk
    CHECK (address = LOWER(address));

---- create above / drop below ----

ALTER TABLE contracts     DROP CONSTRAINT IF EXISTS contracts_address_lowercase_chk;
ALTER TABLE address_stats DROP CONSTRAINT IF EXISTS address_stats_address_lowercase_chk;
