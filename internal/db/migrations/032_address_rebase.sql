-- address_stats derived from the ledger: when a span closes, its addresses' rows are
-- set to the closed ledger rows plus their rows above the closed spans. rebased_at marks a span whose addresses are
-- done, so a rebase interrupted by a restart resumes; ledger_reverted_address holds the addresses of reverted blocks
-- until the next rebase takes them.

ALTER TABLE ledger_spans ADD COLUMN rebased_at TIMESTAMPTZ;
UPDATE ledger_spans SET rebased_at = now();

CREATE TABLE ledger_reverted_address (address TEXT PRIMARY KEY);

-- Rebases since the days' address columns were last recounted that moved a first_seen; 0 when none is pending.
INSERT INTO ledger_verify_state (name, value) VALUES ('address_days_recount', 0) ON CONFLICT (name) DO NOTHING;

-- ledger_address_contributions from block p_lo up, for the given addresses only, through each table's address index.
CREATE FUNCTION ledger_address_contributions_of(p_lo BIGINT, p_addresses TEXT[])
RETURNS TABLE (address TEXT, tx TEXT, block BIGINT, sent INT, internal INT, transfers INT) LANGUAGE sql STABLE AS $f$
    SELECT from_address, hash, block_number, CASE WHEN tx_type BETWEEN 0 AND 4 THEN 1 ELSE 0 END, 0, 0
    FROM transactions WHERE from_address = ANY (p_addresses) AND block_number >= p_lo
    UNION ALL
    SELECT to_address, hash, block_number, 0, 0, 0
    FROM transactions WHERE to_address = ANY (p_addresses) AND block_number >= p_lo
    UNION ALL
    SELECT from_address, tx_hash, block_number, 0, 0, 1
    FROM token_transfers WHERE from_address = ANY (p_addresses) AND block_number >= p_lo
      AND from_address <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT to_address, tx_hash, block_number, 0, 0, 1
    FROM token_transfers WHERE to_address = ANY (p_addresses) AND block_number >= p_lo
      AND to_address <> '0x0000000000000000000000000000000000000000' AND to_address <> from_address
    UNION ALL
    SELECT from_address, NULL, block_number, 0, 1, 0
    FROM internal_transactions WHERE from_address = ANY (p_addresses) AND block_number >= p_lo
    UNION ALL
    SELECT to_address, NULL, block_number, 0, 1, 0
    FROM internal_transactions WHERE to_address = ANY (p_addresses) AND block_number >= p_lo AND to_address <> from_address
    UNION ALL
    SELECT address, NULL, block_number, 0, 0, 0
    FROM contracts WHERE address = ANY (p_addresses) AND block_number >= p_lo
$f$;

-- What address_stats should hold for the given addresses, as ledger_running_diff_address counts it: the closed ledger
-- rows, the open stats backfill span's staged rows, and the rows above the closed spans. An address with none is
-- absent.
CREATE FUNCTION ledger_address_truth(p_addresses TEXT[])
RETURNS TABLE (address TEXT, txs NUMERIC, sent NUMERIC, internal NUMERIC, transfers NUMERIC, first_seen BIGINT, last_seen BIGINT)
LANGUAGE sql STABLE AS $f$
    SELECT address, sum(txs), sum(sent), sum(internal), sum(transfers), min(first_seen), max(last_seen)
    FROM (
        SELECT l.address, l.txs, l.sent, l.internal, l.transfers, l.first_seen, l.last_seen
        FROM ledger_address l WHERE l.address = ANY (p_addresses)
        UNION ALL
        SELECT l.address, l.txs, l.sent, l.internal, l.transfers, l.first_seen, l.last_seen
        FROM ledger_stage_address l WHERE l.lo = stats_backfill_open_span() AND l.address = ANY (p_addresses)
        UNION ALL
        SELECT x.address, count(DISTINCT x.tx), sum(x.sent), sum(x.internal), sum(x.transfers), min(x.block), max(x.block)
        FROM ledger_address_contributions_of(ledger_closed_through(), p_addresses) x
        GROUP BY x.address
    ) u
    GROUP BY address
$f$;

---- create above / drop below ----

DROP FUNCTION ledger_address_truth(TEXT[]);
DROP FUNCTION ledger_address_contributions_of(BIGINT, TEXT[]);
DELETE FROM ledger_verify_state WHERE name = 'address_days_recount';
DROP TABLE ledger_reverted_address;
ALTER TABLE ledger_spans DROP COLUMN rebased_at;
