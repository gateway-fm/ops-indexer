package db

// ledgerAddressRowsSQL is ledger_close_span's per-address statement (migration 022) over the rows of [$1, $2), with
// token_transfers read in place of the span's temp table: the counts ingest would have written for those blocks.
// TestLedgerAddressRowsMatchTheCloser pins it to what the closer writes.
const ledgerAddressRowsSQL = `
	SELECT address, sum(txs) AS txs, sum(internal) AS internal, sum(transfers) AS transfers,
	       min(block) AS first_seen, max(block) AS last_seen
	FROM (
		SELECT x.address, 1 AS txs, 0 AS internal, 0 AS transfers, t.block_number AS block
		FROM transactions t
		LEFT JOIN (
			SELECT tx_hash, array_agg(DISTINCT p.address) AS addresses
			FROM token_transfers CROSS JOIN LATERAL (VALUES (from_address), (to_address)) AS p(address)
			WHERE block_number >= $1 AND block_number < $2 AND p.address <> '0x0000000000000000000000000000000000000000'
			GROUP BY tx_hash
		) part ON part.tx_hash = t.hash
		CROSS JOIN LATERAL (
			SELECT t.from_address AS address
			UNION ALL
			SELECT t.to_address WHERE t.to_address IS NOT NULL AND t.to_address <> t.from_address
			UNION ALL
			SELECT a FROM unnest(part.addresses) AS a WHERE a <> t.from_address AND a IS DISTINCT FROM t.to_address
		) x
		WHERE t.block_number >= $1 AND t.block_number < $2
		UNION ALL
		SELECT x.address, 0, 0, 1, s.block_number
		FROM token_transfers s CROSS JOIN LATERAL (VALUES (s.from_address, true), (s.to_address, false)) AS x(address, sender)
		WHERE s.block_number >= $1 AND s.block_number < $2
		  AND x.address <> '0x0000000000000000000000000000000000000000' AND (x.sender OR s.to_address <> s.from_address)
		UNION ALL
		SELECT x.address, 0, 1, 0, i.block_number
		FROM internal_transactions i CROSS JOIN LATERAL (VALUES (i.from_address, true), (i.to_address, false)) AS x(address, sender)
		WHERE i.block_number >= $1 AND i.block_number < $2
		  AND x.address IS NOT NULL AND (x.sender OR i.to_address <> i.from_address)
		UNION ALL
		SELECT address, 0, 0, 0, block_number FROM contracts WHERE block_number >= $1 AND block_number < $2
	) u
	GROUP BY address`
