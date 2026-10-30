package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// BackfillContractsFromInternalTransactions writes a contracts row for every internal creation frame still in
// internal_transactions (the retained range), by the rule ingest applies to new blocks: a create / create2 frame
// below the top level, successful, under no failed enclosing frame, with deployed code in its output. Rows that
// exist stay as they are. It holds the history-DDL gate shared, as a ledger span does, so retention's DDL waits
// for the scan instead of queueing ingest behind it.
func (d *DB) BackfillContractsFromInternalTransactions(ctx context.Context) (int64, error) {
	var inserted int64
	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, historyDDLGate); err != nil {
			return err
		}
		ct, err := tx.Exec(ctx, `
			INSERT INTO contracts (address, bytecode, bytecode_hash, creator, creation_tx, block_number, is_verified)
			SELECT DISTINCT ON (c.to_address) LOWER(c.to_address), substr(c.output, 3),
			       '0x' || lpad(right(substr(c.output, 3), 64), 64, '0'), LOWER(c.from_address), c.tx_hash, c.block_number, false
			FROM internal_transactions c
			WHERE c.call_type IN ('create', 'create2') AND c.trace_address <> '' AND c.error IS NULL
			  AND c.to_address IS NOT NULL AND c.output LIKE '0x%' AND length(c.output) > 2
			  AND NOT EXISTS (SELECT 1 FROM internal_transactions p
			                  WHERE p.tx_hash = c.tx_hash AND p.error IS NOT NULL
			                    AND (p.trace_address = '' OR c.trace_address LIKE p.trace_address || ',%'))
			ORDER BY c.to_address, c.block_number
			ON CONFLICT (address) DO NOTHING`)
		if err != nil {
			return err
		}
		inserted = ct.RowsAffected()
		return nil
	})
	return inserted, err
}

// ExistingContracts reports which of addresses (lowercase) have a contracts row.
func (d *DB) ExistingContracts(ctx context.Context, addresses []string) (map[string]bool, error) {
	rows, err := d.pool.Query(ctx, `SELECT address FROM contracts WHERE address = ANY($1)`, addresses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		found[a] = true
	}
	return found, rows.Err()
}
