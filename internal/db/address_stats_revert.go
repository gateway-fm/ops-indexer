package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// revertAddressStats takes a reverted block's rows off address_stats, before they are deleted: each address loses
// what ledgerAddressRowsSQL counts for it in the block, one address at a time in address order like the writer.
// first_seen and last_seen that point into the block move to the address's remaining rows (the closed ledger when
// only pruned history remains), is_contract follows the remaining contract creations, and an address no remaining
// block has seen loses its row. It returns the number of rows removed.
func revertAddressStats(ctx context.Context, tx pgx.Tx, number uint64) (int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT c.address, c.txs, c.internal, c.transfers,
		       EXISTS (SELECT 1 FROM contracts k WHERE k.address = c.address AND k.block_number = $1)
		FROM (`+ledgerAddressRowsSQL+`) c
		ORDER BY c.address`, int64(number), int64(number)+1)
	if err != nil {
		return 0, err
	}
	type contribution struct {
		address                  string
		txs, internal, transfers int64
		created                  bool
	}
	all, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (contribution, error) {
		var c contribution
		return c, r.Scan(&c.address, &c.txs, &c.internal, &c.transfers, &c.created)
	})
	if err != nil {
		return 0, err
	}

	var forgotten int64
	for _, c := range all {
		var first, last *int64
		var stillSeen bool
		if err := tx.QueryRow(ctx, `
			SELECT min(b), max(b), EXISTS (SELECT 1 FROM address_stats WHERE address = $1 AND first_seen < $2)
			FROM (
				(SELECT block_number AS b FROM transactions WHERE from_address = $1 AND block_number <> $2 ORDER BY block_number LIMIT 1)
				UNION ALL (SELECT block_number FROM transactions WHERE from_address = $1 AND block_number <> $2 ORDER BY block_number DESC LIMIT 1)
				UNION ALL (SELECT block_number FROM transactions WHERE to_address = $1 AND block_number <> $2 ORDER BY block_number LIMIT 1)
				UNION ALL (SELECT block_number FROM transactions WHERE to_address = $1 AND block_number <> $2 ORDER BY block_number DESC LIMIT 1)
				UNION ALL (SELECT block_number FROM token_transfers WHERE from_address = $1 AND $1 <> '`+zeroAddress+`' AND block_number <> $2 ORDER BY block_number LIMIT 1)
				UNION ALL (SELECT block_number FROM token_transfers WHERE from_address = $1 AND $1 <> '`+zeroAddress+`' AND block_number <> $2 ORDER BY block_number DESC LIMIT 1)
				UNION ALL (SELECT block_number FROM token_transfers WHERE to_address = $1 AND $1 <> '`+zeroAddress+`' AND block_number <> $2 ORDER BY block_number LIMIT 1)
				UNION ALL (SELECT block_number FROM token_transfers WHERE to_address = $1 AND $1 <> '`+zeroAddress+`' AND block_number <> $2 ORDER BY block_number DESC LIMIT 1)
				UNION ALL (SELECT block_number FROM internal_transactions WHERE from_address = $1 AND block_number <> $2 ORDER BY block_number LIMIT 1)
				UNION ALL (SELECT block_number FROM internal_transactions WHERE from_address = $1 AND block_number <> $2 ORDER BY block_number DESC LIMIT 1)
				UNION ALL (SELECT block_number FROM internal_transactions WHERE to_address = $1 AND block_number <> $2 ORDER BY block_number LIMIT 1)
				UNION ALL (SELECT block_number FROM internal_transactions WHERE to_address = $1 AND block_number <> $2 ORDER BY block_number DESC LIMIT 1)
				UNION ALL SELECT block_number FROM contracts WHERE address = $1 AND block_number <> $2
			) s`, c.address, int64(number)).Scan(&first, &last, &stillSeen); err != nil {
			return 0, err
		}

		if last == nil && !stillSeen {
			tag, err := tx.Exec(ctx, `DELETE FROM address_stats WHERE address = $1`, c.address)
			if err != nil {
				return 0, err
			}
			forgotten += tag.RowsAffected()
			continue
		}
		if last == nil {
			if err := tx.QueryRow(ctx, `
				SELECT a.last_seen FROM ledger_spans s
				CROSS JOIN LATERAL (SELECT last_seen FROM ledger_address WHERE lo = s.lo AND address = $1) a
				ORDER BY s.lo DESC LIMIT 1`, c.address).Scan(&last); err != nil && err != pgx.ErrNoRows {
				return 0, err
			}
		}

		if _, err := tx.Exec(ctx, `
			UPDATE address_stats SET
				tx_count = GREATEST(tx_count - $3, 0),
				internal_tx_count = GREATEST(internal_tx_count - $4, 0),
				token_transfer_count = GREATEST(token_transfer_count - $5, 0),
				first_seen = CASE WHEN first_seen >= $2 AND $6::BIGINT IS NOT NULL THEN $6 ELSE first_seen END,
				last_seen = CASE WHEN last_seen >= $2 AND $7::BIGINT IS NOT NULL THEN $7 ELSE last_seen END,
				is_contract = CASE WHEN $8 THEN EXISTS (SELECT 1 FROM contracts WHERE address = $1 AND block_number <> $2) ELSE is_contract END,
				updated_at = NOW()
			WHERE address = $1`,
			c.address, int64(number), c.txs, c.internal, c.transfers, first, last, c.created); err != nil {
			return 0, err
		}
	}
	return forgotten, nil
}
