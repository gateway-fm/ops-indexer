package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// revertAddressStats takes a reverted block's rows off address_stats, before they are deleted: each address loses
// what the ledger counts for it in the block (ledger_address_contributions, sent transactions included), one address at a time in address order like the writer.
// first_seen and last_seen that point into the block move to the address's remaining rows (the closed ledger when
// only pruned history remains), is_contract follows the remaining contract creations, and an address no remaining
// block has seen loses its row. Every address is recorded for the next span rebase, which corrects whatever the
// subtraction could not; an address already recorded is recorded again, as a new row version, so a rebase that read the
// earlier one does not clear it. It returns the number of rows removed.
// It first adds one to the address_stats_reverts counter, before any address_stats row: after a rollback to the
// earlier release, its staged repair compares that counter (addressStatsRevertsCounter).
func revertAddressStats(ctx context.Context, tx pgx.Tx, number uint64) (int64, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO chain_counters (name, count, updated_at) VALUES ($1, 1, NOW())
		ON CONFLICT (name) DO UPDATE SET count = chain_counters.count + 1, updated_at = NOW()`, addressStatsRevertsCounter); err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `
		SELECT c.address, c.txs, c.sent, c.internal, c.transfers,
		       EXISTS (SELECT 1 FROM contracts k WHERE k.address = c.address AND k.block_number = $1)
		FROM (
			SELECT address, count(DISTINCT tx) AS txs, sum(sent) AS sent, sum(internal) AS internal, sum(transfers) AS transfers
			FROM ledger_address_contributions($1::BIGINT, $2::BIGINT) GROUP BY address
		) c
		ORDER BY c.address COLLATE "C"`, int64(number), int64(number)+1)
	if err != nil {
		return 0, err
	}
	type contribution struct {
		address                        string
		txs, sent, internal, transfers int64
		created                        bool
	}
	all, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (contribution, error) {
		var c contribution
		return c, r.Scan(&c.address, &c.txs, &c.sent, &c.internal, &c.transfers, &c.created)
	})
	if err != nil {
		return 0, err
	}

	var forgotten int64
	for _, c := range all {
		// The row first, in address order like every writer: the decision below then sees what a writer that held it
		// (a stats backfill batch adding the address's earlier blocks) committed, and nothing changes it until this
		// revert commits.
		if _, err := tx.Exec(ctx, `SELECT 1 FROM address_stats WHERE address = $1 FOR UPDATE`, c.address); err != nil {
			return 0, err
		}
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
			// The newest closed span holding the address, read span by span through the ledger's primary key (its
			// address index is built after startup, and a revert must not wait for it or scan the ledger), and the
			// stats backfill's open span, which has not closed yet.
			if err := tx.QueryRow(ctx, `
				SELECT max(last_seen) FROM (
					(SELECT a.last_seen FROM ledger_spans s
					 CROSS JOIN LATERAL (SELECT last_seen FROM ledger_address WHERE lo = s.lo AND address = $1) a
					 ORDER BY s.lo DESC LIMIT 1)
					UNION ALL
					SELECT last_seen FROM ledger_stage_address WHERE lo = stats_backfill_open_span() AND address = $1
				) l`, c.address).Scan(&last); err != nil {
				return 0, err
			}
		}

		if _, err := tx.Exec(ctx, `
			UPDATE address_stats SET
				tx_count = GREATEST(tx_count - $3, 0),
				sent_count = GREATEST(sent_count - $9, 0),
				internal_tx_count = GREATEST(internal_tx_count - $4, 0),
				token_transfer_count = GREATEST(token_transfer_count - $5, 0),
				first_seen = CASE WHEN first_seen >= $2 AND $6::BIGINT IS NOT NULL THEN $6 ELSE first_seen END,
				last_seen = CASE WHEN last_seen >= $2 AND $7::BIGINT IS NOT NULL THEN $7 ELSE last_seen END,
				is_contract = CASE WHEN $8 THEN EXISTS (SELECT 1 FROM contracts WHERE address = $1 AND block_number <> $2) ELSE is_contract END,
				updated_at = NOW()
			WHERE address = $1`,
			c.address, int64(number), c.txs, c.internal, c.transfers, first, last, c.created, c.sent); err != nil {
			return 0, err
		}
	}

	// Last, after the rows: a rebase holding one of them takes the address off this table only once it commits.
	addresses := make([]string, len(all))
	for i, c := range all {
		addresses[i] = c.address
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_reverted_address (address) SELECT a FROM unnest($1::TEXT[]) a ORDER BY a COLLATE "C"
		ON CONFLICT (address) DO UPDATE SET address = EXCLUDED.address`, addresses)
	return forgotten, err
}
