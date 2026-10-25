package db

import (
	"context"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/internal/log"
)

// rebaseBatch is how many addresses one rebase transaction locks and recounts.
const rebaseBatch = 1000

// RebaseResult is what a rebase changed: rows set to a different value, rows it created, rows it removed, and the
// earliest first_seen block, old or new, of a row whose first_seen moved or that was removed (nil when none did).
type RebaseResult struct {
	Changed, Created, Removed int
	FirstSeenFrom             *int64
}

func (r *RebaseResult) add(o RebaseResult) {
	r.Changed += o.Changed
	r.Created += o.Created
	r.Removed += o.Removed
	if o.FirstSeenFrom != nil && (r.FirstSeenFrom == nil || *o.FirstSeenFrom < *r.FirstSeenFrom) {
		r.FirstSeenFrom = o.FirstSeenFrom
	}
}

// RebaseAddresses sets the address_stats rows of the given addresses to their truth, the closed ledger rows plus
// their rows above the closed spans (ledger_address_truth), in batches of rebaseBatch, each in its own READ COMMITTED
// transaction. create are the addresses a missing row may be created for; it is created only for one some canonical
// block has seen (ledger_address_seen), and any other address is only corrected. A row whose address has no truth, no
// block left that saw it, is removed: an address no canonical block has seen has no row.
//
// A batch first inserts the missing rows it may create, then locks every row of the batch in byte order (ingest's
// order), and only then counts the truth, in a later statement: every delta committed before the locks is in both the
// row and the truth, and none can commit until the batch does. Inserting before locking keeps a row ingest creates
// during the batch from being counted twice. A batch does not retry on a conflict, so it finishes under any ingest load.
func (d *DB) RebaseAddresses(ctx context.Context, addresses, create []string) (RebaseResult, error) {
	return d.rebaseAddresses(ctx, addresses, create, nil)
}

// rebaseAddresses is RebaseAddresses with inBatch, if not nil, run in each batch's transaction after its rows are set.
func (d *DB) rebaseAddresses(ctx context.Context, addresses, create []string,
	inBatch func(ctx context.Context, tx pgx.Tx, batch []string) error) (RebaseResult, error) {
	sort.Strings(addresses)
	creatable := make(map[string]bool, len(create))
	for _, a := range create {
		creatable[a] = true
	}
	var total RebaseResult
	for start := 0; start < len(addresses); start += rebaseBatch {
		batch := addresses[start:min(start+rebaseBatch, len(addresses))]
		var toCreate []string
		for _, a := range batch {
			if creatable[a] {
				toCreate = append(toCreate, a)
			}
		}
		var r RebaseResult
		if err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
			var err error
			if r, err = rebaseAddressBatch(ctx, tx, batch, toCreate); err != nil || inBatch == nil {
				return err
			}
			return inBatch(ctx, tx, batch)
		}); err != nil {
			return total, err
		}
		total.add(r)
	}
	return total, nil
}

func rebaseAddressBatch(ctx context.Context, tx pgx.Tx, batch, create []string) (RebaseResult, error) {
	var r RebaseResult
	if len(create) > 0 {
		tag, err := tx.Exec(ctx, `
			INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, sent_count, is_contract, updated_at)
			SELECT a, 0, 0, 0, 0, EXISTS (SELECT 1 FROM contracts c WHERE c.address = a), NOW()
			FROM ledger_address_seen($1::TEXT[]) a ORDER BY a COLLATE "C"
			ON CONFLICT (address) DO NOTHING`, create)
		if err != nil {
			return r, err
		}
		r.Created = int(tag.RowsAffected())
	}
	if _, err := tx.Exec(ctx, `
		SELECT 1 FROM address_stats WHERE address = ANY ($1::TEXT[]) ORDER BY address COLLATE "C" FOR UPDATE`, batch); err != nil {
		return r, err
	}
	if err := tx.QueryRow(ctx, `
		WITH truth AS (SELECT * FROM ledger_address_truth($1::TEXT[])),
		gone AS (
			DELETE FROM address_stats a WHERE a.address = ANY ($1::TEXT[])
			  AND NOT EXISTS (SELECT 1 FROM truth t WHERE t.address = a.address)
			RETURNING a.first_seen
		),
		set AS (
			UPDATE address_stats a SET
				tx_count = COALESCE(t.txs, 0), sent_count = COALESCE(t.sent, 0),
				internal_tx_count = COALESCE(t.internal, 0), token_transfer_count = COALESCE(t.transfers, 0),
				first_seen = t.first_seen, last_seen = t.last_seen, updated_at = NOW()
			FROM address_stats old JOIN truth t ON t.address = old.address
			WHERE a.address = old.address AND old.address = ANY ($1::TEXT[])
			  AND (old.tx_count, old.sent_count, old.internal_tx_count, old.token_transfer_count, old.first_seen, old.last_seen)
			      IS DISTINCT FROM (COALESCE(t.txs, 0), COALESCE(t.sent, 0), COALESCE(t.internal, 0), COALESCE(t.transfers, 0),
			                        t.first_seen, t.last_seen)
			RETURNING CASE WHEN old.first_seen IS DISTINCT FROM t.first_seen THEN LEAST(old.first_seen, t.first_seen) END AS moved
		)
		SELECT (SELECT count(*) FROM set), (SELECT count(*) FROM gone),
		       LEAST((SELECT min(moved) FROM set), (SELECT min(first_seen) FROM gone))`, batch).
		Scan(&r.Changed, &r.Removed, &r.FirstSeenFrom); err != nil {
		return r, err
	}
	if r.Created != r.Removed {
		if _, err := tx.Exec(ctx, `UPDATE chain_counters SET count = count + $1, updated_at = NOW() WHERE name = 'addresses_total'`,
			r.Created-r.Removed); err != nil {
			return r, err
		}
	}
	if r.FirstSeenFrom != nil {
		if _, err := tx.Exec(ctx, `UPDATE ledger_verify_state SET value = value + 1 WHERE name = 'address_days_recount'`); err != nil {
			return r, err
		}
	}
	// Last, holding the rows: a revert of one of these addresses either committed before the lock, and is in the
	// truth, or waits for it and records the address again.
	_, err := tx.Exec(ctx, `DELETE FROM ledger_reverted_address WHERE address = ANY ($1::TEXT[])`, batch)
	return r, err
}

// hotTxs is the transactions in one span above which an address is rebased in a batch of its own: its row stays
// locked while its own rows above the closed spans are counted, and no other address waits with it.
const hotTxs = 10_000

// rebasePendingSpans rebases the addresses of every closed span not rebased yet, oldest first, together with the
// addresses of blocks reverted since the last rebase, and then recounts the days' address columns if a first_seen
// moved. A span is marked rebased once its addresses are; a restart resumes at the first one that is not.
func hotBatches(hot []string) [][]string {
	batches := make([][]string, 0, len(hot))
	for _, a := range hot {
		batches = append(batches, []string{a})
	}
	return batches
}

func (d *DB) rebasePendingSpans(ctx context.Context) error {
	if err := d.recountPendingDays(ctx); err != nil {
		return err
	}
	rows, err := d.pool.Query(ctx, `SELECT lo FROM ledger_spans WHERE rebased_at IS NULL AND source <> 'merged' ORDER BY lo`)
	if err != nil {
		return err
	}
	los, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return err
	}
	type member struct {
		address     string
		inSpan, hot bool
	}
	var total RebaseResult
	for _, lo := range los {
		rows, err := d.pool.Query(ctx, `
			SELECT address, true, txs > $2 FROM ledger_address
			WHERE lo = $1 AND address <> '0x0000000000000000000000000000000000000000'
			UNION ALL
			SELECT address, false, false FROM ledger_reverted_address
			WHERE address NOT IN (SELECT address FROM ledger_address WHERE lo = $1)`, lo, hotTxs)
		if err != nil {
			return err
		}
		members, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (member, error) {
			var m member
			return m, r.Scan(&m.address, &m.inSpan, &m.hot)
		})
		if err != nil {
			return err
		}
		var span, hot, rest []string
		for _, m := range members {
			if m.inSpan {
				span = append(span, m.address)
			}
			if m.hot {
				hot = append(hot, m.address)
			} else {
				rest = append(rest, m.address)
			}
		}
		var spanResult RebaseResult
		for _, batch := range append(hotBatches(hot), rest) {
			r, err := d.RebaseAddresses(ctx, batch, span)
			if err != nil {
				return err
			}
			spanResult.add(r)
		}
		total.add(spanResult)
		if _, err := d.pool.Exec(ctx, `UPDATE ledger_spans SET rebased_at = now() WHERE lo = $1`, lo); err != nil {
			return err
		}
		log.Info("ledger: span's addresses rebased", "lo", lo, "addresses", len(members), "hot", len(hot),
			"changed", spanResult.Changed, "created", spanResult.Created, "removed", spanResult.Removed)
	}
	return d.recountPendingDays(ctx)
}

// recountPendingDays recounts the days' address columns from address_stats if a rebase has moved a first_seen since
// the last recount, under daily_stats_advance's lock and holding no address_stats row. A batch that moves one counts
// it in ledger_verify_state in its own transaction, and the recount clears only the count it read, so a recount that
// fails, or a rebase that commits while it runs, leaves it pending for the next.
func (d *DB) recountPendingDays(ctx context.Context) error {
	var pending int64
	if err := d.pool.QueryRow(ctx, `SELECT value FROM ledger_verify_state WHERE name = 'address_days_recount'`).Scan(&pending); err != nil {
		return err
	}
	if pending == 0 {
		return nil
	}
	return pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`); err != nil {
			return err
		}
		if err := recountDailyAddressColumns(ctx, tx); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE ledger_verify_state SET value = 0 WHERE name = 'address_days_recount' AND value = $1`, pending)
		return err
	})
}
