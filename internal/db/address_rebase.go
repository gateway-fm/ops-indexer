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
// their rows above the closed spans (ledger_address_truth), in batches of rebaseBatch (rebaseAddressBatch). create are
// the addresses a missing row may be created for; it is created only for one the truth has, and any other address is
// only corrected. A row whose address has no truth, no block left that saw it, is removed: an address no canonical
// block has seen has no row. A batch counts without holding any row and never retries for a conflict, so it neither
// stalls ingest nor stalls behind it.
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
		r, err := rebaseAddressBatch(ctx, d, batch, toCreate, inBatch)
		if err != nil {
			return total, err
		}
		total.add(r)
	}
	return total, nil
}

// rebaseRow is one address as a batch reads it: its row and its truth, in one snapshot, and the version of its
// ledger_reverted_address entry, if any.
type rebaseRow struct {
	address                                     string
	present, seen                               bool
	txs, sent, internal, transfers              int64
	firstSeen, lastSeen                         *int64
	wantTxs, wantSent, wantInternal, wantTransf int64
	wantFirst, wantLast                         *int64
	reverted                                    *string
}

func (r rebaseRow) differs() bool {
	return r.txs != r.wantTxs || r.sent != r.wantSent || r.internal != r.wantInternal || r.transfers != r.wantTransf ||
		!sameBlock(r.firstSeen, r.wantFirst) || !sameBlock(r.lastSeen, r.wantLast)
}

// rebaseWrite is what one of a batch's writes does to its row.
type rebaseWrite int

const (
	rebaseCorrect rebaseWrite = iota
	rebaseCreate
	rebaseRemove
)

func sameBlock(a, b *int64) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

// rebaseAddressBatch sets one batch's rows to their truth without holding them while it counts. It reads every row and
// its truth in one statement, so in one snapshot, and takes the correction truth - row from it. A delta ingest commits
// afterwards moves the row and the truth alike, so adding the correction later sets the row to the truth then: the
// correction is applied additively, in a short transaction that takes the rows in byte order (ingest's order).
// first_seen and last_seen become the truth's unless ingest moved them since the snapshot, in which case the lower or
// higher of the two. A row missing at the snapshot is inserted with the correction, added to any row ingest created
// since. A row whose address had no truth at the snapshot is taken and its truth counted again: it is removed if the
// address still has none, and set to it otherwise. Counting the truth under the rows' locks held every ingest write to
// them for the whole count.
func rebaseAddressBatch(ctx context.Context, d *DB, batch, create []string,
	inBatch func(ctx context.Context, tx pgx.Tx, batch []string) error) (RebaseResult, error) {
	var r RebaseResult
	creatable := make(map[string]bool, len(create))
	for _, a := range create {
		creatable[a] = true
	}
	rows, err := d.pool.Query(ctx, `
		SELECT a, s.address IS NOT NULL, t.address IS NOT NULL,
		       COALESCE(s.tx_count, 0), COALESCE(s.sent_count, 0), COALESCE(s.internal_tx_count, 0),
		       COALESCE(s.token_transfer_count, 0), s.first_seen, s.last_seen,
		       COALESCE(t.txs, 0)::BIGINT, COALESCE(t.sent, 0)::BIGINT, COALESCE(t.internal, 0)::BIGINT,
		       COALESCE(t.transfers, 0)::BIGINT, t.first_seen, t.last_seen,
		       (SELECT r.xmin::TEXT FROM ledger_reverted_address r WHERE r.address = a)
		FROM unnest($1::TEXT[]) a
		LEFT JOIN address_stats s ON s.address = a
		LEFT JOIN ledger_address_truth($1::TEXT[]) t ON t.address = a`, batch)
	if err != nil {
		return r, err
	}
	read, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (rebaseRow, error) {
		var v rebaseRow
		return v, row.Scan(&v.address, &v.present, &v.seen, &v.txs, &v.sent, &v.internal, &v.transfers, &v.firstSeen, &v.lastSeen,
			&v.wantTxs, &v.wantSent, &v.wantInternal, &v.wantTransf, &v.wantFirst, &v.wantLast, &v.reverted)
	})
	if err != nil {
		return r, err
	}
	sort.Slice(read, func(i, j int) bool { return read[i].address < read[j].address })
	writes := &pgx.Batch{}
	var kinds []rebaseWrite
	var revertedAddresses, revertedVersions []string
	for _, v := range read {
		if v.reverted != nil {
			revertedAddresses = append(revertedAddresses, v.address)
			revertedVersions = append(revertedVersions, *v.reverted)
		}
		switch {
		case v.present && !v.seen:
			// Taken first, in its own statement, so the next statement's truth follows any write that held the row:
			// the row goes if the address still has none, and is set to it otherwise. Only rows without a truth at the
			// snapshot pay for this count.
			writes.Queue(`SELECT 1 FROM address_stats WHERE address = $1 FOR UPDATE`, v.address)
			kinds = append(kinds, rebaseCorrect)
			writes.Queue(`
				WITH t AS (SELECT * FROM ledger_address_truth(ARRAY[$1::TEXT])),
				gone AS (
					DELETE FROM address_stats WHERE address = $1 AND NOT EXISTS (SELECT 1 FROM t)
					RETURNING first_seen
				), kept AS (
					UPDATE address_stats a SET
						tx_count = t.txs, sent_count = t.sent, internal_tx_count = t.internal, token_transfer_count = t.transfers,
						first_seen = t.first_seen, last_seen = t.last_seen, updated_at = NOW()
					FROM t WHERE a.address = $1
				)
				SELECT EXISTS (SELECT 1 FROM gone)`, v.address)
			kinds = append(kinds, rebaseRemove)
			if v.firstSeen != nil && (r.FirstSeenFrom == nil || *v.firstSeen < *r.FirstSeenFrom) {
				r.FirstSeenFrom = v.firstSeen
			}
			continue
		case v.present && v.differs():
			kinds = append(kinds, rebaseCorrect)
			writes.Queue(`
				UPDATE address_stats SET
					tx_count = COALESCE(tx_count, 0) + $2, sent_count = COALESCE(sent_count, 0) + $3,
					internal_tx_count = COALESCE(internal_tx_count, 0) + $4,
					token_transfer_count = COALESCE(token_transfer_count, 0) + $5,
					first_seen = CASE WHEN first_seen IS NOT DISTINCT FROM $6 THEN $7 ELSE LEAST(first_seen, $7) END,
					last_seen = CASE WHEN last_seen IS NOT DISTINCT FROM $8 THEN $9 ELSE GREATEST(last_seen, $9) END,
					updated_at = NOW()
				WHERE address = $1`, v.address, v.wantTxs-v.txs, v.wantSent-v.sent, v.wantInternal-v.internal,
				v.wantTransf-v.transfers, v.firstSeen, v.wantFirst, v.lastSeen, v.wantLast)
		case !v.present && v.seen && creatable[v.address] && v.differs():
			kinds = append(kinds, rebaseCreate)
			writes.Queue(`
				INSERT INTO address_stats AS s (address, tx_count, sent_count, internal_tx_count, token_transfer_count,
					first_seen, last_seen, is_contract, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, EXISTS (SELECT 1 FROM contracts c WHERE c.address = $1), NOW())
				ON CONFLICT (address) DO UPDATE SET
					tx_count = COALESCE(s.tx_count, 0) + EXCLUDED.tx_count, sent_count = COALESCE(s.sent_count, 0) + EXCLUDED.sent_count,
					internal_tx_count = COALESCE(s.internal_tx_count, 0) + EXCLUDED.internal_tx_count,
					token_transfer_count = COALESCE(s.token_transfer_count, 0) + EXCLUDED.token_transfer_count,
					first_seen = LEAST(s.first_seen, EXCLUDED.first_seen), last_seen = GREATEST(s.last_seen, EXCLUDED.last_seen),
					updated_at = NOW()
				RETURNING (xmax = 0)`, v.address, v.wantTxs, v.wantSent, v.wantInternal, v.wantTransf, v.wantFirst, v.wantLast)
		default:
			continue
		}
		r.Changed++
		if !sameBlock(v.firstSeen, v.wantFirst) {
			moved := v.wantFirst
			if moved == nil || (v.firstSeen != nil && *v.firstSeen < *moved) {
				moved = v.firstSeen
			}
			if moved != nil && (r.FirstSeenFrom == nil || *moved < *r.FirstSeenFrom) {
				r.FirstSeenFrom = moved
			}
		}
	}
	if writes.Len() == 0 && len(revertedAddresses) == 0 && inBatch == nil {
		return r, nil
	}
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		r.Created, r.Removed = 0, 0
		if writes.Len() > 0 {
			results := tx.SendBatch(ctx, writes)
			for _, kind := range kinds {
				if kind == rebaseCorrect {
					if _, err := results.Exec(); err != nil {
						_ = results.Close()
						return err
					}
					continue
				}
				var done bool
				if err := results.QueryRow().Scan(&done); err != nil {
					_ = results.Close()
					return err
				}
				switch {
				case kind == rebaseCreate && done:
					r.Created++
				case kind == rebaseRemove && done:
					r.Removed++
				case kind == rebaseRemove:
					r.Changed++
				}
			}
			if err := results.Close(); err != nil {
				return err
			}
		}
		if r.Created != r.Removed {
			if _, err := tx.Exec(ctx, `UPDATE chain_counters SET count = count + $1, updated_at = NOW() WHERE name = 'addresses_total'`,
				r.Created-r.Removed); err != nil {
				return err
			}
		}
		if r.FirstSeenFrom != nil {
			if _, err := tx.Exec(ctx, `UPDATE ledger_verify_state SET value = value + 1 WHERE name = 'address_days_recount'`); err != nil {
				return err
			}
		}
		// Only the entries the snapshot saw: a revert after it records its address again, as a new row version.
		if len(revertedAddresses) > 0 {
			if _, err := tx.Exec(ctx, `
				DELETE FROM ledger_reverted_address r USING unnest($1::TEXT[], $2::TEXT[]) AS seen(address, version)
				WHERE r.address = seen.address AND r.xmin::TEXT = seen.version`, revertedAddresses, revertedVersions); err != nil {
				return err
			}
		}
		if inBatch != nil {
			return inBatch(ctx, tx, batch)
		}
		return nil
	})
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
