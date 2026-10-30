package db

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/internal/log"
)

// rebaseBatch is how many addresses one rebase transaction recounts.
const rebaseBatch = 1000

// rebaseLock serializes rebases among themselves (advisory lock key).
const rebaseLock = 4674

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
	creatable, present, seen                    bool
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
// correction is applied additively, taking the rows in byte order (ingest's order) only for the writes.
// first_seen and last_seen become the truth's unless ingest moved them since the snapshot, in which case the lower or
// higher of the two. A row missing at the snapshot is inserted with the correction, added to any row ingest created
// since. A row whose address had no truth at the snapshot is taken and its truth counted again: it is removed if the
// address still has none, and set to it otherwise. Counting the truth under the rows' locks held every ingest write to
// them for the whole count.
func rebaseAddressBatch(ctx context.Context, d *DB, batch, create []string,
	inBatch func(ctx context.Context, tx pgx.Tx, batch []string) error) (RebaseResult, error) {
	var r RebaseResult
	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		// Rebases one at a time: two that both read before either applied would each add the same correction.
		// Ingest never takes this lock. The read is a later statement, so its snapshot follows the lock.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, 1)`, rebaseLock); err != nil {
			return err
		}
		var err error
		r, err = rebaseAddressBatchLocked(ctx, tx, batch, create, inBatch)
		return err
	})
	return r, err
}

// spanRebaseStagedHook, when a test sets it, runs between a span rebase's stage (its read) and its first batch.
var spanRebaseStagedHook func(lo int64)

// rebaseReadHook, when a test sets it, runs between a batch's read and its first write.
var rebaseReadHook func(batch []string)

func rebaseAddressBatchLocked(ctx context.Context, tx pgx.Tx, batch, create []string,
	inBatch func(ctx context.Context, tx pgx.Tx, batch []string) error) (RebaseResult, error) {
	creatable := make(map[string]bool, len(create))
	for _, a := range create {
		creatable[a] = true
	}
	rows, err := tx.Query(ctx, `
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
		return RebaseResult{}, err
	}
	read, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (rebaseRow, error) {
		var v rebaseRow
		err := row.Scan(&v.address, &v.present, &v.seen, &v.txs, &v.sent, &v.internal, &v.transfers, &v.firstSeen, &v.lastSeen,
			&v.wantTxs, &v.wantSent, &v.wantInternal, &v.wantTransf, &v.wantFirst, &v.wantLast, &v.reverted)
		v.creatable = creatable[v.address]
		return v, err
	})
	if err != nil {
		return RebaseResult{}, err
	}
	if rebaseReadHook != nil {
		rebaseReadHook(batch)
	}
	return applyRebaseRows(ctx, tx, read, func(ctx context.Context, tx pgx.Tx) error {
		if inBatch == nil {
			return nil
		}
		return inBatch(ctx, tx, batch)
	})
}

// revertedSinceRead locks the batch's rows in byte order and returns the addresses a revert has recorded since the
// read (a ledger_reverted_address version the read did not see). A revert subtracts its block from the row and from
// the truth, but its subtraction stops at zero and it moves first_seen and last_seen, so a correction read before it
// can overshoot; those rows are counted again under their locks, and a row missing at the read is not created from
// them. A revert still in progress holds the rows, so the lock waits for it and the versions read after it are its.
func revertedSinceRead(ctx context.Context, tx pgx.Tx, read []rebaseRow) (map[string]bool, error) {
	addresses := make([]string, len(read))
	seen := make(map[string]*string, len(read))
	for i, v := range read {
		addresses[i] = v.address
		seen[v.address] = v.reverted
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM address_stats WHERE address = ANY($1) ORDER BY address FOR UPDATE`, addresses); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT a, (SELECT r.xmin::TEXT FROM ledger_reverted_address r WHERE r.address = a)
		FROM unnest($1::TEXT[]) a`, addresses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stale := map[string]bool{}
	for rows.Next() {
		var address string
		var version *string
		if err := rows.Scan(&address, &version); err != nil {
			return nil, err
		}
		if version != nil && (seen[address] == nil || *seen[address] != *version) {
			stale[address] = true
		}
	}
	return stale, rows.Err()
}

// applyRebaseRows applies the corrections of rows read in one snapshot, in one transaction, taking the rows in byte
// order (ingest's order); after, if not nil, runs in the same transaction.
func applyRebaseRows(ctx context.Context, tx pgx.Tx, read []rebaseRow, after func(ctx context.Context, tx pgx.Tx) error) (RebaseResult, error) {
	var r RebaseResult
	sort.Slice(read, func(i, j int) bool { return read[i].address < read[j].address })
	stale, err := revertedSinceRead(ctx, tx, read)
	if err != nil {
		return r, err
	}
	writes := &pgx.Batch{}
	var kinds []rebaseWrite
	var revertedAddresses, revertedVersions []string
	for _, v := range read {
		if v.reverted != nil {
			revertedAddresses = append(revertedAddresses, v.address)
			revertedVersions = append(revertedVersions, *v.reverted)
		}
		switch {
		case v.present && (!v.seen || stale[v.address]):
			// Taken first, in its own statement, so the next statement's truth follows any write that held the row:
			// the row goes if the address still has none, and is set to it otherwise. Only rows without a truth at the
			// snapshot, and rows a revert changed since it, pay for this count.
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
			if !sameBlock(v.firstSeen, v.wantFirst) || !sameBlock(v.lastSeen, v.wantLast) {
				// A first_seen or last_seen this sets may meet a block catch-up stored since the snapshot that ingest's
				// LEAST/GREATEST did not show (it was not past the wrong value): it is counted again under the row's
				// lock, which this takes first, so the next statement's snapshot follows any write that held it.
				// Only rows whose first or last block this corrects pay for it.
				writes.Queue(`SELECT 1 FROM address_stats WHERE address = $1 FOR UPDATE`, v.address)
				kinds = append(kinds, rebaseCorrect)
			}
			kinds = append(kinds, rebaseCorrect)
			writes.Queue(`
				UPDATE address_stats SET
					tx_count = COALESCE(tx_count, 0) + $2, sent_count = COALESCE(sent_count, 0) + $3,
					internal_tx_count = COALESCE(internal_tx_count, 0) + $4,
					token_transfer_count = COALESCE(token_transfer_count, 0) + $5,
					first_seen = CASE WHEN first_seen IS DISTINCT FROM $6 THEN LEAST(first_seen, $7)
					                  WHEN $7 IS NOT DISTINCT FROM $6 THEN first_seen
					                  ELSE (SELECT t.first_seen FROM ledger_address_truth(ARRAY[$1]) t) END,
					last_seen = CASE WHEN last_seen IS DISTINCT FROM $8 THEN GREATEST(last_seen, $9)
					                 WHEN $9 IS NOT DISTINCT FROM $8 THEN last_seen
					                 ELSE (SELECT t.last_seen FROM ledger_address_truth(ARRAY[$1]) t) END,
					updated_at = NOW()
				WHERE address = $1`, v.address, v.wantTxs-v.txs, v.wantSent-v.sent, v.wantInternal-v.internal,
				v.wantTransf-v.transfers, v.firstSeen, v.wantFirst, v.lastSeen, v.wantLast)
		case !v.present && v.seen && v.creatable && v.differs() && !stale[v.address]:
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
	if writes.Len() == 0 && len(revertedAddresses) == 0 && after == nil {
		return r, nil
	}
	err = func() error {
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
		if after != nil {
			return after(ctx, tx)
		}
		return nil
	}()
	return r, err
}

// rebasePendingSpans rebases the addresses of every closed span not rebased yet, oldest first, together with the
// addresses of blocks reverted since the last rebase, and then recounts the days' address columns if a first_seen
// moved. A span is marked rebased once its addresses are; a restart resumes at the first one that is not.
//
// A span holds up to millions of addresses, so its truth is counted for all of them at once: one statement, so one
// snapshot, joins them with the closed ledger rows and with one pass over the rows above the closed spans, reads
// their address_stats rows, and stages the rows whose correction is not zero (address_rebase_stage). The
// corrections are then applied in short byte-ordered batches. Counted address by address through the indexes, a
// span of a million addresses took about a day. The whole span runs under the rebase lock, held on one
// connection; a stage left by a crash is counted again, since another rebase may have applied it since.
func (d *DB) rebasePendingSpans(ctx context.Context, workMem string) error {
	if !d.ledgerIndexesReadyFor(ctx, "span rebase") {
		return nil
	}
	if err := d.recountPendingDays(ctx); err != nil {
		return err
	}
	rows, err := d.pool.Query(ctx, `SELECT lo FROM ledger_spans WHERE rebased_at IS NULL AND source <> 'merged' ORDER BY lo`)
	if err != nil {
		return err
	}
	los, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil || len(los) == 0 {
		return err
	}
	// The spans run on one connection, given back before the day recount takes one: a pool of a single connection,
	// which startup accepts, would otherwise wait for itself.
	if err := d.rebaseSpans(ctx, los, workMem); err != nil {
		return err
	}
	return d.recountPendingDays(ctx)
}

// rebaseSpans rebases the given closed spans, oldest first, on one connection holding the rebase lock.
func (d *DB) rebaseSpans(ctx context.Context, los []int64, workMem string) error {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1, 1)`, rebaseLock); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1, 1)`, rebaseLock) }()
	for _, lo := range los {
		start := time.Now()
		staged, err := stageSpanRebase(ctx, conn.Conn(), lo, workMem)
		if err != nil {
			return err
		}
		if spanRebaseStagedHook != nil {
			spanRebaseStagedHook(lo)
		}
		var result RebaseResult
		for {
			var batch []rebaseRow
			if err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				rows, err := tx.Query(ctx, `
					DELETE FROM address_rebase_stage
					WHERE address IN (SELECT address FROM address_rebase_stage ORDER BY address LIMIT $1)
					RETURNING address, creatable, present, seen, txs, sent, internal, transfers, first_seen, last_seen,
					          want_txs, want_sent, want_internal, want_transfers, want_first, want_last, reverted`, rebaseBatch)
				if err != nil {
					return err
				}
				batch, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (rebaseRow, error) {
					var v rebaseRow
					return v, row.Scan(&v.address, &v.creatable, &v.present, &v.seen, &v.txs, &v.sent, &v.internal, &v.transfers,
						&v.firstSeen, &v.lastSeen, &v.wantTxs, &v.wantSent, &v.wantInternal, &v.wantTransf, &v.wantFirst,
						&v.wantLast, &v.reverted)
				})
				if err != nil || len(batch) == 0 {
					return err
				}
				r, err := applyRebaseRows(ctx, tx, batch, nil)
				result.add(r)
				return err
			}); err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
		}
		if _, err := conn.Exec(ctx, `UPDATE ledger_spans SET rebased_at = now() WHERE lo = $1`, lo); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, `DROP TABLE address_rebase_stage`); err != nil {
			return err
		}
		log.Info("ledger: span's addresses rebased", "lo", lo, "staged", staged.Staged, "changed", result.Changed,
			"created", result.Created, "removed", result.Removed, "elapsed", time.Since(start).Round(time.Millisecond),
			"wrong_txs", staged.Txs, "wrong_sent", staged.Sent, "wrong_internal", staged.Internal,
			"wrong_transfers", staged.Transfers, "wrong_first_seen", staged.FirstSeen, "wrong_last_seen", staged.LastSeen,
			"missing", staged.Created, "no_truth", staged.NoTruth)
	}
	return nil
}

// spanCorrections counts what a span's rebase staged: rows, and the rows whose each column differs from the truth,
// that are missing (Created), or whose address has no truth (NoTruth, removed).
type spanCorrections struct {
	Staged, Txs, Sent, Internal, Transfers, FirstSeen, LastSeen, Created, NoTruth int64
}

// stageSpanSelect selects the rows address_rebase_stage holds for span $1, counting the rows at or above $2, the end of
// the closed spans, in one pass over each table.
const stageSpanSelect = `
		WITH addresses AS (
			SELECT address, true AS creatable FROM ledger_address
			WHERE lo = $1 AND address <> '0x0000000000000000000000000000000000000000'
			UNION ALL
			SELECT address, false FROM ledger_reverted_address
			WHERE address NOT IN (SELECT address FROM ledger_address WHERE lo = $1)
		),
		parts AS (
			SELECT l.address, sum(l.txs) AS txs, sum(l.sent) AS sent, sum(l.internal) AS internal,
			       sum(l.transfers) AS transfers, min(l.first_seen) AS first_seen, max(l.last_seen) AS last_seen
			FROM ledger_address l JOIN addresses a USING (address) GROUP BY l.address
			UNION ALL
			SELECT l.address, l.txs, l.sent, l.internal, l.transfers, l.first_seen, l.last_seen
			FROM ledger_stage_address l JOIN addresses a USING (address) WHERE l.lo = stats_backfill_open_span()
			UNION ALL
			SELECT y.address, count(y.tx), sum(y.sent), sum(y.internal), sum(y.transfers), min(y.first_block), max(y.last_block)
			FROM (
				SELECT x.address, x.tx, sum(x.sent) AS sent, sum(x.internal) AS internal, sum(x.transfers) AS transfers,
				       min(x.block) AS first_block, max(x.block) AS last_block
				FROM (
					SELECT v.address, t.hash AS tx, t.block_number AS block, v.sent, 0 AS internal, 0 AS transfers
					FROM transactions t
					CROSS JOIN LATERAL (VALUES (t.from_address, CASE WHEN t.tx_type BETWEEN 0 AND 4 THEN 1 ELSE 0 END),
					                           (t.to_address, 0)) v(address, sent)
					WHERE t.block_number >= $2 AND v.address IS NOT NULL
					UNION ALL
					SELECT v.address, t.tx_hash, t.block_number, 0, 0, 1
					FROM token_transfers t
					CROSS JOIN LATERAL (VALUES (t.from_address), (NULLIF(t.to_address, t.from_address))) v(address)
					WHERE t.block_number >= $2 AND v.address IS NOT NULL
					  AND v.address <> '0x0000000000000000000000000000000000000000'
					UNION ALL
					SELECT v.address, NULL, t.block_number, 0, 1, 0
					FROM internal_transactions t
					CROSS JOIN LATERAL (VALUES (t.from_address), (NULLIF(t.to_address, t.from_address))) v(address)
					WHERE t.block_number >= $2 AND v.address IS NOT NULL
					UNION ALL
					SELECT address, NULL, block_number, 0, 0, 0 FROM contracts WHERE block_number >= $2
				) x JOIN addresses a USING (address)
				GROUP BY x.address, x.tx
			) y
			GROUP BY y.address
		),
		truth AS (
			SELECT address, sum(txs)::BIGINT AS txs, sum(sent)::BIGINT AS sent, sum(internal)::BIGINT AS internal,
			       sum(transfers)::BIGINT AS transfers, min(first_seen) AS first_seen, max(last_seen) AS last_seen
			FROM parts GROUP BY address
		)
		SELECT a.address, a.creatable, s.address IS NOT NULL AS present, t.address IS NOT NULL AS seen,
		       COALESCE(s.tx_count, 0) AS txs, COALESCE(s.sent_count, 0) AS sent, COALESCE(s.internal_tx_count, 0) AS internal,
		       COALESCE(s.token_transfer_count, 0) AS transfers, s.first_seen, s.last_seen,
		       COALESCE(t.txs, 0) AS want_txs, COALESCE(t.sent, 0) AS want_sent, COALESCE(t.internal, 0) AS want_internal,
		       COALESCE(t.transfers, 0) AS want_transfers, t.first_seen AS want_first, t.last_seen AS want_last,
		       (SELECT r.xmin::TEXT FROM ledger_reverted_address r WHERE r.address = a.address) AS reverted
		FROM addresses a
		LEFT JOIN address_stats s ON s.address = a.address
		LEFT JOIN truth t ON t.address = a.address
		WHERE (s.address IS NOT NULL
		       AND (COALESCE(s.tx_count, 0), COALESCE(s.sent_count, 0), COALESCE(s.internal_tx_count, 0),
		            COALESCE(s.token_transfer_count, 0), s.first_seen, s.last_seen)
		           IS DISTINCT FROM (COALESCE(t.txs, 0), COALESCE(t.sent, 0), COALESCE(t.internal, 0), COALESCE(t.transfers, 0),
		                             t.first_seen, t.last_seen))
		   OR (s.address IS NULL AND a.creatable AND t.address IS NOT NULL)
		   OR EXISTS (SELECT 1 FROM ledger_reverted_address r WHERE r.address = a.address)`

// stageSpanSettings sets the staging transaction's work_mem, the closing transaction's ("" keeps the server's).
func stageSpanSettings(ctx context.Context, tx pgx.Tx, workMem string) error {
	if workMem == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `SELECT set_config('work_mem', $1, true)`, workMem)
	return err
}

// stageSpanRebase stages, in one statement, the addresses of span lo and of blocks reverted since the last rebase whose
// row differs from their truth, with the row and the truth it read, and counts them by column.
func stageSpanRebase(ctx context.Context, conn *pgx.Conn, lo int64, workMem string) (spanCorrections, error) {
	var c spanCorrections
	if _, err := conn.Exec(ctx, `DROP TABLE IF EXISTS address_rebase_stage`); err != nil {
		return c, err
	}
	if _, err := conn.Exec(ctx, `ANALYZE ledger_address`); err != nil {
		return c, err
	}
	if err := pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		if err := stageSpanSettings(ctx, tx, workMem); err != nil {
			return err
		}
		var closed int64
		if err := tx.QueryRow(ctx, `SELECT ledger_closed_through()`).Scan(&closed); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `CREATE UNLOGGED TABLE address_rebase_stage AS `+stageSpanSelect, lo, closed)
		return err
	}); err != nil {
		return c, err
	}
	if _, err := conn.Exec(ctx, `ALTER TABLE address_rebase_stage ADD PRIMARY KEY (address)`); err != nil {
		return c, err
	}
	err := conn.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE present AND txs <> want_txs), count(*) FILTER (WHERE present AND sent <> want_sent),
		       count(*) FILTER (WHERE present AND internal <> want_internal),
		       count(*) FILTER (WHERE present AND transfers <> want_transfers),
		       count(*) FILTER (WHERE present AND first_seen IS DISTINCT FROM want_first),
		       count(*) FILTER (WHERE present AND last_seen IS DISTINCT FROM want_last),
		       count(*) FILTER (WHERE NOT present AND creatable AND seen), count(*) FILTER (WHERE present AND NOT seen)
		FROM address_rebase_stage`).Scan(&c.Staged, &c.Txs, &c.Sent, &c.Internal, &c.Transfers, &c.FirstSeen, &c.LastSeen, &c.Created,
		&c.NoTruth)
	return c, err
}

// recountPendingDays recounts the days' address columns from address_stats if a rebase has moved a first_seen since
// the last recount, under daily_stats_advance's lock and holding no address_stats row. A batch that moves one counts
// it in ledger_verify_state in its own transaction, and the recount clears only the count it read, so a recount that
// fails, or a rebase that commits while it runs, leaves it pending for the next. The count is read under the lock:
// only a recount, which holds it, sets the count back, so while the lock is held the count only grows, and a count
// equal to the one read means nothing moved since.
func (d *DB) recountPendingDays(ctx context.Context) error {
	return pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`); err != nil {
			return err
		}
		var pending int64
		if err := tx.QueryRow(ctx, `SELECT value FROM ledger_verify_state WHERE name = 'address_days_recount'`).Scan(&pending); err != nil {
			return err
		}
		if pending == 0 {
			return nil
		}
		if err := recountDailyAddressColumns(ctx, tx); err != nil {
			return err
		}
		if daysRecountHook != nil {
			daysRecountHook()
		}
		_, err := tx.Exec(ctx, `UPDATE ledger_verify_state SET value = 0 WHERE name = 'address_days_recount' AND value = $1`, pending)
		return err
	})
}

// daysRecountHook, when a test sets it, runs between a day recount and its clearing of the pending count.
var daysRecountHook func()
