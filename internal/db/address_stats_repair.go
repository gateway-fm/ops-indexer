package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

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

// AddressStatsRepairTable holds the corrections a repair has staged and not applied yet.
const AddressStatsRepairTable = "address_stats_repair"

// addressStatsRepairTotalTable holds how many corrections were staged and how many are applied, so a repair is done
// only when every staged correction was applied.
const addressStatsRepairTotalTable = "address_stats_repair_total"

// ErrAddressStatsRepairPending: corrections are staged and not all applied; apply them before staging again.
var ErrAddressStatsRepairPending = errors.New("a staged address_stats repair is not applied yet")

// ErrAddressStatsRepairLost: staged corrections disappeared before they were applied (an unlogged staging table from
// an earlier build emptied by a crash or a failover, or one dropped by hand). The stage is removed; stage again.
var ErrAddressStatsRepairLost = errors.New("staged address_stats corrections were lost before they were applied")

// AddressStatsRepairStage describes what a staged repair changes.
type AddressStatsRepairStage struct {
	LedgerClosedBelow, Head uint64
	// Changes counts the addresses whose row is wrong; Missing those with no row at all.
	Changes, Missing int64
	// Untouched are address_stats rows the ledger and the retained rows know nothing about: reported, not changed.
	Untouched int64
	Largest   []AddressStatsCorrection
}

type AddressStatsCorrection struct {
	Address                  string
	Txs, Internal, Transfers int64
	FirstSeen, LastSeen      uint64
	Missing                  bool
}

// StageAddressStatsRepair computes, in one snapshot, what every address's counters should be (its closed ledger rows
// plus the retained rows above the ledger, counted as ingest counts them) against what address_stats holds, and
// keeps only the differences. Dry, it reports them and keeps nothing. Otherwise they go to AddressStatsRepairTable
// for ApplyAddressStatsRepairBatch; a table that still holds unapplied corrections is never restaged over.
//
// The corrections are additive: expected minus actual at the snapshot. Ingest only ever adds deltas, for blocks
// above the snapshot, so a correction stays right whatever ingest commits before or while it is applied.
func (d *DB) StageAddressStatsRepair(ctx context.Context, dry bool) (*AddressStatsRepairStage, error) {
	stage := &AddressStatsRepairStage{}
	table := AddressStatsRepairTable
	if dry {
		table = "address_stats_repair_dry"
	}
	err := pgx.BeginTxFunc(ctx, d.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, historyDDLGate); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('work_mem', '256MB', true)`); err != nil {
			return err
		}
		if !dry {
			var pending bool
			if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL OR to_regclass($2) IS NOT NULL`,
				table, addressStatsRepairTotalTable).Scan(&pending); err != nil {
				return err
			}
			if pending {
				return ErrAddressStatsRepairPending
			}
		}
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE((SELECT max(hi) FROM ledger_spans), 0), COALESCE((SELECT max(number) FROM blocks), 0)`).
			Scan(&stage.LedgerClosedBelow, &stage.Head); err != nil {
			return err
		}
		create := `CREATE TABLE ` + table
		if dry {
			create = `CREATE TEMP TABLE ` + table
		}
		if _, err := tx.Exec(ctx, create+` (
			address TEXT PRIMARY KEY, txs BIGINT NOT NULL, internal BIGINT NOT NULL, transfers BIGINT NOT NULL,
			first_seen BIGINT NOT NULL, last_seen BIGINT NOT NULL, missing BOOLEAN NOT NULL)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			CREATE TEMP TABLE address_stats_expected ON COMMIT DROP AS
			SELECT address, sum(txs) AS txs, sum(internal) AS internal, sum(transfers) AS transfers,
			       min(first_seen) AS first_seen, max(last_seen) AS last_seen
			FROM (
				SELECT address, txs, internal, transfers, first_seen, last_seen FROM ledger_address
				UNION ALL
				SELECT * FROM (`+ledgerAddressRowsSQL+`) tail
			) u
			GROUP BY address`, int64(stage.LedgerClosedBelow), int64(stage.Head)+1); err != nil {
			return fmt.Errorf("expected counts: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO `+table+`
			SELECT e.address, e.txs - COALESCE(a.tx_count, 0), e.internal - COALESCE(a.internal_tx_count, 0),
			       e.transfers - COALESCE(a.token_transfer_count, 0), e.first_seen, e.last_seen, a.address IS NULL
			FROM address_stats_expected e LEFT JOIN address_stats a ON a.address = e.address
			WHERE a.address IS NULL
			   OR e.txs <> COALESCE(a.tx_count, 0) OR e.internal <> COALESCE(a.internal_tx_count, 0)
			   OR e.transfers <> COALESCE(a.token_transfer_count, 0)
			   OR e.first_seen < COALESCE(a.first_seen, e.first_seen + 1) OR e.last_seen > COALESCE(a.last_seen, e.last_seen - 1)`); err != nil {
			return fmt.Errorf("stage: %w", err)
		}
		if err := tx.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM `+table+`), (SELECT count(*) FROM `+table+` WHERE missing),
			       (SELECT count(*) FROM address_stats a WHERE NOT EXISTS (SELECT 1 FROM address_stats_expected e WHERE e.address = a.address))`).
			Scan(&stage.Changes, &stage.Missing, &stage.Untouched); err != nil {
			return err
		}
		if !dry {
			if _, err := tx.Exec(ctx, `CREATE TABLE `+addressStatsRepairTotalTable+` (staged BIGINT NOT NULL, applied BIGINT NOT NULL)`); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+addressStatsRepairTotalTable+` VALUES ($1, 0)`, stage.Changes); err != nil {
				return err
			}
		}
		rows, err := tx.Query(ctx, `
			SELECT address, txs, internal, transfers, first_seen, last_seen, missing FROM `+table+`
			ORDER BY abs(txs) + abs(internal) + abs(transfers) DESC, address LIMIT 10`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c AddressStatsCorrection
			var first, last int64
			if err := rows.Scan(&c.Address, &c.Txs, &c.Internal, &c.Transfers, &first, &last, &c.Missing); err != nil {
				return err
			}
			c.FirstSeen, c.LastSeen = uint64(first), uint64(last)
			stage.Largest = append(stage.Largest, c)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if dry {
			return errDryRun
		}
		return nil
	})
	if errors.Is(err, errDryRun) {
		err = nil
	}
	return stage, err
}

var errDryRun = errors.New("dry run")

// ApplyAddressStatsRepairBatch applies the next batch of staged corrections, in address order (the order ingest locks
// address_stats rows in), and removes them from the staging table in the same transaction, so a stopped repair
// resumes at the next batch and never applies one twice. Counters move by the correction, first_seen and last_seen
// only outward; a row that did not exist is created (is_contract from contracts) and counted in addresses_total, but
// only for an address that still has rows: one a reorg removed after the stage keeps no row. Such a correction is
// counted as handled, not applied.
// The staging table is dropped with the last batch, which also recounts the days' address columns from the repaired
// address_stats, under daily_stats_advance's lock: the repair moves first_seen and adds addresses. The last batch
// first checks that every staged correction was applied; if any is missing it removes the stage and returns
// ErrAddressStatsRepairLost instead of done, and the repair is staged again.
func (d *DB) ApplyAddressStatsRepairBatch(ctx context.Context, batch int) (applied, created int, done bool, err error) {
	var lost error
	var handled int
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var exists, totalled bool
		if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL, to_regclass($2) IS NOT NULL`,
			AddressStatsRepairTable, addressStatsRepairTotalTable).Scan(&exists, &totalled); err != nil {
			return err
		}
		if !exists && !totalled {
			done = true
			return nil
		}
		if !exists {
			lost = fmt.Errorf("%w: the staging table is gone", ErrAddressStatsRepairLost)
			_, err := tx.Exec(ctx, `DROP TABLE `+addressStatsRepairTotalTable)
			return err
		}
		if err := tx.QueryRow(ctx, `
			WITH next AS (
				DELETE FROM `+AddressStatsRepairTable+` r
				WHERE r.address IN (SELECT address FROM `+AddressStatsRepairTable+` ORDER BY address LIMIT $1)
				RETURNING r.*
			), kept AS (
				-- A correction creates a row only for an address that still has rows: one a reorg removed since the
				-- stage (no remaining block had seen it) stays without a row, as the revert left it.
				SELECT n.* FROM next n
				WHERE EXISTS (SELECT 1 FROM address_stats a WHERE a.address = n.address)
				   OR EXISTS (SELECT 1 FROM transactions t WHERE t.from_address = n.address)
				   OR EXISTS (SELECT 1 FROM transactions t WHERE t.to_address = n.address)
				   OR EXISTS (SELECT 1 FROM token_transfers t WHERE t.from_address = n.address)
				   OR EXISTS (SELECT 1 FROM token_transfers t WHERE t.to_address = n.address)
				   OR EXISTS (SELECT 1 FROM internal_transactions t WHERE t.from_address = n.address)
				   OR EXISTS (SELECT 1 FROM internal_transactions t WHERE t.to_address = n.address)
				   OR EXISTS (SELECT 1 FROM contracts c WHERE c.address = n.address)
				   OR EXISTS (SELECT 1 FROM ledger_spans s WHERE EXISTS (
				          SELECT 1 FROM ledger_address l WHERE l.lo = s.lo AND l.address = n.address))
			), applied AS (
				INSERT INTO address_stats AS a (address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, updated_at)
				SELECT n.address, n.txs, n.internal, n.transfers, n.first_seen, n.last_seen,
				       EXISTS (SELECT 1 FROM contracts c WHERE c.address = n.address), NOW()
				FROM kept n ORDER BY n.address
				ON CONFLICT (address) DO UPDATE SET
					tx_count = a.tx_count + EXCLUDED.tx_count,
					internal_tx_count = a.internal_tx_count + EXCLUDED.internal_tx_count,
					token_transfer_count = a.token_transfer_count + EXCLUDED.token_transfer_count,
					first_seen = LEAST(a.first_seen, EXCLUDED.first_seen),
					last_seen = GREATEST(a.last_seen, EXCLUDED.last_seen),
					updated_at = NOW()
				RETURNING (xmax = 0) AS created
			), counted AS (
				UPDATE chain_counters SET count = count + (SELECT count(*) FROM applied WHERE created), updated_at = NOW()
				WHERE name = 'addresses_total' AND EXISTS (SELECT 1 FROM applied WHERE created)
			)
			SELECT (SELECT count(*) FROM next), count(*), count(*) FILTER (WHERE created) FROM applied`, batch).
			Scan(&handled, &applied, &created); err != nil {
			return err
		}
		if handled > 0 {
			if totalled {
				_, err := tx.Exec(ctx, `UPDATE `+addressStatsRepairTotalTable+` SET applied = applied + $1`, handled)
				return err
			}
			return nil
		}
		var staged, appliedSoFar int64 = -1, 0
		if totalled {
			if err := tx.QueryRow(ctx, `SELECT staged, applied FROM `+addressStatsRepairTotalTable).Scan(&staged, &appliedSoFar); err != nil &&
				!errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}
		if staged != appliedSoFar {
			lost = fmt.Errorf("%w: %d staged, %d applied", ErrAddressStatsRepairLost, staged, appliedSoFar)
			if staged < 0 {
				lost = fmt.Errorf("%w: the stage has no recorded total, so whether it was all applied is unknown", ErrAddressStatsRepairLost)
			}
			if _, err := tx.Exec(ctx, `DROP TABLE `+AddressStatsRepairTable); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `DROP TABLE IF EXISTS `+addressStatsRepairTotalTable)
			return err
		}
		done = true
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`); err != nil {
			return err
		}
		if err := recountDailyAddressColumns(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DROP TABLE `+AddressStatsRepairTable); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DROP TABLE `+addressStatsRepairTotalTable)
		return err
	})
	if err == nil && lost != nil {
		err = lost
	}
	return applied, created, done, err
}
