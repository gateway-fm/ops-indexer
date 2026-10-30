package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// AddressStatsRepairTable and addressStatsRepairTotalTable are the stage of the repair an earlier release ran. That
// repair is gone; a stage it left is dropped (DropAddressStatsRepairStage), applied or not.
const (
	AddressStatsRepairTable      = "address_stats_repair"
	addressStatsRepairTotalTable = "address_stats_repair_total"
)

// addressStatsRevertsCounter is the chain_counters row every reorg revert of address_stats adds one to. Nothing in this
// release reads it. The earlier release's staged repair compares it, so after a rollback to that release a stage is
// still found stale when a revert this release ran came after it.
const addressStatsRevertsCounter = "address_stats_reverts"

// ErrAddressStatsRepairStale is the earlier release's error for a stage a revert came after; kept with the counter.
var ErrAddressStatsRepairStale = errors.New("a reorg ran after the address_stats repair was staged")

// AddressBuckets is how many address buckets the repair walks, as the ledger reconciliation does.
const AddressBuckets = addressBuckets

// addressRepairCursor is the ledger_verify_state row holding the next bucket of a repair that has not finished.
const addressRepairCursor = "address_repair_next"

// AddressRepairBatch is the default number of addresses one repair transaction rebases; AddressRepairBatchTimeout is
// the longest one may run. A batch that runs out of time is split in half and run again; one address that does stops
// the repair. A batch holds the rebase lock (span closes and the running check wait for it) for its whole
// transaction, and the rows it corrects only from its first write to its commit.
const (
	AddressRepairBatch        = 100
	AddressRepairBatchTimeout = 30 * time.Second
)

// repairBatchTimeout is AddressRepairBatchTimeout; a test shortens it.
var repairBatchTimeout = AddressRepairBatchTimeout

// AddressRepairDrift is what a bucket's repair changes, read without changing it: rows that differ from their truth
// (Changed), addresses with a truth and no row (Missing), and rows whose address has no truth (NoTruth).
type AddressRepairDrift struct {
	Addresses                 []string
	Changed, Missing, NoTruth int
}

// AddressRepairDriftIn reads, in one statement, the addresses of bucket whose address_stats row differs from their
// truth (ledger_address_truth's: the closed ledger, the open stats backfill span and the rows above the closed spans).
// It writes nothing.
func (d *DB) AddressRepairDriftIn(ctx context.Context, bucket int) (AddressRepairDrift, error) {
	var drift AddressRepairDrift
	rows, err := d.pool.Query(ctx, `
		SELECT key, bool_and(running IS NULL), bool_and(expected IS NULL)
		FROM ledger_running_diff_address($1) GROUP BY key ORDER BY key`, bucket)
	if err != nil {
		return drift, err
	}
	defer rows.Close()
	for rows.Next() {
		var address string
		var missing, noTruth bool
		if err := rows.Scan(&address, &missing, &noTruth); err != nil {
			return drift, err
		}
		drift.Addresses = append(drift.Addresses, address)
		switch {
		case missing:
			drift.Missing++
		case noTruth:
			drift.NoTruth++
		default:
			drift.Changed++
		}
	}
	return drift, rows.Err()
}

// RepairAddressBucket sets every drifted address of bucket to its truth through the span rebase, batch addresses a
// transaction, then recounts the days whose address columns that moved. A row is created for an address with a truth
// and removed for one without.
func (d *DB) RepairAddressBucket(ctx context.Context, bucket, batch int) (RebaseResult, error) {
	drift, err := d.AddressRepairDriftIn(ctx, bucket)
	if err != nil {
		return RebaseResult{}, err
	}
	var total RebaseResult
	for start := 0; start < len(drift.Addresses); start += batch {
		r, err := d.repairAddresses(ctx, drift.Addresses[start:min(start+batch, len(drift.Addresses))])
		total.add(r)
		if err != nil {
			return total, err
		}
	}
	return total, d.recountPendingDays(ctx)
}

// repairAddresses rebases addresses in one transaction: the rebase lock with no deadline, then the count and the
// writes bounded by AddressRepairBatchTimeout, halving the batch when they ran out of time.
func (d *DB) repairAddresses(ctx context.Context, addresses []string) (RebaseResult, error) {
	var r RebaseResult
	var countErr error
	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		// The rebase lock first, with no deadline: a span close holds it for as long as its rebase takes, and a batch
		// that only waited for it has not run long.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, 1)`, rebaseLock); err != nil {
			return err
		}
		batchCtx, cancel := context.WithTimeout(ctx, repairBatchTimeout)
		defer cancel()
		var err error
		r, err = rebaseAddressBatchLocked(batchCtx, tx, addresses, addresses, nil)
		if err != nil && batchCtx.Err() != nil && ctx.Err() == nil {
			countErr = err
		}
		return err
	})
	if err == nil || countErr == nil {
		return r, err
	}
	if len(addresses) == 1 {
		return r, fmt.Errorf("address %s: its rebase ran longer than %s: %w", addresses[0], repairBatchTimeout, countErr)
	}
	half := len(addresses) / 2
	first, err := d.repairAddresses(ctx, addresses[:half])
	if err != nil {
		return first, err
	}
	second, err := d.repairAddresses(ctx, addresses[half:])
	first.add(second)
	return first, err
}

// AddressRepairNext is the bucket an unfinished repair resumes at, 0 when none is unfinished.
func (d *DB) AddressRepairNext(ctx context.Context) (int, error) {
	var next int
	err := d.pool.QueryRow(ctx, `SELECT value FROM ledger_verify_state WHERE name = $1`, addressRepairCursor).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return next, err
}

// SetAddressRepairNext records that the buckets below next are repaired.
func (d *DB) SetAddressRepairNext(ctx context.Context, next int) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO ledger_verify_state (name, value) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value`, addressRepairCursor, next)
	return err
}

// FinishAddressRepair forgets a finished repair's place, so the next one starts at the first bucket.
func (d *DB) FinishAddressRepair(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM ledger_verify_state WHERE name = $1`, addressRepairCursor)
	return err
}

// AddressStatsRepairStageExists reports whether an earlier release left a repair stage.
func (d *DB) AddressStatsRepairStageExists(ctx context.Context) (bool, error) {
	var exists bool
	err := d.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL OR to_regclass($2) IS NOT NULL`,
		AddressStatsRepairTable, addressStatsRepairTotalTable).Scan(&exists)
	return exists, err
}

// DropAddressStatsRepairStage drops a stage an earlier release left, applied in part or not at all. The repair of this
// release recounts every address against its truth, so whatever the stage applied is corrected along with the rest.
func (d *DB) DropAddressStatsRepairStage(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, `DROP TABLE IF EXISTS `+AddressStatsRepairTable+`, `+addressStatsRepairTotalTable)
	return err
}
