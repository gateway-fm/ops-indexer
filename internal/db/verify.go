package db

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

const addressBuckets = 256

type VerifyConfig struct {
	// RecountInterval paces the retained-span recount and the running check: one span and one address bucket per tick.
	RecountInterval time.Duration
	Repair          bool
	// WorkMem is the recount transaction's work_mem (LEDGER_WORK_MEM).
	WorkMem string
	// MaxOpenTail is the most blocks above the ledger's closed spans for which the running check and the address
	// reconciliation run: beyond it they would read most of history every pass. 0 means no limit.
	MaxOpenTail int64
}

// MaxOpenTail is the open tail up to which the running check and the address reconciliation run for spans of
// spanBlocks: three spans above the PruneFloorGap blocks a close always leaves open, since a span closes only that far
// below the head.
func MaxOpenTail(spanBlocks uint64) int64 {
	return PruneFloorGap + 3*int64(spanBlocks)
}

// OpenTail is how many stored blocks lie above the ledger's closed spans.
func (d *DB) OpenTail(ctx context.Context) (int64, error) {
	var n int64
	err := d.pool.QueryRow(ctx, `SELECT GREATEST(COALESCE((SELECT max(number) FROM blocks), 0) + 1 - ledger_closed_through(), 0)`).Scan(&n)
	return n, err
}

func (d *DB) tailShort(ctx context.Context, cfg VerifyConfig) bool {
	if cfg.MaxOpenTail <= 0 {
		return true
	}
	tail, err := d.OpenTail(ctx)
	if err != nil {
		return false
	}
	if tail > cfg.MaxOpenTail {
		log.Debug("ledger: open tail too long for the running check; waiting for the closer", "blocks", tail, "max", cfg.MaxOpenTail)
		return false
	}
	return true
}

// ReconcileAddressBucket sets the addresses of the next bucket not reconciled yet whose counters disagree with the
// closed ledger rows plus the open tail to that count (RebaseAddresses), and then records the bucket done: an
// interrupted bucket is redone, which changes nothing it already set. done is true once all 256 are.
func (d *DB) ReconcileAddressBucket(ctx context.Context) (bucket, drifted int, done bool, err error) {
	if err := d.pool.QueryRow(ctx, `SELECT value FROM ledger_verify_state WHERE name = 'address_reconcile_next'`).Scan(&bucket); err != nil {
		return bucket, 0, false, err
	}
	if bucket >= addressBuckets {
		return bucket, 0, true, nil
	}
	rows, err := d.pool.Query(ctx, `SELECT DISTINCT key FROM ledger_running_diff_address($1)`, bucket)
	if err != nil {
		return bucket, 0, false, err
	}
	addresses, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return bucket, 0, false, err
	}
	r, err := d.RebaseAddresses(ctx, addresses, addresses)
	if err == nil {
		err = d.recountPendingDays(ctx)
	}
	if err != nil {
		return bucket, r.Changed, false, err
	}
	_, err = d.pool.Exec(ctx, `UPDATE ledger_verify_state SET value = $1 WHERE name = 'address_reconcile_next' AND value = $2`,
		bucket+1, bucket)
	return bucket, r.Changed, false, err
}

// reconcileAddresses runs the reconciliation bucket after bucket, a second apart, until it is done or the open tail
// grows too long; it reports whether it is done.
func (d *DB) reconcileAddresses(ctx context.Context, cfg VerifyConfig) bool {
	for ctx.Err() == nil && d.tailShort(ctx, cfg) {
		bucket, drifted, done, err := d.ReconcileAddressBucket(ctx)
		if err != nil {
			log.Warn("ledger: address reconciliation failed; retrying next interval", "error", err)
			return false
		}
		if done {
			return true
		}
		log.Info("ledger: address bucket reconciled", "bucket", bucket, "repaired_counters", drifted)
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	return false
}

// Drift is one counter that disagrees: stored (or running) against the recount.
type Drift struct {
	Family, Key, Counter string
	Stored, Recount      *string
}

// SpanCheck is the outcome of recounting one closed span.
type SpanCheck struct {
	Lo, Hi  int64
	Drifted int
	State   string
}

func inRepeatableRead(ctx context.Context, d *DB, fn func(pgx.Tx) error) error {
	for attempt := 1; ; attempt++ {
		err := pgx.BeginTxFunc(ctx, d.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, fn)
		var pgErr *pgconn.PgError
		if attempt < 5 && errors.As(err, &pgErr) && pgErr.Code == "40001" {
			continue
		}
		return err
	}
}

// RecountRetainedSpan recounts, from its history rows, the closed retained span checked longest ago and settles it
// (ledger_span_settle); ok is false when there is none.
//
// It holds the history-DDL gate shared from before its snapshot to its commit, as the closer does, so retention
// cannot detach or drop the span's range under the count: a range detached after the snapshot reads as no rows,
// and the settle would have written zeros over the span's counts. A span retention has pruned meanwhile is left
// to the node recount, and a recount whose blocks_total is not the span's width is never settled.
func (d *DB) RecountRetainedSpan(ctx context.Context, repair bool, workMem string) (c SpanCheck, ok bool, err error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return c, false, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock_shared($1, 1)`, historyDDLGate); err != nil {
		return c, false, err
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock_shared($1, 1)`, historyDDLGate)
	}()
	for attempt := 1; ; attempt++ {
		c, ok = SpanCheck{}, false
		err = pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
			if workMem != "" {
				if _, err := tx.Exec(ctx, `SELECT set_config('work_mem', $1, true)`, workMem); err != nil {
					return err
				}
			}
			err := tx.QueryRow(ctx, `
				SELECT lo, hi FROM ledger_spans
				WHERE lo >= (SELECT history_from()) AND check_state <> 'quarantined'
				ORDER BY checked_at NULLS FIRST, lo LIMIT 1`).Scan(&c.Lo, &c.Hi)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			ok = true
			if _, err := tx.Exec(ctx, `SELECT ledger_stage_from_rows($1, $2)`, c.Lo, c.Hi); err != nil {
				return err
			}
			if retainedRecountStagedHook != nil {
				retainedRecountStagedHook(c.Lo)
			}
			// The span's row, locked: a compaction that merged it since the snapshot makes this a serialization failure
			// and the recount starts over; one that comes later waits for this settle.
			var hi int64
			var source string
			if err := tx.QueryRow(ctx, `SELECT hi, source FROM ledger_spans WHERE lo = $1 FOR UPDATE`, c.Lo).Scan(&hi, &source); err != nil {
				return err
			}
			if hi != c.Hi || source == "merged" {
				c.State = "changed"
				return nil
			}
			var retained bool
			var blocks int64
			if err := tx.QueryRow(ctx, `
				SELECT $1 >= history_from(), COALESCE((SELECT value FROM ledger_stage_chain WHERE lo = $1 AND name = 'blocks_total'), 0)`,
				c.Lo).Scan(&retained, &blocks); err != nil {
				return err
			}
			if !retained {
				c.State = "pruned"
				return nil
			}
			if blocks != c.Hi-c.Lo && !(c.Lo == 0 && blocks == c.Hi-1) {
				log.Warn("ledger: a retained span's recount is missing blocks; not settled", "lo", c.Lo, "hi", c.Hi,
					"blocks", blocks)
				c.State = "incomplete"
				return nil
			}
			return settleSpan(ctx, tx, &c, repair)
		})
		var pgErr *pgconn.PgError
		if attempt < 5 && errors.As(err, &pgErr) && pgErr.Code == "40001" {
			continue
		}
		return c, ok, err
	}
}

// retainedRecountStagedHook, when a test sets it, runs after a retained span's recount staged its totals.
var retainedRecountStagedHook func(lo int64)

func settleSpan(ctx context.Context, tx pgx.Tx, c *SpanCheck, repair bool) error {
	drifts, err := collectDrifts(ctx, tx, `SELECT family, key, counter, stored::TEXT, recount::TEXT FROM ledger_span_diff($1)`, c.Lo)
	if err != nil {
		return err
	}
	for _, dr := range drifts {
		log.Warn("ledger: span drifted", "lo", c.Lo, "hi", c.Hi, "family", dr.Family, "key", dr.Key, "counter", dr.Counter,
			"stored", deref(dr.Stored), "recount", deref(dr.Recount))
	}
	if err := tx.QueryRow(ctx, `SELECT ledger_span_settle($1, $2)`, c.Lo, repair).Scan(&c.Drifted); err != nil {
		return err
	}
	return tx.QueryRow(ctx, `SELECT check_state FROM ledger_spans WHERE lo = $1`, c.Lo).Scan(&c.State)
}

func collectDrifts(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]Drift, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Drift, error) {
		var dr Drift
		err := r.Scan(&dr.Family, &dr.Key, &dr.Counter, &dr.Stored, &dr.Recount)
		return dr, err
	})
}

func deref(s *string) string {
	if s == nil {
		return "none"
	}
	return *s
}

var runningTables = map[string]struct{ table, key string }{
	"chain":   {"chain_counters", "name"},
	"token":   {"tokens", "address"},
	"address": {"address_stats", "address"},
}

var runningColumns = map[string]map[string]string{
	"chain":   {"count": "count"},
	"token":   {"transfers": "transfer_count", "supply": "total_supply"},
	"address": {"txs": "tx_count", "sent": "sent_count", "internal": "internal_tx_count", "transfers": "token_transfer_count"},
}

// CheckRunning compares the running chain and token counters, and the address counters of one bucket, with the
// closed ledger rows plus the open history rows, in one snapshot. With repair, each drifting counter is logged in
// ledger_repairs, or, if the key drifts again after a repair, quarantined instead. The repairs run after the snapshot,
// in READ COMMITTED: a chain or token counter moves by its drift, which concurrent writes do not change; an address is
// set to the ledger's count by RebaseAddresses. Under REPEATABLE READ any row ingest wrote after the snapshot failed
// the repair with a serialization error, and a hot row always had one.
func (d *DB) CheckRunning(ctx context.Context, bucket int, repair bool) ([]Drift, error) {
	var drifts []Drift
	err := inRepeatableRead(ctx, d, func(tx pgx.Tx) error {
		var err error
		drifts, err = collectDrifts(ctx, tx, `
			SELECT family, key, counter, running::TEXT, expected::TEXT FROM ledger_running_diff_chain_token()
			UNION ALL
			SELECT family, key, counter, running::TEXT, expected::TEXT FROM ledger_running_diff_address($1)`, bucket)
		return err
	})
	if err != nil || !repair || len(drifts) == 0 {
		return drifts, err
	}
	return drifts, d.repairDrifts(ctx, drifts)
}

// repairDrifts repairs drifts the running check found. A chain or token counter is logged in ledger_repairs and moved
// by its drift in one transaction; a drifted address is rebased, its ledger_repairs rows written in the rebase batch's
// transaction, so a rebase that fails leaves no repair on record to quarantine the address at the next check.
func (d *DB) repairDrifts(ctx context.Context, drifts []Drift) error {
	// Tokens rows by address, then chain_counters: the writers' order (a block write takes its tokens rows and
	// chain_counters last), so a repair never holds a counter a block write waits for while it waits for a token.
	drifts = slices.Clone(drifts)
	rank := map[string]int{"token": 0, "chain": 1, "address": 2}
	sort.SliceStable(drifts, func(i, j int) bool {
		if rank[drifts[i].Family] != rank[drifts[j].Family] {
			return rank[drifts[i].Family] < rank[drifts[j].Family]
		}
		return drifts[i].Key < drifts[j].Key
	})
	byAddress := map[string][]Drift{}
	var addresses []string
	if err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		for _, dr := range drifts {
			repair, err := repairable(ctx, tx, dr)
			if err != nil || !repair {
				if err != nil {
					return err
				}
				continue
			}
			if dr.Family == "address" {
				if byAddress[dr.Key] == nil {
					addresses = append(addresses, dr.Key)
				}
				byAddress[dr.Key] = append(byAddress[dr.Key], dr)
				continue
			}
			if err := settleRunning(ctx, tx, dr); err != nil {
				return err
			}
		}
		return nil
	}); err != nil || len(addresses) == 0 {
		return err
	}
	if _, err := d.rebaseAddresses(ctx, addresses, addresses, func(ctx context.Context, tx pgx.Tx, batch []string) error {
		for _, address := range batch {
			for _, dr := range byAddress[address] {
				if err := logRepair(ctx, tx, dr); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return d.recountPendingDays(ctx)
}

// repairable logs a drift and says whether to repair it: a key quarantined, or one that drifted again after a repair
// (which quarantines it now), is not.
func repairable(ctx context.Context, tx pgx.Tx, dr Drift) (bool, error) {
	log.Warn("ledger: running counter drifted", "family", dr.Family, "key", dr.Key, "counter", dr.Counter,
		"running", deref(dr.Stored), "expected", deref(dr.Recount))
	var quarantined, repairedBefore bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM ledger_quarantine WHERE family = $1 AND key = $2),
		       EXISTS (SELECT 1 FROM ledger_repairs WHERE lo IS NULL AND family = $1 AND key = $2 AND counter = $3)`,
		dr.Family, dr.Key, dr.Counter).Scan(&quarantined, &repairedBefore); err != nil {
		return false, err
	}
	if quarantined {
		return false, nil
	}
	if repairedBefore {
		log.Error("ledger: running counter drifted again after a repair; quarantined", "family", dr.Family, "key", dr.Key, "counter", dr.Counter)
		_, err := tx.Exec(ctx, `INSERT INTO ledger_quarantine (family, key) VALUES ($1, $2) ON CONFLICT DO NOTHING`, dr.Family, dr.Key)
		return false, err
	}
	return true, nil
}

func logRepair(ctx context.Context, tx pgx.Tx, dr Drift) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO ledger_repairs (family, key, counter, stored, repaired) VALUES ($1, $2, $3, $4::NUMERIC, $5::NUMERIC)`,
		dr.Family, dr.Key, dr.Counter, dr.Stored, dr.Recount)
	return err
}

// runningTruth is a chain or token counter's truth: its closed ledger rows, the open stats backfill span's, and the
// open tail's history rows, read in one statement, as ledger_running_diff_chain_token reads them for every key.
var runningTruth = map[string]string{
	"chain": `
		SELECT COALESCE(sum(v), 0) FROM (
			SELECT value AS v FROM ledger_chain WHERE name = $1
			UNION ALL SELECT value FROM ledger_stage_chain WHERE lo = stats_backfill_open_span() AND name = $1
			UNION ALL SELECT count(*) FROM blocks WHERE $1 = 'blocks_total' AND number >= ledger_closed_through()
			UNION ALL SELECT count(*) FROM transactions
				WHERE $1 = 'transactions_total' AND block_number >= ledger_closed_through()
			UNION ALL SELECT count(*) FROM token_transfers
				WHERE $1 = 'transfers_total' AND block_number >= ledger_closed_through()
		) u`,
	"token": `
		SELECT CASE $2 WHEN 'transfers' THEN COALESCE(sum(transfers), 0) ELSE COALESCE(sum(supply), 0) END FROM (
			SELECT transfers, supply FROM ledger_token WHERE token = $1
			UNION ALL SELECT transfers, supply FROM ledger_stage_token WHERE lo = stats_backfill_open_span() AND token = $1
			UNION ALL
			SELECT count(*),
			       COALESCE(SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
			              - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END), 0)
			FROM token_transfers WHERE token_address = $1 AND block_number >= ledger_closed_through()
		) u`,
}

// settleRunning puts a chain or token counter's repair on record, then sets the counter to its truth, read under the
// counter's row lock. Moving it by the drift the check read would overshoot after a reorg revert in between, which
// lowers the counter (stopping at zero), and the next check would quarantine the key. Every write that moves the
// counter takes that row lock, so none commits between the read and the write. A counter that already is its truth
// takes its repair off the record.
func settleRunning(ctx context.Context, tx pgx.Tx, dr Drift) error {
	column, ok := runningColumns[dr.Family][dr.Counter]
	if !ok {
		return fmt.Errorf("ledger: no running column for %s %s", dr.Family, dr.Counter)
	}
	var logged string
	if err := tx.QueryRow(ctx, `
		INSERT INTO ledger_repairs (family, key, counter, stored, repaired) VALUES ($1, $2, $3, $4::NUMERIC, $5::NUMERIC)
		RETURNING ctid::TEXT`, dr.Family, dr.Key, dr.Counter, dr.Stored, dr.Recount).Scan(&logged); err != nil {
		return err
	}
	t := runningTables[dr.Family]
	col := pgx.Identifier{column}.Sanitize()
	var running string
	err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(%s, 0)::TEXT FROM %s WHERE %s = $1 FOR UPDATE`, col, t.table, t.key),
		dr.Key).Scan(&running)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `DELETE FROM ledger_repairs WHERE ctid = $1::TID`, logged)
		return err
	}
	if err != nil {
		return err
	}
	args := []any{dr.Key}
	if dr.Family == "token" {
		args = append(args, dr.Counter)
	}
	var truth string
	if err := tx.QueryRow(ctx, `SELECT t::TEXT FROM (`+runningTruth[dr.Family]+`) AS truth(t)`, args...).Scan(&truth); err != nil {
		return err
	}
	var same bool
	if err := tx.QueryRow(ctx, `SELECT $1::NUMERIC = $2::NUMERIC`, running, truth).Scan(&same); err != nil {
		return err
	}
	if same {
		log.Info("ledger: a running counter was already right when its repair ran (a reorg revert in between); no repair",
			"family", dr.Family, "key", dr.Key, "counter", dr.Counter)
		_, err := tx.Exec(ctx, `DELETE FROM ledger_repairs WHERE ctid = $1::TID`, logged)
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET %s = $2::NUMERIC WHERE %s = $1`, t.table, col, t.key),
		dr.Key, truth); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE ledger_repairs SET stored = $2::NUMERIC, repaired = $3::NUMERIC WHERE ctid = $1::TID`,
		logged, running, truth)
	return err
}

// RunVerify recounts one retained span and checks the running counters of one address bucket per interval, with
// retention on or off.
func (d *DB) RunVerify(ctx context.Context, cfg VerifyConfig) {
	log.Info("ledger verification started", "interval", cfg.RecountInterval, "repair", cfg.Repair)
	if !d.waitLedgerIndexes(ctx, "ledger verification", cfg.RecountInterval) {
		return
	}
	bucket := 0
	recountCycle, runningCycle := time.Now(), time.Now()
	reconciled := !cfg.Repair
	for {
		if !reconciled {
			reconciled = d.reconcileAddresses(ctx, cfg)
		}
		if c, ok, err := d.RecountRetainedSpan(ctx, cfg.Repair, cfg.WorkMem); err != nil && ctx.Err() == nil {
			log.Warn("ledger: retained recount failed", "error", err)
		} else if ok {
			metrics.VerifyChecked("recount", c.Drifted, c.State)
			if done, err := d.retainedCycleDone(ctx, recountCycle); err == nil && done {
				metrics.VerifyCycle("recount", time.Since(recountCycle))
				recountCycle = time.Now()
			}
		}
		if d.tailShort(ctx, cfg) {
			d.checkRunningBucket(ctx, cfg, &bucket, &runningCycle)
		}
		if n, err := d.QuarantinedCount(ctx); err == nil {
			metrics.SetVerifyQuarantined(n)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(cfg.RecountInterval):
		}
	}
}

// checkRunningBucket runs the running check on one address bucket and moves to the next.
func (d *DB) checkRunningBucket(ctx context.Context, cfg VerifyConfig, bucket *int, cycle *time.Time) {
	drifts, err := d.CheckRunning(ctx, *bucket, cfg.Repair)
	if err != nil {
		if ctx.Err() == nil {
			log.Warn("ledger: running check failed", "bucket", *bucket, "error", err)
		}
		return
	}
	metrics.VerifyChecked("running", len(drifts), runningState(len(drifts), cfg.Repair))
	if *bucket = (*bucket + 1) % addressBuckets; *bucket == 0 {
		metrics.VerifyCycle("running", time.Since(*cycle))
		*cycle = time.Now()
		if err := d.countRunningCycle(ctx); err != nil {
			log.Warn("ledger: recording a running cycle failed", "error", err)
		}
	}
}

func runningState(drifted int, repair bool) string {
	switch {
	case drifted == 0:
		return "ok"
	case repair:
		return "repaired"
	default:
		return "drift"
	}
}

func (d *DB) retainedCycleDone(ctx context.Context, since time.Time) (bool, error) {
	var left bool
	err := d.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM ledger_spans WHERE lo >= (SELECT history_from()) AND check_state <> 'quarantined'
		               AND (checked_at IS NULL OR checked_at < $1))`, since).Scan(&left)
	return !left, err
}

// QuarantinedCount is the spans and running keys whose drift came back after a repair.
func (d *DB) QuarantinedCount(ctx context.Context) (int64, error) {
	var n int64
	err := d.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM ledger_spans WHERE check_state = 'quarantined') + (SELECT count(*) FROM ledger_quarantine)`).Scan(&n)
	return n, err
}
