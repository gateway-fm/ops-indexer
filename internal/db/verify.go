package db

import (
	"context"
	"errors"
	"fmt"
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
		err = d.recountDaysIfMoved(ctx, r)
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
func (d *DB) RecountRetainedSpan(ctx context.Context, repair bool, workMem string) (c SpanCheck, ok bool, err error) {
	err = inRepeatableRead(ctx, d, func(tx pgx.Tx) error {
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
		return settleSpan(ctx, tx, &c, repair)
	})
	return c, ok, err
}

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

func (d *DB) repairDrifts(ctx context.Context, drifts []Drift) error {
	var addresses []string
	seen := map[string]bool{}
	if err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		for _, dr := range drifts {
			apply, err := recordRepair(ctx, tx, dr)
			if err != nil || !apply {
				if err != nil {
					return err
				}
				continue
			}
			if dr.Family == "address" {
				if !seen[dr.Key] {
					seen[dr.Key] = true
					addresses = append(addresses, dr.Key)
				}
				continue
			}
			if err := applyRunning(ctx, tx, dr); err != nil {
				return err
			}
		}
		return nil
	}); err != nil || len(addresses) == 0 {
		return err
	}
	r, err := d.RebaseAddresses(ctx, addresses, addresses)
	if err != nil {
		return err
	}
	return d.recountDaysIfMoved(ctx, r)
}

// recordRepair logs a drift and says whether to repair it: a key that drifted again after a repair is quarantined.
func recordRepair(ctx context.Context, tx pgx.Tx, dr Drift) (bool, error) {
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
	_, err := tx.Exec(ctx, `
		INSERT INTO ledger_repairs (family, key, counter, stored, repaired) VALUES ($1, $2, $3, $4::NUMERIC, $5::NUMERIC)`,
		dr.Family, dr.Key, dr.Counter, dr.Stored, dr.Recount)
	return err == nil, err
}

func applyRunning(ctx context.Context, tx pgx.Tx, dr Drift) error {
	expected := dr.Recount
	if expected == nil {
		zero := "0"
		expected = &zero
	}
	column, ok := runningColumns[dr.Family][dr.Counter]
	if !ok {
		return fmt.Errorf("ledger: no running column for %s %s", dr.Family, dr.Counter)
	}
	t := runningTables[dr.Family]
	col := pgx.Identifier{column}.Sanitize()
	_, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET %s = COALESCE(%s, 0) + ($2::NUMERIC - COALESCE($3::NUMERIC, 0)) WHERE %s = $1`,
		t.table, col, col, t.key), dr.Key, *expected, dr.Stored)
	return err
}

// RunVerify recounts one retained span and checks the running counters of one address bucket per interval, with
// retention on or off.
func (d *DB) RunVerify(ctx context.Context, cfg VerifyConfig) {
	log.Info("ledger verification started", "interval", cfg.RecountInterval, "repair", cfg.Repair)
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
