package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

// ErrDayPruned refuses to recompute a day whose first blocks were pruned: the result would be partial.
var ErrDayPruned = errors.New("day is below the prune watermark")

// PruneFloorGap mirrors prune_floor_gap() (migration 014): the minimum distance between the stored head and the
// prune watermark, 100x the deepest reorg the indexer reverts.
const PruneFloorGap = 12800

// HistoryTables mirrors history_tables() (migration 014).
var HistoryTables = []string{"blocks", "transactions", "logs", "token_transfers", "internal_transactions", "balances"}

type RetentionConfig struct {
	// RetainBlocks is the window kept below the stored head; 0 keeps everything.
	RetainBlocks uint64
	// PartitionBlocks is the size of each range partition once the legacy partition is cut.
	PartitionBlocks uint64
	Interval        time.Duration
	// EnsureInterval is how often partitions are kept ahead of the head, apart from Interval: at the catch-up rate
	// 2 partitions last minutes, and a pass can be held for the length of a cut. Default 10 s.
	EnsureInterval time.Duration
	// ArchiveDir, when set, receives every detached partition before it is dropped.
	ArchiveDir string
	// NodeHead, when set, is the chain head: the cut is placed above it, not only above the stored head, so a cut
	// started while ingest is behind (a restart) keeps its full margin.
	NodeHead func(ctx context.Context) (uint64, error)
}

// PrunedThrough returns the prune watermark (every block at or below it is gone on purpose) and the timestamp of
// the newest pruned block; both are 0 on a database that was never pruned.
func (d *DB) PrunedThrough(ctx context.Context) (uint64, int64, error) {
	var through, ts int64
	err := d.pool.QueryRow(ctx, `
		SELECT COALESCE(max(pruned_through), 0), COALESCE(max(pruned_through_ts), 0) FROM prune_state`).Scan(&through, &ts)
	return uint64(through), ts, err
}

// HistoryCut reports whether the legacy partition is bounded, i.e. retention has been enabled at least once and
// fixed-size partitions must be kept ahead of the head from now on, window or not.
func (d *DB) HistoryCut(ctx context.Context) (bool, error) {
	var bounded bool
	err := d.pool.QueryRow(ctx, `
		SELECT NOT EXISTS (SELECT 1 FROM history_partitions WHERE suffix = 'legacy' AND hi IS NULL)`).Scan(&bounded)
	return bounded, err
}

// EnsurePartitionsAhead keeps two partitions ahead of the head once the history is cut. With no partition size
// configured (retention switched off after a cut: PARTITION_BLOCKS defaults to PRUNE_RETAIN_BLOCKS / 10 = 0) it
// uses the newest partition's size: ensuring only up to the head itself would leave the next block no partition
// to go into, and ingest would stop there for good.
func (d *DB) EnsurePartitionsAhead(ctx context.Context, head, partitionBlocks uint64) (int, error) {
	d.ensureMu.Lock()
	defer d.ensureMu.Unlock()
	size := partitionBlocks
	if size == 0 {
		var newest *int64
		if err := d.pool.QueryRow(ctx, `
			SELECT hi - lo FROM history_partitions WHERE lo IS NOT NULL AND hi IS NOT NULL ORDER BY lo DESC LIMIT 1`).Scan(&newest); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
		if newest == nil {
			return 0, nil
		}
		size = uint64(*newest)
	}
	return d.EnsureHistoryPartitions(ctx, head+2*size, size)
}

// EnsureHistoryPartitions creates partitions of sizeBlocks up to at least upto; a no-op before the cut.
func (d *DB) EnsureHistoryPartitions(ctx context.Context, upto, sizeBlocks uint64) (int, error) {
	start := time.Now()
	var made int
	err := d.pool.QueryRow(ctx, `SELECT history_partitions_ensure($1, $2)`, int64(upto), int64(sizeBlocks)).Scan(&made)
	if err == nil && made > 0 {
		metrics.ObserveRetentionStep("ensure", time.Since(start))
	}
	return made, err
}

// RunRetention keeps partitions ahead of the head once the history is cut and, with a window, cuts it and drops
// whole ranges below the window. With RetainBlocks 0 on an uncut database it returns at once.
func (d *DB) RunRetention(ctx context.Context, cfg RetentionConfig) {
	cut, err := d.HistoryCut(ctx)
	if err != nil {
		log.Error("retention: read partition state failed", "error", err)
		return
	}
	if !cut && cfg.RetainBlocks == 0 {
		return
	}
	log.Info("retention started", "retain_blocks", cfg.RetainBlocks, "partition_blocks", cfg.PartitionBlocks,
		"interval", cfg.Interval, "archive_dir", cfg.ArchiveDir)
	go d.keepPartitionsAhead(ctx, cfg)
	for {
		if err := d.retentionPass(ctx, cfg); err != nil && ctx.Err() == nil {
			log.Warn("retention pass failed; retrying next interval", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(cfg.Interval):
		}
	}
}

func (d *DB) keepPartitionsAhead(ctx context.Context, cfg RetentionConfig) {
	interval := cfg.EnsureInterval
	if interval == 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		head, err := d.GetLatestBlockNumber(ctx)
		if err == nil {
			var made int
			if made, err = d.EnsurePartitionsAhead(ctx, head, cfg.PartitionBlocks); made > 0 {
				log.Info("retention: history partitions created", "count", made, "head", head)
			}
		}
		if err != nil && ctx.Err() == nil {
			log.Warn("retention: keeping partitions ahead failed; retrying", "error", err)
		}
	}
}

func (d *DB) retentionPass(ctx context.Context, cfg RetentionConfig) error {
	cut, err := d.HistoryCut(ctx)
	if err != nil {
		return err
	}
	if !cut {
		if err := d.cutHistory(ctx, cfg); err != nil {
			return fmt.Errorf("cut: %w", err)
		}
	}
	head, err := d.GetLatestBlockNumber(ctx)
	if err != nil {
		return err
	}
	if made, err := d.EnsurePartitionsAhead(ctx, head, cfg.PartitionBlocks); err != nil {
		return fmt.Errorf("ensure partitions: %w", err)
	} else if made > 0 {
		log.Info("retention: history partitions created", "count", made, "head", head)
	}
	if cfg.RetainBlocks == 0 {
		return nil
	}
	if err := d.finishDetached(ctx, cfg); err != nil {
		return err
	}
	for {
		pruned, err := d.pruneOldest(ctx, cfg)
		if err != nil || !pruned {
			return err
		}
		if err := d.finishDetached(ctx, cfg); err != nil {
			return err
		}
	}
}

// cutHistory bounds the legacy partitions at C = head + 2 partitions and creates the first partition above it.
// From the CHECK until the re-attach a legacy row at or above C is refused, so C keeps that much room ahead of
// ingest; if the head comes within one partition of C while the checks validate, the cut is abandoned and retried.
// The gate is held for the whole cut and taken before the check exists: waiting for a ledger span with the check in
// force would let the head reach the cut, where every insert into the legacy partition is refused.
func (d *DB) cutHistory(ctx context.Context, cfg RetentionConfig) (err error) {
	release, err := d.holdHistoryDDLGate(ctx)
	if err != nil {
		return err
	}
	defer release()
	head, err := d.GetLatestBlockNumber(ctx)
	if err != nil {
		return err
	}
	headAtStart := head
	base := head
	if cfg.NodeHead != nil {
		if node, err := cfg.NodeHead(ctx); err == nil && node > base {
			base = node
		}
	}
	cutAt := base + 2*cfg.PartitionBlocks + d.cutMargin
	log.Info("retention: cutting the legacy partitions", "head", head, "node_head", base, "cut_at", cutAt, "extra_margin", d.cutMargin)

	start := time.Now()
	if err := d.withLockTimeout(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `SELECT history_cut_constrain($1)`, int64(cutAt))
		return err
	}); err != nil {
		return err
	}
	metrics.ObserveRetentionStep("cut_constrain", time.Since(start))
	defer func() {
		if err != nil {
			if rerr := d.dropCutChecks(context.WithoutCancel(ctx)); rerr != nil {
				log.Error("retention: dropping an abandoned cut's check failed; it is dropped again at the next start", "error", rerr)
			}
		}
	}()

	start = time.Now()
	for _, t := range HistoryTables {
		if _, err := d.pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s VALIDATE CONSTRAINT history_cut`, pgx.Identifier{t + "_legacy"}.Sanitize())); err != nil {
			return fmt.Errorf("validate %s_legacy: %w", t, err)
		}
		if head, err = d.GetLatestBlockNumber(ctx); err != nil {
			return err
		}
		if head+cfg.PartitionBlocks >= cutAt {
			d.cutMargin = nextCutMargin(d.cutMargin, head-headAtStart)
			return fmt.Errorf("head %d came within a partition of the cut %d while validating; retrying with %d more blocks of margin",
				head, cutAt, d.cutMargin)
		}
	}
	metrics.ObserveRetentionStep("cut_validate", time.Since(start))
	log.Info("retention: legacy partitions validated below the cut", "cut_at", cutAt, "elapsed", time.Since(start))

	start = time.Now()
	if err := d.withLockTimeout(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `SELECT history_cut_finish($1, $2)`, int64(cutAt), int64(cfg.PartitionBlocks))
		return err
	}); err != nil {
		return err
	}
	metrics.ObserveRetentionStep("cut_finish", time.Since(start))
	log.Info("retention: history cut", "cut_at", cutAt, "finish", time.Since(start))
	return nil
}

// nextCutMargin widens the cut's extra margin after the head advanced by advanced blocks during one attempt's
// validation: at least twice that advance, and at least twice the previous margin, so it outgrows validation time
// even while a catch-up runs.
func nextCutMargin(prev, advanced uint64) uint64 {
	return max(2*prev, 2*advanced)
}

// ReleaseAbandonedCut drops the cut's check from the legacy partitions while the history is not cut: a cut that was
// abandoned (the head came near it, a failure, a restart) leaves a validated "below the cut" check, and once the
// head reaches it every insert into the legacy partition is refused. After the cut the check stays.
func (d *DB) ReleaseAbandonedCut(ctx context.Context) error {
	release, err := d.holdHistoryDDLGate(ctx)
	if err != nil {
		return err
	}
	defer release()
	return d.dropCutChecks(ctx)
}

// dropCutChecks is ReleaseAbandonedCut for a caller that already holds the history-DDL gate.
func (d *DB) dropCutChecks(ctx context.Context) error {
	cut, err := d.HistoryCut(ctx)
	if err != nil || cut {
		return err
	}
	for _, t := range HistoryTables {
		var exists bool
		if err := d.pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass($1) AND conname = 'history_cut')`,
			t+"_legacy").Scan(&exists); err != nil {
			return err
		}
		if !exists {
			continue
		}
		if err := d.withLockTimeout(ctx, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT IF EXISTS history_cut`, pgx.Identifier{t + "_legacy"}.Sanitize()))
			return err
		}); err != nil {
			return fmt.Errorf("drop the abandoned cut on %s_legacy: %w", t, err)
		}
		log.Warn("retention: dropped an abandoned cut's check", "table", t+"_legacy")
	}
	return nil
}

// historyDDLGate is the advisory key that keeps retention's DDL and the ledger closer apart: the closer holds it
// shared while it closes a span, retention exclusively while it changes the history tables.
const historyDDLGate = 4673

// holdHistoryDDLGate takes the history-DDL gate exclusively on its own connection, waiting up to 30 min for the
// span in progress; a waiting exclusive request keeps the closer from starting another. Queued behind a span read
// instead, a DDL lock request would hold up ingest's own statements on the history tables. Ingest never takes the
// gate. The returned func releases it.
func (d *DB) holdHistoryDDLGate(ctx context.Context) (func(), error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	reset := func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `RESET lock_timeout`)
		conn.Release()
	}
	start := time.Now()
	if _, err := conn.Exec(ctx, `SET lock_timeout = '30min'`); err != nil {
		reset()
		return nil, err
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1, 1)`, historyDDLGate); err != nil {
		reset()
		return nil, fmt.Errorf("wait for the ledger closer: %w", err)
	}
	if waited := time.Since(start); waited > time.Second {
		log.Info("retention: waited for the ledger closer before history DDL", "waited", waited)
	}
	return func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1, 1)`, historyDDLGate)
		reset()
	}, nil
}

// withLockTimeout runs fn in a transaction that gives up on a lock after 2 s, retrying up to 20 times: a DDL lock
// waiting in the queue blocks every ingest statement behind it.
func (d *DB) withLockTimeout(ctx context.Context, fn func(pgx.Tx) error) error {
	var err error
	for attempt := 1; attempt <= 20; attempt++ {
		err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '2s'`); err != nil {
				return err
			}
			return fn(tx)
		})
		if err == nil || ctx.Err() != nil {
			return err
		}
		log.Warn("retention: DDL attempt failed; retrying", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return err
}

// pruneOldest moves the watermark past the oldest attached range if it is entirely below the window, then
// detaches it; reports whether it did.
func (d *DB) pruneOldest(ctx context.Context, cfg RetentionConfig) (bool, error) {
	var suffix string
	var hi *int64
	var dsw *int64
	err := d.pool.QueryRow(ctx, `
		SELECT suffix, hi, (SELECT last_block FROM daily_stats_watermark WHERE id = 1)
		FROM history_partitions WHERE state = 'attached' ORDER BY lo NULLS FIRST LIMIT 1`).Scan(&suffix, &hi, &dsw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	head, err := d.GetLatestBlockNumber(ctx)
	if err != nil {
		return false, err
	}
	if hi == nil || dsw == nil || head < cfg.RetainBlocks {
		return false, nil
	}
	last := uint64(*hi - 1)
	if last > head-cfg.RetainBlocks || last+PruneFloorGap > head || *hi-1 > *dsw {
		return false, nil
	}
	var closed bool
	if err := d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ledger_spans WHERE hi = $1)`, *hi).Scan(&closed); err != nil {
		return false, err
	}
	if !closed {
		log.Info("retention: oldest range is not closed in the ledger yet; not pruned", "range", suffix, "below", *hi)
		return false, nil
	}
	var complete bool
	if err := d.pool.QueryRow(ctx, `SELECT history_range_complete($1)`, suffix).Scan(&complete); err != nil {
		return false, err
	}
	if !complete {
		log.Info("retention: oldest range is missing blocks; not pruned", "range", suffix, "below", *hi)
		return false, nil
	}

	start := time.Now()
	var wm int64
	if err := d.pool.QueryRow(ctx, `SELECT prune_advance($1)`, suffix).Scan(&wm); err != nil {
		return false, fmt.Errorf("advance %s: %w", suffix, err)
	}
	metrics.ObserveRetentionStep("advance", time.Since(start))
	metrics.SetPruneWatermark(uint64(wm))
	log.Info("retention: watermark moved", "range", suffix, "pruned_through", wm)
	return true, nil
}

// finishDetached completes every range past the watermark: detaches what is still attached, archives it if
// archiving is on, and drops it once nothing more is owed.
func (d *DB) finishDetached(ctx context.Context, cfg RetentionConfig) error {
	rows, err := d.pool.Query(ctx, `
		SELECT suffix, state FROM history_partitions WHERE state IN ('detaching', 'detached') ORDER BY lo NULLS FIRST`)
	if err != nil {
		return err
	}
	type pending struct{ suffix, state string }
	var ranges []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.suffix, &p.state); err != nil {
			rows.Close()
			return err
		}
		ranges = append(ranges, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, p := range ranges {
		if p.state == "detaching" {
			if err := d.detachRange(ctx, p.suffix); err != nil {
				return err
			}
		}
		if cfg.ArchiveDir != "" {
			if err := d.archiveRange(ctx, cfg.ArchiveDir, p.suffix); err != nil {
				return fmt.Errorf("archive %s: %w", p.suffix, err)
			}
		}
		if err := d.dropRange(ctx, p.suffix); err != nil {
			return err
		}
	}
	return nil
}

// detachRange detaches the range's partition of every history table with DETACH CONCURRENTLY (SHARE UPDATE
// EXCLUSIVE on the parent: ingest keeps running), finishing a detach a crash left pending.
func (d *DB) detachRange(ctx context.Context, suffix string) error {
	start := time.Now()
	for _, t := range HistoryTables {
		part := t + "_" + suffix
		var attached, pending bool
		err := d.pool.QueryRow(ctx, `
			SELECT true, i.inhdetachpending FROM pg_inherits i
			WHERE i.inhrelid = to_regclass($1) AND i.inhparent = to_regclass($2)`, part, t).Scan(&attached, &pending)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		mode := "CONCURRENTLY"
		if pending {
			mode = "FINALIZE"
		}
		if _, err := d.pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s %s`,
			pgx.Identifier{t}.Sanitize(), pgx.Identifier{part}.Sanitize(), mode)); err != nil {
			return fmt.Errorf("detach %s: %w", part, err)
		}
	}
	if _, err := d.pool.Exec(ctx, `
		UPDATE history_partitions SET state = 'detached', detached_at = now() WHERE suffix = $1`, suffix); err != nil {
		return err
	}
	metrics.ObserveRetentionStep("detach", time.Since(start))
	log.Info("retention: range detached", "range", suffix, "elapsed", time.Since(start))
	return nil
}

// dropRange drops a detached range's tables. With archiving on, archiveRange has recorded each one first.
func (d *DB) dropRange(ctx context.Context, suffix string) error {
	start := time.Now()
	for _, t := range HistoryTables {
		if _, err := d.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, pgx.Identifier{t + "_" + suffix}.Sanitize())); err != nil {
			return fmt.Errorf("drop %s_%s: %w", t, suffix, err)
		}
	}
	if _, err := d.pool.Exec(ctx, `DELETE FROM history_partitions WHERE suffix = $1`, suffix); err != nil {
		return err
	}
	metrics.ObserveRetentionStep("drop", time.Since(start))
	log.Info("retention: range dropped", "range", suffix, "elapsed", time.Since(start))
	return nil
}
