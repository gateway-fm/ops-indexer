package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	// DropGateWait bounds how long a detach or a drop waits for the ledger closer's span; past it the range is left
	// for the next pass. Default 10 min.
	DropGateWait time.Duration
	// DetachWait bounds how long a detach or a drop waits for the readers of the range's tables; past it the range is
	// left for the next pass, a detach pending. Default 30 s.
	DetachWait time.Duration
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
	filled, err := d.fillHistoryGaps(ctx)
	if err != nil {
		return filled, err
	}
	made, err := d.EnsureHistoryPartitions(ctx, head+2*size, size)
	return filled + made, err
}

// repairHistoryCoverage fills the gaps in partition coverage above the watermark at the start, before ingest.
func (d *DB) repairHistoryCoverage(ctx context.Context) error {
	d.ensureMu.Lock()
	defer d.ensureMu.Unlock()
	_, err := d.fillHistoryGaps(ctx)
	return err
}

// fillHistoryGaps creates the partitions missing between the first retained block (history_from) and the highest
// partition, in ranges of the newest partition's size, each in its own transaction; the caller holds ensureMu.
// Below the watermark ranges are dropped on purpose, but above it every block needs a partition: a wipe that
// recreates the pruned ranges and stops part way leaves gaps, which the next start fills here once it has finished
// the wipe, and which the ensure would not, since it only extends above the highest partition.
//
// Retention prunes and drops ranges meanwhile without ensureMu. The watermark and the ranges are read in one
// snapshot: read apart, a range pruned and dropped in between looks like a gap above the old watermark. Each
// partition is then created under a share lock on the watermark's row, which a prune's advance waits for, and only
// if it still starts at or above the first retained block and overlaps no range.
func (d *DB) fillHistoryGaps(ctx context.Context) (int, error) {
	type gap struct{ lo, hi int64 }
	var gaps []gap
	var size *int64
	if err := pgx.BeginTxFunc(ctx, d.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var from int64
		if err := tx.QueryRow(ctx, `
			SELECT history_from(),
			       (SELECT hi - lo FROM history_partitions WHERE lo IS NOT NULL AND hi IS NOT NULL ORDER BY lo DESC LIMIT 1)`).
			Scan(&from, &size); err != nil {
			return err
		}
		if size == nil {
			return nil
		}
		rows, err := tx.Query(ctx, `SELECT lo, hi FROM history_partitions ORDER BY lo NULLS FIRST`)
		if err != nil {
			return err
		}
		defer rows.Close()
		covered := from
		for rows.Next() {
			var lo, hi *int64
			if err := rows.Scan(&lo, &hi); err != nil {
				return err
			}
			if hi == nil {
				gaps = nil
				return nil
			}
			if lo != nil && *lo > covered {
				gaps = append(gaps, gap{covered, *lo})
			}
			covered = max(covered, *hi)
		}
		return rows.Err()
	}); err != nil {
		return 0, err
	}
	if gapFillHook != nil {
		gapFillHook()
	}
	created := 0
	for _, g := range gaps {
		for lo := g.lo; lo < g.hi; {
			hi := min(lo+*size, g.hi)
			if lo == g.lo && (g.hi-g.lo)%*size != 0 {
				hi = lo + (g.hi-g.lo)%*size
			}
			ok, err := d.createInGap(ctx, lo, hi)
			if err != nil {
				return created, fmt.Errorf("create the missing history partition from %d: %w", lo, err)
			}
			if !ok {
				log.Info("retention: a gap changed while it was filled; the next ensure reads it again", "from", lo, "to", hi)
				return created, nil
			}
			created++
			lo = hi
		}
		log.Warn("retention: history partitions created in a gap above the watermark", "from", g.lo, "to", g.hi)
	}
	return created, nil
}

// createInGap creates the partition [lo, hi) if it still starts at or above the first retained block and overlaps
// no range, holding the watermark's row so no prune advances past it until the partition is in place.
func (d *DB) createInGap(ctx context.Context, lo, hi int64) (created bool, err error) {
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var from int64
		if err := tx.QueryRow(ctx, `
			SELECT CASE WHEN pruned_through > 0 THEN pruned_through + 1 ELSE 0 END FROM prune_state WHERE id = 1 FOR SHARE`).
			Scan(&from); err != nil {
			return err
		}
		var overlaps bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM history_partitions WHERE COALESCE(lo, $1) < $2 AND COALESCE(hi, $2) > $1)`, lo, hi).
			Scan(&overlaps); err != nil {
			return err
		}
		if lo < from || overlaps {
			return nil
		}
		if _, err := tx.Exec(ctx, `SELECT history_partition_create($1, $2)`, lo, hi); err != nil {
			return err
		}
		created = true
		return nil
	})
	return created, err
}

// gapFillHook, when set by a test, runs inside fillHistoryGaps between its snapshot and its creates.
var gapFillHook func()

// maxPartitionsPerEnsure bounds the partitions one ensure creates. After an outage the head can be hundreds of
// ranges past the newest partition; the rest are created by the next calls, a step apart.
const maxPartitionsPerEnsure = 32

// EnsureHistoryPartitions creates partitions of sizeBlocks towards upto, lowest first, at most
// maxPartitionsPerEnsure per call; a no-op before the cut. Each partition is created in its own transaction:
// one transaction creating them all takes locks on every new table and index, and enough of them exhaust the
// lock table (max_locks_per_transaction).
func (d *DB) EnsureHistoryPartitions(ctx context.Context, upto, sizeBlocks uint64) (int, error) {
	start := time.Now()
	made := 0
	for made < maxPartitionsPerEnsure {
		var top *int64
		if err := d.pool.QueryRow(ctx, `SELECT max(hi) FROM history_partitions`).Scan(&top); err != nil {
			return made, err
		}
		if top == nil || uint64(*top) > upto {
			break
		}
		var n int
		if err := d.pool.QueryRow(ctx, `SELECT history_partitions_ensure($1, $2)`, *top, int64(sizeBlocks)).Scan(&n); err != nil {
			return made, err
		}
		if n == 0 {
			break
		}
		made += n
	}
	if made > 0 {
		metrics.ObserveRetentionStep("ensure", time.Since(start))
	}
	return made, nil
}

// coverHistoryFromZero gives a wiped database partitions from block 0 again: the ranges past the old watermark
// are dropped without an archive (their rows are the wiped chain's), and fillHistoryGaps creates the ranges below
// the lowest one left. Stopped part way, the wipe is redone at the next start (ResumeInterruptedWipe), and any
// ensure fills the gaps meanwhile.
func (d *DB) coverHistoryFromZero(ctx context.Context) error {
	d.ensureMu.Lock()
	defer d.ensureMu.Unlock()
	rows, err := d.pool.Query(ctx, `
		SELECT suffix, state FROM history_partitions WHERE state IN ('detaching', 'detached') ORDER BY lo NULLS FIRST`)
	if err != nil {
		return err
	}
	type pruned struct{ suffix, state string }
	var ranges []pruned
	for rows.Next() {
		var p pruned
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
			if err := d.detachRange(ctx, p.suffix, 0); err != nil {
				return err
			}
		}
		if err := d.dropRange(ctx, p.suffix, 0); err != nil {
			return err
		}
	}

	_, err = d.fillHistoryGaps(ctx)
	return err
}

// RunRetention keeps partitions ahead of the head once the history is cut and, with a window, cuts it and drops
// whole ranges below the window. With RetainBlocks 0 on an uncut database it returns at once.
func (d *DB) RunRetention(ctx context.Context, cfg RetentionConfig) {
	var cut bool
	for {
		var err error
		if cut, err = d.HistoryCut(ctx); err == nil {
			break
		}
		log.Error("retention: read partition state failed; retrying", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(ensureInterval(cfg)):
		}
	}
	if !cut {
		if err := d.releaseLeftoverCut(ctx); err != nil {
			return
		}
	}
	if !cut && cfg.RetainBlocks == 0 {
		return
	}
	d.warnPartlyPrunedReorgDay(ctx)
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

// warnPartlyPrunedReorgDay warns when an earlier build pruned into a UTC day the reorg window still reaches: a reorg
// into that day cannot be reverted (daily_stats_invalidate refuses it), and pruning waits until the window has left
// the day.
func (d *DB) warnPartlyPrunedReorgDay(ctx context.Context) {
	var day *time.Time
	err := d.pool.QueryRow(ctx, `
		SELECT daily_stats_day(pruned_through_ts)::TIMESTAMP FROM prune_state
		WHERE id = 1 AND pruned_through > 0 AND pruned_through_ts >= prune_day_floor()`).Scan(&day)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && day == nil) {
		return
	}
	if err != nil {
		log.Warn("retention: could not check for a partly pruned day in the reorg window", "error", err)
		return
	}
	log.Warn("retention: an earlier build pruned part of a UTC day the reorg window reaches; a reorg into it cannot be reverted, and pruning resumes once the block floor_gap_blocks below the head is past day_ends",
		"day", day.Format(time.DateOnly), "day_ends", day.Add(24*time.Hour).Format(time.RFC3339),
		"floor_gap_blocks", PruneFloorGap)
}

// nodeHeadTimeout bounds a read of the node's head for retention. A node answers eth_blockNumber from memory in
// milliseconds, so five seconds is far past a healthy answer. The node client's own bound, RPC_TIMEOUT (60 s by
// default), is sized for block and receipt replies: a node that stopped answering would hold the cut, and with it the
// history-DDL gate the ledger closer waits on, for that long on every pass. On a timeout the stored head is used, as on any error: the cut then sits two partitions above it.
const nodeHeadTimeout = 5 * time.Second

// readNodeHead reads the node's head under nodeHeadTimeout; ok is false without a reader, or on an error, which it
// logs.
func readNodeHead(ctx context.Context, nodeHead func(context.Context) (uint64, error)) (head uint64, ok bool) {
	if nodeHead == nil {
		return 0, false
	}
	readCtx, cancel := context.WithTimeout(ctx, nodeHeadTimeout)
	defer cancel()
	head, err := nodeHead(readCtx)
	if err != nil {
		if ctx.Err() == nil {
			log.Warn("retention: the node's head was not read; using the stored head", "timeout", nodeHeadTimeout, "error", err)
		}
		return 0, false
	}
	return head, true
}

// ensureInterval is how often partitions are kept ahead, and how long a failed start waits before reading again.
func ensureInterval(cfg RetentionConfig) time.Duration {
	if cfg.EnsureInterval == 0 {
		return 10 * time.Second
	}
	return cfg.EnsureInterval
}

// keepPartitionsAhead keeps partitions ahead of the higher of the stored head and the node head: after a stall,
// realtime's jump writes at the node head before the stored head gets there.
func (d *DB) keepPartitionsAhead(ctx context.Context, cfg RetentionConfig) {
	ticker := time.NewTicker(ensureInterval(cfg))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		head, err := d.GetLatestBlockNumber(ctx)
		if err == nil {
			if node, ok := readNodeHead(ctx, cfg.NodeHead); ok && node > head {
				head = node
			}
		}
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
		if err := d.releaseLeftoverCut(ctx); err != nil {
			return err
		}
		if err := d.cutHistory(ctx, cfg); err != nil {
			if rerr := d.releaseLeftoverCut(ctx); rerr != nil {
				return fmt.Errorf("cut: %w; release its check: %v", err, rerr)
			}
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
	if finished, err := d.finishDetached(ctx, cfg); err != nil || !finished {
		return err
	}
	for {
		pruned, err := d.pruneOldest(ctx, cfg)
		if err != nil || !pruned {
			return err
		}
		if finished, err := d.finishDetached(ctx, cfg); err != nil || !finished {
			return err
		}
	}
}

// cutHistory bounds the legacy partitions at C = head + 2 partitions and creates the first partition above it.
// From the CHECK until the re-attach a legacy row at or above C is refused, so C keeps that much room ahead of
// ingest; if the head comes within one partition of C while the checks validate, the cut is abandoned and retried.
// The gate is held for the whole cut and taken before the check exists: waiting for a ledger span with the check in
// force would let the head reach the cut, where every insert into the legacy partition is refused. A transaction
// that holds a history table off the finish is waited out while the head stays more than a partition below C; past
// that the cut is abandoned, and retentionPass drops its check, waiting for that transaction as long as it lasts.
func (d *DB) cutHistory(ctx context.Context, cfg RetentionConfig) (err error) {
	node, nodeKnown := readNodeHead(ctx, cfg.NodeHead)
	release, err := d.holdHistoryDDLGate(ctx, cutGateWait)
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
	if nodeKnown && node > base {
		base = node
	}
	cutAt := base + 2*cfg.PartitionBlocks + d.cutMargin
	log.Info("retention: cutting the legacy partitions", "head", head, "node_head", base, "cut_at", cutAt, "extra_margin", d.cutMargin)
	if deps, err := d.rangeDependents(ctx, "legacy"); err != nil {
		return err
	} else if len(deps) > 0 {
		log.Warn("retention: objects outside the indexer read the legacy partitions, not the partitioned tables: from the cut "+
			"on they see no new block, and once the legacy range is pruned they keep it on disk; re-create them on the partitioned tables",
			"cut_at", cutAt, "dependents", strings.Join(deps, "; "))
	}

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
			if rerr := d.dropCutChecks(ctx, func(attempt int) (bool, error) { return attempt <= 20, nil }); rerr != nil {
				log.Warn("retention: an abandoned cut's check stays for now; retention keeps dropping it", "error", rerr)
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
	headNearCut := false
	if err := d.withLockTimeoutWhile(ctx, func(attempt int) (bool, error) {
		if attempt == 1 {
			return true, nil
		}
		var herr error
		if head, herr = d.GetLatestBlockNumber(ctx); herr != nil {
			return false, herr
		}
		headNearCut = head+cfg.PartitionBlocks >= cutAt
		return !headNearCut, nil
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `SELECT history_cut_finish($1, $2)`, int64(cutAt), int64(cfg.PartitionBlocks))
		return err
	}); err != nil {
		if headNearCut {
			d.cutMargin = nextCutMargin(d.cutMargin, head-headAtStart)
			return fmt.Errorf("head %d came within a partition of the cut %d before it could finish; retrying with %d more blocks of margin: %w",
				head, cutAt, d.cutMargin, err)
		}
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
// head reaches it every insert into the legacy partition is refused. After the cut the check stays. It tries once:
// while a transaction holds a history table the check stays, and retention, which starts after it, drops it once
// that transaction ends.
func (d *DB) ReleaseAbandonedCut(ctx context.Context) error {
	release, err := d.holdHistoryDDLGate(ctx, cutGateWait)
	if err != nil {
		return err
	}
	defer release()
	if err := d.dropCutChecks(ctx, func(attempt int) (bool, error) { return attempt == 1, nil }); err != nil && ctx.Err() == nil {
		log.Warn("retention: an abandoned cut's check stays for now; retention drops it once nothing holds the history tables",
			"error", err)
	}
	return ctx.Err()
}

// releaseLeftoverCut drops an abandoned cut's check, waiting for as long as a transaction holds a history table; it
// takes the history-DDL gate for each round of attempts, so the ledger closer runs in between.
func (d *DB) releaseLeftoverCut(ctx context.Context) error {
	for {
		release, err := d.holdHistoryDDLGate(ctx, cutGateWait)
		if err == nil {
			err = d.dropCutChecks(ctx, func(attempt int) (bool, error) { return attempt <= 20, nil })
			release()
		}
		if err == nil || ctx.Err() != nil {
			return err
		}
		log.Error("retention: an abandoned cut's check cannot be dropped yet; inserts into the legacy partitions at or above it fail until it is",
			"error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// dropCutChecks is ReleaseAbandonedCut for a caller that already holds the history-DDL gate; more decides, before
// each attempt, whether to make it.
func (d *DB) dropCutChecks(ctx context.Context, more func(attempt int) (bool, error)) error {
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
		if err := d.withLockTimeoutWhile(ctx, more, func(tx pgx.Tx) error {
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

const cutGateWait = 30 * time.Minute

// errHistoryDDLGateBusy: the closer's span outlasted the wait.
var errHistoryDDLGateBusy = errors.New("the ledger closer held the history-DDL gate past the wait")

// holdHistoryDDLGate takes the history-DDL gate exclusively on its own connection, waiting up to wait for the span
// in progress; a waiting exclusive request keeps the closer from starting another. Queued behind a span read
// instead, a DDL lock request would hold up ingest's own statements on the history tables. Ingest never takes the
// gate. The returned func releases it.
//
// The connection is opened beside the pool, not taken from it: the statements under the gate run on the pool, and
// with the gate holding a pool connection a pool of one would wait for a second connection for good.
func (d *DB) holdHistoryDDLGate(ctx context.Context, wait time.Duration) (func(), error) {
	conn, err := pgx.ConnectConfig(ctx, d.pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("open the history-DDL gate's connection: %w", err)
	}
	closeConn := func() { _ = conn.Close(context.WithoutCancel(ctx)) }
	start := time.Now()
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SET lock_timeout = '%dms'`, wait.Milliseconds())); err != nil {
		closeConn()
		return nil, err
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1, 1)`, historyDDLGate); err != nil {
		closeConn()
		if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			return nil, fmt.Errorf("%w (%s)", errHistoryDDLGateBusy, wait)
		}
		return nil, fmt.Errorf("wait for the ledger closer: %w", err)
	}
	if waited := time.Since(start); waited > time.Second {
		log.Info("retention: waited for the ledger closer before history DDL", "waited", waited)
	}
	return func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1, 1)`, historyDDLGate)
		closeConn()
	}, nil
}

// errHistoryHeld: a transaction open for more than a second holds a history table, so DDL was not attempted.
var errHistoryHeld = errors.New("a transaction holds a history table")

// withLockTimeout runs fn in a transaction that gives up on a lock after 2 s, retrying up to 20 times: a DDL lock
// waiting in the queue blocks every ingest statement behind it.
func (d *DB) withLockTimeout(ctx context.Context, fn func(pgx.Tx) error) error {
	return d.withLockTimeoutWhile(ctx, func(attempt int) (bool, error) { return attempt <= 20, nil }, fn)
}

// withLockTimeoutWhile is withLockTimeout retrying for as long as more, asked before each attempt, reports true; it
// returns the last attempt's error once more reports false.
func (d *DB) withLockTimeoutWhile(ctx context.Context, more func(attempt int) (bool, error), fn func(pgx.Tx) error) error {
	var err error
	for attempt := 1; ; attempt++ {
		if ok, merr := more(attempt); merr != nil {
			return merr
		} else if !ok {
			return err
		}
		if holder, held, herr := d.longHistoryHolder(ctx); herr != nil {
			err = herr
		} else if held {
			// Requesting ACCESS EXCLUSIVE now would queue every insert behind the request until it times out.
			err = fmt.Errorf("%w: open for %s (pid %d, application %q)", errHistoryHeld, holder.age, holder.pid, holder.app)
		} else {
			err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '2s'`); err != nil {
					return err
				}
				return fn(tx)
			})
		}
		if err == nil || ctx.Err() != nil {
			return err
		}
		if attempt <= 20 || attempt%60 == 0 {
			log.Warn("retention: DDL attempt failed; retrying", "attempt", attempt, "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

type historyHolder struct {
	pid int
	app string
	age time.Duration
}

// longHistoryHolder finds another backend whose transaction, open for more than a second, holds a lock on a history
// table or one of its partitions: a reader the cut's DDL would wait for while ingest queues behind the DDL.
// Ingest's own transactions last milliseconds and do not count, nor does an autovacuum, which the DDL's lock request
// cancels, unless it is preventing wraparound.
func (d *DB) longHistoryHolder(ctx context.Context) (historyHolder, bool, error) {
	var h historyHolder
	var ageMS int64
	err := d.pool.QueryRow(ctx, `
		SELECT a.pid, COALESCE(a.application_name, ''), (extract(epoch FROM now() - a.xact_start) * 1000)::BIGINT
		FROM pg_locks l
		JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.granted AND l.pid <> pg_backend_pid() AND a.xact_start < now() - interval '1 second'
		  AND (a.backend_type <> 'autovacuum worker' OR a.query LIKE '%(to prevent wraparound)%')
		  AND l.relation IN (
		      SELECT c.oid FROM pg_class c WHERE c.relname = ANY(history_tables())
		      UNION SELECT i.inhrelid FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
		            WHERE p.relname = ANY(history_tables()))
		ORDER BY a.xact_start LIMIT 1`).Scan(&h.pid, &h.app, &ageMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return h, false, nil
	}
	if err != nil {
		return h, false, err
	}
	h.age = time.Duration(ageMS) * time.Millisecond
	return h, true, nil
}

// pruneOldest moves the watermark past the oldest attached range if it is entirely below the window, then
// detaches it; reports whether it did.
func (d *DB) pruneOldest(ctx context.Context, cfg RetentionConfig) (bool, error) {
	var suffix string
	var hi *int64
	var dsw *int64
	var through int64
	err := d.pool.QueryRow(ctx, `
		SELECT suffix, hi, (SELECT last_block FROM daily_stats_watermark WHERE id = 1), pruned_through()
		FROM history_partitions WHERE state = 'attached' ORDER BY lo NULLS FIRST LIMIT 1`).Scan(&suffix, &hi, &dsw, &through)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// A range wholly at or below the watermark is pruned history recreated (by an earlier build's gap fill racing a
	// prune). It ends where no ledger span does, so it would be refused below at every pass and stop retention:
	// it goes the way of a pruned range.
	if hi != nil && through > 0 && *hi-1 <= through {
		if _, err := d.pool.Exec(ctx, `UPDATE history_partitions SET state = 'detaching' WHERE suffix = $1 AND state = 'attached'`, suffix); err != nil {
			return false, err
		}
		log.Warn("retention: a range below the watermark was attached; dropping it", "range", suffix, "below", *hi, "pruned_through", through)
		return true, nil
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
	var beforeReorgDay bool
	if err := d.pool.QueryRow(ctx, `SELECT history_range_before_reorg_day($1)`, suffix).Scan(&beforeReorgDay); err != nil {
		return false, err
	}
	if !beforeReorgDay {
		log.Debug("retention: oldest range reaches the UTC day a reorg can revert into; not pruned", "range", suffix, "below", *hi)
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
// archiving is on, and drops it once nothing more is owed. The detach and the drop each run under the history-DDL
// gate; when the closer holds it past DropGateWait the range is left as it is and finished reports false.
func (d *DB) finishDetached(ctx context.Context, cfg RetentionConfig) (finished bool, err error) {
	rows, err := d.pool.Query(ctx, `
		SELECT suffix, state FROM history_partitions WHERE state IN ('detaching', 'detached') ORDER BY lo NULLS FIRST`)
	if err != nil {
		return false, err
	}
	type pending struct{ suffix, state string }
	var ranges []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.suffix, &p.state); err != nil {
			rows.Close()
			return false, err
		}
		ranges = append(ranges, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	wait := cfg.DropGateWait
	if wait == 0 {
		wait = 10 * time.Minute
	}
	readersWait := cfg.DetachWait
	if readersWait == 0 {
		readersWait = 30 * time.Second
	}
	underGate := func(step, suffix string, fn func() error) (bool, error) {
		release, err := d.holdHistoryDDLGate(ctx, wait)
		if errors.Is(err, errHistoryDDLGateBusy) {
			log.Info("retention: the ledger closer kept the history-DDL gate; retrying next pass", "step", step, "range", suffix, "wait", wait)
			return false, nil
		}
		if err != nil {
			return false, err
		}
		defer release()
		if err := fn(); errors.Is(err, errRangeReadersBusy) {
			log.Info("retention: readers of the range outlasted the wait; retrying next pass", "step", step, "range", suffix,
				"wait", readersWait, "error", err)
			return false, nil
		} else if err != nil {
			return false, err
		}
		return true, nil
	}
	for _, p := range ranges {
		if p.state == "detaching" {
			if ok, err := underGate("detach", p.suffix, func() error { return d.detachRange(ctx, p.suffix, readersWait) }); err != nil || !ok {
				return false, err
			}
		}
		if cfg.ArchiveDir != "" {
			if err := d.archiveRange(ctx, cfg.ArchiveDir, p.suffix); err != nil {
				return false, fmt.Errorf("archive %s: %w", p.suffix, err)
			}
		}
		ok, err := underGate("drop", p.suffix, func() error { return d.dropRange(ctx, p.suffix, readersWait) })
		if errors.Is(err, errRangeHasDependents) {
			// Detached, the range holds nothing ingest or the API reads: the ranges after it go on.
			log.Error("retention: a detached range stays on disk because objects outside the indexer depend on its tables; "+
				"drop them, or re-create them on the partitioned tables, and the next pass drops it", "range", p.suffix, "error", err)
			continue
		}
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// errRangeHasDependents: objects outside the indexer depend on a range's tables, so they cannot be dropped.
var errRangeHasDependents = errors.New("objects outside the indexer depend on the range's tables")

// historyDependents lists the objects that Postgres would refuse to drop table for: views that read it and foreign
// keys that reference it, which consumers made on the history tables before they were partitioned, and any other.
// The table's own constraints, such as the bound DETACH CONCURRENTLY adds, are not among them.
func (d *DB) historyDependents(ctx context.Context, table string) ([]string, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT DISTINCT COALESCE(
		           (SELECT 'view ' || r.ev_class::regclass::text FROM pg_rewrite r
		            WHERE d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid),
		           pg_describe_object(d.classid, d.objid, d.objsubid))
		FROM pg_depend d
		WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = to_regclass($1) AND d.deptype = 'n'
		  AND NOT EXISTS (SELECT 1 FROM pg_constraint c
		                  WHERE d.classid = 'pg_constraint'::regclass AND c.oid = d.objid AND c.conrelid = d.refobjid)
		ORDER BY 1`, table)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// rangeDependents is historyDependents for every table of a range, each prefixed with its table.
func (d *DB) rangeDependents(ctx context.Context, suffix string) ([]string, error) {
	var all []string
	for _, t := range HistoryTables {
		deps, err := d.historyDependents(ctx, t+"_"+suffix)
		if err != nil {
			return nil, err
		}
		for _, dep := range deps {
			all = append(all, t+"_"+suffix+": "+dep)
		}
	}
	return all, nil
}

// errRangeReadersBusy: a detach or a drop waited for the readers of a range's tables past its wait.
var errRangeReadersBusy = errors.New("readers of the range held it past the wait")

// execWithin runs one statement on its own connection under a lock and a statement timeout of wait, so it never
// waits on a reader for longer. A statement it cancels reports errRangeReadersBusy.
func (d *DB) execWithin(ctx context.Context, wait time.Duration, sql string) error {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `RESET statement_timeout; RESET lock_timeout`)
	}()
	ms := wait.Milliseconds()
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SET statement_timeout = '%dms'; SET lock_timeout = '%dms'`, ms, ms)); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, sql)
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && (pgErr.Code == "57014" || pgErr.Code == "55P03") && ctx.Err() == nil {
		return fmt.Errorf("%w (%s): %v", errRangeReadersBusy, wait, err)
	}
	return err
}

// detachRange detaches the range's partition of every history table with DETACH CONCURRENTLY (SHARE UPDATE
// EXCLUSIVE on the parent: ingest keeps running), finishing a detach a crash or an earlier wait left pending. Each
// detach waits at most wait for the transactions that still read the partition; one that outlasts it stays pending.
func (d *DB) detachRange(ctx context.Context, suffix string, wait time.Duration) error {
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
		if err := d.execWithin(ctx, wait, fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s %s`,
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

// dropRange drops a detached range's tables, each waiting at most wait for a transaction that still reads it. With
// archiving on, archiveRange has recorded each one first.
func (d *DB) dropRange(ctx context.Context, suffix string, wait time.Duration) error {
	start := time.Now()
	if deps, err := d.rangeDependents(ctx, suffix); err != nil {
		return err
	} else if len(deps) > 0 {
		return fmt.Errorf("%w: %s", errRangeHasDependents, strings.Join(deps, "; "))
	}
	for _, t := range HistoryTables {
		if err := d.execWithin(ctx, wait, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, pgx.Identifier{t + "_" + suffix}.Sanitize())); err != nil {
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
