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

type LedgerConfig struct {
	SpanBlocks uint64
	FirstBlock uint64
	Interval   time.Duration
	// WorkMem is the closing transaction's work_mem, so a span's grouping does not spill ("" keeps the server's).
	WorkMem string
	// HeadLag reports how many blocks ingest is behind the node; while it exceeds MaxHeadLag the closer waits.
	HeadLag    func(ctx context.Context) (uint64, error)
	MaxHeadLag uint64
	// GateWait bounds a span close's wait for the history-DDL gate (0: defaultLedgerGateWait).
	GateWait time.Duration
}

// defaultLedgerGateWait: retention holds the history-DDL gate for its DDL, a cut's validation the longest. Past a
// minute the close gives up and the closer logs it and retries next interval, so a gate held for good shows in the
// log at least once a minute instead of stopping the closer silently.
const defaultLedgerGateWait = time.Minute

var errLedgerGateBusy = errors.New("the history-DDL gate is held by retention")

// CloseLedgerSpans closes up to max spans after the closed prefix, stopping at the first that is not final and
// complete; it returns how many it closed.
func (d *DB) CloseLedgerSpans(ctx context.Context, cfg LedgerConfig, max int) (int, error) {
	closed := 0
	for closed < max {
		var lo, hi int64
		if err := d.pool.QueryRow(ctx, `SELECT lo, hi FROM ledger_next_span($1, $2)`, int64(cfg.SpanBlocks), int64(cfg.FirstBlock)).Scan(&lo, &hi); err != nil {
			return closed, err
		}
		start := time.Now()
		var ok bool
		if err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
			if cfg.WorkMem != "" {
				if _, err := tx.Exec(ctx, `SELECT set_config('work_mem', $1, true)`, cfg.WorkMem); err != nil {
					return err
				}
			}
			if err := lockGateShared(ctx, tx, cfg.GateWait); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `SELECT ledger_close_span($1, $2)`, lo, hi).Scan(&ok)
		}); err != nil {
			return closed, err
		}
		if !ok {
			return closed, nil
		}
		closed++
		metrics.ObserveLedgerClose(time.Since(start))
		metrics.SetLedgerClosedThrough(uint64(hi - 1))
	}
	return closed, nil
}

// lockGateShared takes the history-DDL gate shared for the transaction, waiting at most wait; the transaction's own
// lock_timeout applies again afterwards.
func lockGateShared(ctx context.Context, tx pgx.Tx, wait time.Duration) error {
	if wait == 0 {
		wait = defaultLedgerGateWait
	}
	var prev string
	if err := tx.QueryRow(ctx, `SELECT current_setting('lock_timeout')`).Scan(&prev); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true)`, fmt.Sprintf("%dms", wait.Milliseconds())); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, historyDDLGate); err != nil {
		if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			return fmt.Errorf("%w for more than %s", errLedgerGateBusy, wait)
		}
		return err
	}
	_, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true)`, prev)
	return err
}

// RunLedger closes one span per interval while there is one to close, retention on or off.
func (d *DB) RunLedger(ctx context.Context, cfg LedgerConfig) {
	log.Info("ledger closer started", "span_blocks", cfg.SpanBlocks, "first_block", cfg.FirstBlock, "interval", cfg.Interval)
	for {
		if behind, lagging := d.ingestLagging(ctx, cfg); lagging {
			log.Info("ledger: ingest is behind the node; not closing a span this interval", "blocks_behind", behind, "max", cfg.MaxHeadLag)
		} else if n, err := d.CloseLedgerSpans(ctx, cfg, 1); err != nil && ctx.Err() == nil {
			log.Warn("ledger: closing a span failed; retrying next interval", "error", err)
		} else if n > 0 {
			log.Debug("ledger: span closed")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(cfg.Interval):
		}
	}
}

// ingestLagging reports whether ingest is more than MaxHeadLag blocks behind the node: the closer then yields its
// interval to the head path. With no MaxHeadLag, or the lag unknown, it never yields.
func (d *DB) ingestLagging(ctx context.Context, cfg LedgerConfig) (uint64, bool) {
	if cfg.MaxHeadLag == 0 || cfg.HeadLag == nil {
		return 0, false
	}
	lagCtx, cancel := context.WithTimeout(ctx, nodeHeadTimeout)
	defer cancel()
	behind, err := cfg.HeadLag(lagCtx)
	if err != nil {
		return 0, false
	}
	return behind, behind > cfg.MaxHeadLag
}
