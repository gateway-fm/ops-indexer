package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/metrics"
)

// ledgerIndexes are the ledger's lookup indexes that grow with the table: ledger_address by address (the span
// rebase, the running check, the reconciliation and repair-address-stats read an address's rows through it) and
// ledger_token by token. They are built CONCURRENTLY after startup, not in a migration, which would hold the table
// against writes, and ingest and gRPC behind it, for as long as the build took.
var ledgerIndexes = []struct{ name, table, column string }{
	{"ledger_address_address", "ledger_address", "address"},
	{"ledger_token_token", "ledger_token", "token"},
}

// EnsureLedgerIndexes builds every missing ledger index CONCURRENTLY, replacing one a stopped build left invalid.
// Ingest and gRPC run meanwhile; the jobs that read through the indexes wait for them (LedgerIndexesReady).
func (d *DB) EnsureLedgerIndexes(ctx context.Context) error {
	for _, ix := range ledgerIndexes {
		var exists, valid bool
		if err := d.pool.QueryRow(ctx, `
			SELECT COALESCE(bool_or(true), false), COALESCE(bool_and(i.indisvalid), false)
			FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE c.relname = $1 AND c.relnamespace = current_schema()::regnamespace`, ix.name).Scan(&exists, &valid); err != nil {
			return err
		}
		if exists && valid {
			continue
		}
		if exists {
			log.Warn("dropping a ledger index a stopped build left invalid", "index", ix.name)
			if _, err := d.pool.Exec(ctx, fmt.Sprintf(`DROP INDEX CONCURRENTLY IF EXISTS %s`, ix.name)); err != nil {
				return fmt.Errorf("drop invalid %s: %w", ix.name, err)
			}
		}
		log.Info("building a ledger index; ingest runs meanwhile, the ledger checks and rebases wait for it", "index", ix.name)
		start := time.Now()
		if _, err := d.pool.Exec(ctx, fmt.Sprintf(`CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)`,
			ix.name, ix.table, ix.column)); err != nil {
			return fmt.Errorf("build %s: %w", ix.name, err)
		}
		log.Info("ledger index built", "index", ix.name, "duration", time.Since(start).Round(time.Millisecond))
	}
	_, err := d.LedgerIndexesReady(ctx)
	return err
}

// LedgerIndexesReady reports whether every ledger index exists and is valid.
func (d *DB) LedgerIndexesReady(ctx context.Context) (bool, error) {
	names := make([]string, len(ledgerIndexes))
	for i, ix := range ledgerIndexes {
		names[i] = ix.name
	}
	var ready int
	err := d.pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = ANY ($1) AND c.relnamespace = current_schema()::regnamespace AND i.indisvalid`, names).Scan(&ready)
	if err == nil {
		metrics.SetLedgerIndexesReady(ready == len(ledgerIndexes))
	}
	return ready == len(ledgerIndexes), err
}

var ledgerIndexWaitLog struct {
	sync.Mutex
	last map[string]time.Time
}

// ledgerIndexesReadyFor reports whether job may read through the ledger indexes, logging at most once a minute per
// job while it waits for them.
func (d *DB) ledgerIndexesReadyFor(ctx context.Context, job string) bool {
	ready, err := d.LedgerIndexesReady(ctx)
	if err == nil && ready {
		return true
	}
	ledgerIndexWaitLog.Lock()
	defer ledgerIndexWaitLog.Unlock()
	if ledgerIndexWaitLog.last == nil {
		ledgerIndexWaitLog.last = map[string]time.Time{}
	}
	if time.Since(ledgerIndexWaitLog.last[job]) >= time.Minute {
		ledgerIndexWaitLog.last[job] = time.Now()
		log.Info("waiting for the ledger indexes to be built", "job", job, "error", err)
	}
	return false
}

// waitLedgerIndexes blocks job until the ledger indexes are ready, checking every interval; false when ctx ends first.
func (d *DB) waitLedgerIndexes(ctx context.Context, job string, interval time.Duration) bool {
	for !d.ledgerIndexesReadyFor(ctx, job) {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(interval):
		}
	}
	return true
}
