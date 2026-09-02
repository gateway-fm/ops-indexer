package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/gateway-fm/chain-indexer/internal/log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
)

//go:embed migrations/*.sql
var migrations embed.FS

type DB struct {
	pool          *pgxpool.Pool
	// HiddenTxTypes are transaction type numbers excluded from default listings
	// (e.g. 126 for OP deposit system transactions). Set via HIDDEN_TX_TYPES env var.
	HiddenTxTypes []int
	// RebuildWorkMem is the work_mem for the address_stats rebuild transaction.
	// Empty falls back to a safe default. Set via REBUILD_WORK_MEM env var.
	RebuildWorkMem string
}

func New(databaseURL string) (*DB, error) {
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(context.Background()); err != nil {
		return nil, err
	}
	return &DB{pool: pool, HiddenTxTypes: []int{}}, nil
}

func (d *DB) Close() {
	d.pool.Close()
}

func (d *DB) Migrate() error {
	ctx := context.Background()

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	migrator, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}

	migrationsFS, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("failed to get migrations sub-fs: %w", err)
	}

	err = migrator.LoadMigrations(migrationsFS)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	migrator.OnStart = func(seq int32, name string, direction string, sql string) {
		log.Info(fmt.Sprintf("running migration %d: %s %s", seq, name, direction))
	}

	count := len(migrator.Migrations)
	log.Info("migrations loaded", "count", count)

	if err = migrator.Migrate(ctx); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	// Reconciles counters against actual row counts in case earlier writes
	// bypassed the live-increment path (e.g. pre-existing rows on first deploy).
	if _, err := d.pool.Exec(ctx, `
		INSERT INTO chain_counters (name, count, updated_at) VALUES
			('blocks_total',       (SELECT COUNT(*) FROM blocks),          NOW()),
			('transactions_total', (SELECT COUNT(*) FROM transactions),    NOW()),
			('addresses_total',    (SELECT COUNT(*) FROM address_stats),   NOW()),
			('tokens_total',       (SELECT COUNT(*) FROM tokens),          NOW()),
			('transfers_total',    (SELECT COUNT(*) FROM token_transfers), NOW())
		ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()`); err != nil {
		return fmt.Errorf("failed to re-seed chain_counters: %w", err)
	}
	log.Info("chain_counters re-seeded from row counts")

	if err := d.reconcileTokenBalancesCurrent(ctx); err != nil {
		return err
	}

	return nil
}

// reconcileTokenBalancesCurrent repairs token_balances_current from balances if
// it has fallen behind.
//
// It runs on every startup rather than only on migration, because the table is
// maintained by the writer and a build that predates that writer keeps
// advancing balances while leaving this table frozen. The 008 migration has
// already been recorded as applied by then, so nothing would ever repair it --
// a downgrade followed by an upgrade would silently serve a stale holder_count
// forever. That is not hypothetical: PRST-4493's first attempt was deployed and
// rolled back on the same day.
//
// The probe is a range scan against idx_balances_block and costs nothing on a
// healthy database, which is the normal path -- including first deploy, where
// the migration has just backfilled and the watermark is current.
//
// A global watermark cannot by itself see a stale row sitting BELOW it, which
// out-of-order balance writes can produce. It does not need to: the repair is a
// full pass, so it only has to be triggered once, and any gap during which
// balances drifted is a gap during which the chain advanced -- which puts rows
// above the watermark and fires the probe. A gap with no new balances is a gap
// in which nothing drifted. Hence the deliberate asymmetry: cheap detection,
// total repair. A windowed repair would have needed the detection to be exact.
func (d *DB) reconcileTokenBalancesCurrent(ctx context.Context) error {
	var behind int64
	if err := d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM balances
		WHERE block_number > COALESCE((SELECT MAX(block_number) FROM token_balances_current), -1)`,
	).Scan(&behind); err != nil {
		return fmt.Errorf("failed to probe token_balances_current staleness: %w", err)
	}
	if behind == 0 {
		return nil
	}

	log.Info("token_balances_current is behind balances, reconciling", "balance_rows_ahead", behind)

	tag, err := d.pool.Exec(ctx, `
		INSERT INTO token_balances_current (token_address, address, balance, block_number)
		SELECT DISTINCT ON (token_address, address) token_address, address, balance, block_number
		FROM balances
		ORDER BY token_address, address, block_number DESC
		ON CONFLICT (token_address, address) DO UPDATE
			SET balance = EXCLUDED.balance,
				block_number = EXCLUDED.block_number
			WHERE EXCLUDED.block_number >= token_balances_current.block_number`)
	if err != nil {
		return fmt.Errorf("failed to reconcile token_balances_current: %w", err)
	}
	log.Info("token_balances_current reconciled", "rows", tag.RowsAffected())

	return nil
}
