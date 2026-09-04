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

func (d *DB) PoolStat() *pgxpool.Stat {
	return d.pool.Stat()
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

	// Same reconciliation, one level down: tokens.transfer_count and
	// tokens.total_supply are maintained by deltas in InsertBlockDataBatch, so
	// every row written before that existed still holds a value computed the
	// old way -- and rows written by a build without the delta would keep
	// drifting. One absolute pass at startup makes them agree with the stored
	// transfers.
	//
	// ERC721 total_supply is left alone: it derives from nft_tokens rather than
	// from token_transfers and RefreshTokenStats still recomputes it per block.
	if _, err := d.pool.Exec(ctx, `
		UPDATE tokens t SET
			transfer_count = s.cnt,
			total_supply = CASE WHEN t.token_type = 'ERC20' THEN `+clampSupply(`s.supply`)+` ELSE t.total_supply END
		FROM (
			SELECT token_address,
			       COUNT(*) AS cnt,
			       COALESCE(
			           SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
			         - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END),
			           0) AS supply
			FROM token_transfers GROUP BY token_address
		) s
		WHERE t.address = s.token_address
		  AND (t.transfer_count IS DISTINCT FROM s.cnt
		       OR (t.token_type = 'ERC20' AND t.total_supply IS DISTINCT FROM s.supply))`); err != nil {
		return fmt.Errorf("failed to re-seed token transfer counters: %w", err)
	}

	// A token whose every transfer row is gone (or that never had one) is not
	// covered by the join above; zero it so a stale count cannot survive.
	if _, err := d.pool.Exec(ctx, `
		UPDATE tokens SET transfer_count = 0
		WHERE transfer_count <> 0
		  AND NOT EXISTS (SELECT 1 FROM token_transfers WHERE token_address = tokens.address)`); err != nil {
		return fmt.Errorf("failed to zero token transfer counters: %w", err)
	}
	log.Info("token transfer counters re-seeded from token_transfers")

	if err := d.reconcileTokenBalancesCurrent(ctx); err != nil {
		return err
	}

	return nil
}

// reconcileTokenBalancesCurrent repairs token_balances_current from balances.
//
// It runs unconditionally on every startup, because the table is maintained by
// the writer and a build without that writer keeps appending to balances while
// leaving this table behind. Migration 008 is recorded as applied by then, so
// nothing else would ever repair it, and holder_count would stay wrong
// indefinitely.
//
// There is no cheap staleness probe, and that is deliberate. Balance writes are
// not ordered: the missing-range collector reprocesses historical blocks
// continuously, so a previously unseen (token, address) can be written at a low
// block number while some other row already holds a much higher global maximum.
// Any watermark-style detector misses exactly that case, and a detector that
// does not -- comparing the source's winning rows against the cache -- is the
// same query as the repair. Measured on a multi-million-row balances table, a
// distinct-key count costs within a few percent of the full comparison and
// still cannot see a stale value on an existing key. Detection cheaper than
// repair does not exist here, so there is nothing to gain by detecting first.
//
// The IS DISTINCT FROM clause is what keeps that affordable: on a healthy cache
// it writes zero rows rather than rewriting every one of them, which also makes
// the logged count mean "rows repaired" instead of "rows examined".
func (d *DB) reconcileTokenBalancesCurrent(ctx context.Context) error {
	tag, err := d.pool.Exec(ctx, `
		INSERT INTO token_balances_current (token_address, address, balance, block_number)
		SELECT DISTINCT ON (token_address, address) token_address, address, balance, block_number
		FROM balances
		ORDER BY token_address, address, block_number DESC
		ON CONFLICT (token_address, address) DO UPDATE
			SET balance = EXCLUDED.balance,
				block_number = EXCLUDED.block_number
			WHERE EXCLUDED.block_number >= token_balances_current.block_number
			  AND (token_balances_current.balance, token_balances_current.block_number)
				  IS DISTINCT FROM (EXCLUDED.balance, EXCLUDED.block_number)`)
	if err != nil {
		return fmt.Errorf("failed to reconcile token_balances_current: %w", err)
	}

	// Anything other than zero means the cache had drifted, which should not
	// happen while the writer is the only thing touching balances -- so it is
	// worth noticing rather than logging at Info alongside normal startup.
	if n := tag.RowsAffected(); n > 0 {
		log.Warn("token_balances_current had drifted and was repaired", "rows", n)
	} else {
		log.Info("token_balances_current reconciled, no drift")
	}

	return nil
}
