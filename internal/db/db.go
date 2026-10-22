package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/gateway-fm/chain-indexer/internal/log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
)

//go:embed migrations/*.sql
var migrations embed.FS

type DB struct {
	pool *pgxpool.Pool
	// HiddenTxTypes are transaction type numbers excluded from default listings
	// (e.g. 126 for OP deposit system transactions). Set via HIDDEN_TX_TYPES env var.
	HiddenTxTypes []int
	// RebuildWorkMem is the work_mem for the address_stats rebuild transaction.
	// Empty falls back to a safe default. Set via REBUILD_WORK_MEM env var.
	RebuildWorkMem string
	lateDays       lateDays
	statsGap       statsGap
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

// Migrate applies the pending migrations and then reconciles, for callers that
// do not open the API in between.
func (d *DB) Migrate() error {
	if err := d.MigrateSchema(); err != nil {
		return err
	}
	return d.Reconcile(context.Background())
}

// MigrateSchema applies the pending migrations only. The indexer binary runs it
// before opening gRPC, then Reconcile before ingest.
func (d *DB) MigrateSchema() error {
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
	return nil
}

// Reconcile re-derives the stored counters and the current-balance cache from
// the history tables. It must run before ingest starts: its absolute writes
// would otherwise overwrite deltas that committed while it was counting.
func (d *DB) Reconcile(ctx context.Context) error {
	if err := reseedCounters(ctx, d.pool); err != nil {
		return err
	}

	if err := d.reconcileTokenBalancesCurrent(ctx); err != nil {
		return err
	}

	// holder_count is delta-maintained by InsertBalancesBatch (PRST-4669), in
	// the transaction that writes the current balances, so no crash separates
	// the two. It is re-seeded here for databases a build without that writer
	// wrote: every database upgraded from 0.4.0, and one that ran an older
	// build again after a rollback.
	if err := d.ReseedHolderCounts(ctx); err != nil {
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

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// prunedButAttached lists, per history table, the partitions whose whole range is at or below the prune watermark
// but which are still attached: a prune moved the watermark past them and has not detached them yet. A parent's
// rows minus these are its retained rows. The watermark only moves past whole ranges, and a detach pending
// partition is no longer read through its parent.
func prunedButAttached(ctx context.Context, q execer) (map[string][]string, error) {
	rows, err := q.Query(ctx, `
		SELECT t, history_relname(t, p.suffix)
		FROM history_partitions p CROSS JOIN unnest(history_tables()) AS t
		WHERE p.hi IS NOT NULL AND p.hi <= (SELECT history_from())
		  AND EXISTS (SELECT 1 FROM pg_inherits i
		              WHERE i.inhrelid = to_regclass(history_relname(t, p.suffix)) AND i.inhparent = to_regclass(t)
		                AND NOT i.inhdetachpending)
		ORDER BY 1, 2`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var table, child string
		if err := rows.Scan(&table, &child); err != nil {
			return nil, err
		}
		out[table] = append(out[table], child)
	}
	return out, rows.Err()
}

// retainedCount counts a history table's retained rows: the whole parent (a parallel sequential scan: a filter on
// the block made Postgres pick an index-only scan whose heap fetches on a written table took 24-30 min on a production deployment)
// minus each pruned-but-attached partition, counted whole.
func retainedCount(table string, pruned []string) string {
	sql := `(SELECT COUNT(*) FROM ` + pgx.Identifier{table}.Sanitize() + `)`
	for _, child := range pruned {
		sql += ` - (SELECT COUNT(*) FROM ` + pgx.Identifier{child}.Sanitize() + `)`
	}
	return sql
}

const transferSupply = `COALESCE(
		SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
	  - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END),
		0)`

// retainedTokenTotals is each token's retained transfer count and ERC20 supply, built the same way.
func retainedTokenTotals(pruned []string) string {
	sql := `SELECT token_address, COUNT(*) AS cnt, ` + transferSupply + ` AS supply FROM token_transfers GROUP BY token_address`
	for _, child := range pruned {
		sql += `
			UNION ALL
			SELECT token_address, -COUNT(*), -` + transferSupply + ` FROM ` + pgx.Identifier{child}.Sanitize() + ` GROUP BY token_address`
	}
	return sql
}

func reseedChainCountersSQL(pruned map[string][]string) string {
	return `
		INSERT INTO chain_counters (name, count, updated_at) VALUES
			('blocks_total',       ` + retainedCount("blocks", pruned["blocks"]) + ` + prune_offset('chain', 'blocks_total'), NOW()),
			('transactions_total', ` + retainedCount("transactions", pruned["transactions"]) + ` + prune_offset('chain', 'transactions_total'), NOW()),
			('addresses_total',    (SELECT COUNT(*) FROM address_stats), NOW()),
			('tokens_total',       (SELECT COUNT(*) FROM tokens), NOW()),
			('transfers_total',    ` + retainedCount("token_transfers", pruned["token_transfers"]) + ` + prune_offset('chain', 'transfers_total'), NOW())
		ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()`
}

func reseedTokenCountersSQL(pruned map[string][]string) string {
	return `
		UPDATE tokens t SET
			transfer_count = s.cnt,
			total_supply = CASE WHEN t.token_type = 'ERC20' THEN ` + clampSupply(`s.supply`) + ` ELSE t.total_supply END
		FROM (
			SELECT token_address, SUM(cnt) AS cnt, SUM(supply) AS supply FROM (
				` + retainedTokenTotals(pruned["token_transfers"]) + `
				UNION ALL
				SELECT key, count, supply FROM prune_offsets WHERE kind = 'token'
			) u GROUP BY token_address
		) s
		WHERE t.address = s.token_address
		  AND (t.transfer_count IS DISTINCT FROM s.cnt
		       OR (t.token_type = 'ERC20' AND t.total_supply IS DISTINCT FROM s.supply))`
}

// A token with no transfer row at all (attached, pruned or not) and no offset is not covered by the re-seed above.
const zeroTokenCountersSQL = `
		UPDATE tokens SET transfer_count = 0
		WHERE transfer_count <> 0
		  AND NOT EXISTS (SELECT 1 FROM token_transfers WHERE token_address = tokens.address)
		  AND NOT EXISTS (SELECT 1 FROM prune_offsets WHERE kind = 'token' AND key = tokens.address)`

// reseedCounters re-derives chain_counters and the tokens' transfer counts and supply from the retained rows plus
// what pruned rows contributed (prune_offsets), the counters being maintained by deltas.
func reseedCounters(ctx context.Context, q execer) error {
	pruned, err := prunedButAttached(ctx, q)
	if err != nil {
		return fmt.Errorf("failed to list pruned partitions: %w", err)
	}
	if _, err := q.Exec(ctx, reseedChainCountersSQL(pruned)); err != nil {
		return fmt.Errorf("failed to re-seed chain_counters: %w", err)
	}
	log.Info("chain_counters re-seeded from row counts")
	if _, err := q.Exec(ctx, reseedTokenCountersSQL(pruned)); err != nil {
		return fmt.Errorf("failed to re-seed token transfer counters: %w", err)
	}
	if _, err := q.Exec(ctx, zeroTokenCountersSQL); err != nil {
		return fmt.Errorf("failed to zero token transfer counters: %w", err)
	}
	log.Info("token transfer counters re-seeded from token_transfers")
	return nil
}
