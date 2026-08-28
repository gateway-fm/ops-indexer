package db

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// benchScales is the set of pre-existing table sizes every benchmark is run
// against, so the cost curve as the table grows is measured rather than
// assumed.
//
// 1M takes ~30-60s to seed and needs ~500MB RAM, and seed() runs again for
// every trial the framework schedules at a growing b.N, so the full set is
// expensive. Override it for quick checks:
//
//	BENCH_SCALES=100000 go test ./internal/db -bench InsertBlockDataBatch
//
// The default floor is 10M rows on purpose. At a sustained 500 tx/s a chain
// reaches 1M transactions in about half an hour and 43M in a day, so anything
// smaller measures only the first minutes of a chain's life -- and, more
// importantly, stays inside the regime where the whole working set is resident
// in RAM. Insert cost is flat there and falls off a cliff when the indexes stop
// fitting, so a small scale reports a flat curve that says nothing about the
// deployed system. Running the default needs roughly 8 GB of disk per scale;
// use BENCH_DATABASE_URL to point it at a properly sized server.
var benchScales = mustParseBenchScales(os.Getenv("BENCH_SCALES"))

// mustParseBenchScales reads a comma-separated scale list, falling back to the
// full set when unset. A malformed value panics rather than silently reverting
// to the slow default -- an override that quietly does nothing is how the
// previous version of this escape hatch went unnoticed.
func mustParseBenchScales(env string) []int {
	if strings.TrimSpace(env) == "" {
		return []int{10_000_000}
	}
	var out []int
	for _, field := range strings.Split(env, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		n, err := strconv.Atoi(field)
		if err != nil || n <= 0 {
			panic(fmt.Sprintf("BENCH_SCALES: %q is not a positive integer", field))
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		panic("BENCH_SCALES: set but contained no scales")
	}
	return out
}

// newBenchDB builds a DB the way New() does rather than by filling in the pool
// alone. HiddenTxTypes is the reason it has to: New() sets it to an empty slice,
// a bare &DB{pool: pool} leaves it nil, and pgx sends a nil []int as SQL NULL.
// `NOT (tx_type = ANY(NULL))` is NULL for every row, so the listing queries match
// nothing -- and instead of an indexed walk that stops at the LIMIT, the planner
// picks a parallel sequential scan of the whole table and returns zero rows.
// BenchmarkGetTransactionsWithCategories_latest10 was timing that: 1.17 s at 10M
// for a query that takes 0.2 ms when the parameter is an empty array.
func newBenchDB(pool *pgxpool.Pool) *DB {
	return &DB{pool: pool, HiddenTxTypes: []int{}}
}

// setupBenchDB returns a benchmark database. By default it starts a throwaway
// Postgres via testcontainers, which is convenient but caps the usable scale at
// whatever the local machine can hold -- and inherits the host's page cache, so
// the working set stays resident far longer than it would on a real server.
//
// Set BENCH_DATABASE_URL to run against an existing Postgres instead. The
// benchmark then does NOT create or drop anything beyond the schema, so point it
// only at a database you are willing to have written to.
func setupBenchDB(b *testing.B) (*DB, func()) {
	b.Helper()
	ctx := context.Background()

	if url := strings.TrimSpace(os.Getenv("BENCH_DATABASE_URL")); url != "" {
		pool, err := pgxpool.New(ctx, url)
		if err != nil {
			b.Fatalf("connect to BENCH_DATABASE_URL: %v", err)
		}
		d := newBenchDB(pool)
		if err := d.Migrate(); err != nil {
			pool.Close()
			b.Fatalf("migrate BENCH_DATABASE_URL: %v", err)
		}
		return d, func() { pool.Close() }
	}

	pgC, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("benchdb"),
		postgres.WithUsername("benchuser"),
		postgres.WithPassword("benchpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		b.Skipf("skipping: could not start postgres container (is Docker running?): %v", err)
	}

	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgC.Terminate(ctx)
		b.Fatalf("failed to get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		_ = pgC.Terminate(ctx)
		b.Fatalf("failed to create pool: %v", err)
	}

	d := newBenchDB(pool)
	if err := d.Migrate(); err != nil {
		pool.Close()
		_ = pgC.Terminate(ctx)
		b.Fatalf("failed to run migrations: %v", err)
	}

	return d, func() {
		pool.Close()
		_ = pgC.Terminate(ctx)
	}
}

// seed bulk-loads nTxs transactions via COPY (the batch-insert path would take
// minutes at 1M), then re-seeds chain_counters since COPY bypasses the live
// increment path.
//
// Every COPY here generates its rows one at a time through pgx.CopyFromSlice
// rather than building a [][]any first. Materialising them is what the obvious
// version does, and it costs roughly 800 bytes per transaction -- about 8 GB at
// the default 10M scale, which is more memory than the database server itself
// is usually given. Measured: 113 MB of client RSS at 100k.
func seed(b *testing.B, d *DB, nTxs int) {
	b.Helper()
	ctx := context.Background()

	const txsPerBlock = 125
	nBlocks := (nTxs + txsPerBlock - 1) / txsPerBlock
	now := time.Now().Unix()

	// With BENCH_DATABASE_URL the database persists across runs and across
	// ascending scales, so seeding has to top up rather than start from zero --
	// every row here has a primary key that would otherwise collide. Row
	// identities are a pure function of the index, so resuming at `have` keeps
	// block numbers and hashes contiguous.
	var have int
	if err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions`).Scan(&have); err != nil {
		b.Fatalf("count existing transactions: %v", err)
	}
	if have >= nTxs {
		if _, err := d.pool.Exec(ctx, "ANALYZE"); err != nil {
			b.Fatalf("analyze: %v", err)
		}
		return
	}
	firstBlock := have / txsPerBlock

	if _, err := d.pool.CopyFrom(ctx,
		[]string{"blocks"},
		[]string{"number", "hash", "parent_hash", "timestamp", "gas_used", "gas_limit", "transaction_count"},
		pgx.CopyFromSlice(nBlocks-firstBlock, func(j int) ([]any, error) {
			i := firstBlock + j
			return []any{
				int64(i + 1),
				fmt.Sprintf("0xblock%010d", i),
				fmt.Sprintf("0xparent%09d", i),
				now - int64(2*(nBlocks-i)),
				int64(21000), int64(30000000),
				int64(txsPerBlock),
			}, nil
		}),
	); err != nil {
		b.Fatalf("seed blocks: %v", err)
	}

	addrPool := make([]string, 20)
	for i := range addrPool {
		addrPool[i] = fmt.Sprintf("0xaddr%036d", i)
	}
	if _, err := d.pool.CopyFrom(ctx,
		[]string{"transactions"},
		[]string{"hash", "block_number", "block_timestamp", "tx_index", "from_address", "to_address",
			"value", "gas_used", "gas_price", "gas_limit", "tx_type", "input_data", "status", "categories"},
		pgx.CopyFromSlice(nTxs-have, func(j int) ([]any, error) {
			i := have + j
			blockNum := int64(i/txsPerBlock + 1)
			return []any{
				fmt.Sprintf("0xtx%062d", i),
				blockNum,
				now - int64(2*(nBlocks-int(blockNum))),
				int64(i % txsPerBlock),
				addrPool[i%len(addrPool)], addrPool[(i+1)%len(addrPool)],
				"1",
				int64(21000), int64(20_000_000),
				int64(21000),
				int64(0),
				"0x",
				int64(1),
				int16(0),
			}, nil
		}),
	); err != nil {
		b.Fatalf("seed transactions: %v", err)
	}

	if firstBlock == 0 {
		if _, err := d.pool.CopyFrom(ctx,
			[]string{"address_stats"},
			[]string{"address", "tx_count", "internal_tx_count", "token_transfer_count", "first_seen", "last_seen", "is_contract"},
			pgx.CopyFromSlice(len(addrPool), func(i int) ([]any, error) {
				return []any{addrPool[i], int32(nTxs / len(addrPool)), int32(0), int32(0),
					int64(0), int64(nBlocks), false}, nil
			}),
		); err != nil {
			b.Fatalf("seed address_stats: %v", err)
		}

		if _, err := d.pool.CopyFrom(ctx,
			[]string{"contracts"},
			[]string{"address", "bytecode", "creation_tx", "creator", "block_number", "is_verified"},
			pgx.CopyFromSlice(5, func(i int) ([]any, error) {
				return []any{
					fmt.Sprintf("0xcontract%030d", i),
					"0x60",
					fmt.Sprintf("0xtx%062d", i),
					addrPool[0],
					int64(1),
					false,
				}, nil
			}),
		); err != nil {
			b.Fatalf("seed contracts: %v", err)
		}
	}

	if _, err := d.pool.Exec(ctx, `
		INSERT INTO chain_counters (name, count, updated_at) VALUES
			('blocks_total',       (SELECT COUNT(*) FROM blocks),        NOW()),
			('transactions_total', (SELECT COUNT(*) FROM transactions),  NOW()),
			('addresses_total',    (SELECT COUNT(*) FROM address_stats), NOW()),
			('tokens_total',       (SELECT COUNT(*) FROM tokens),        NOW())
		ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()`); err != nil {
		b.Fatalf("reseed counters: %v", err)
	}

	if _, err := d.pool.Exec(ctx, "ANALYZE"); err != nil {
		b.Fatalf("analyze: %v", err)
	}
}

func runScaled(b *testing.B, fn func(b *testing.B, d *DB)) {
	for _, n := range benchScales {
		n := n
		b.Run(fmt.Sprintf("n=%s", humanize(n)), func(b *testing.B) {
			d, cleanup := setupBenchDB(b)
			defer cleanup()
			seed(b, d, n)
			b.ResetTimer()
			fn(b, d)
		})
	}
}

func humanize(n int) string {
	switch {
	case n >= 1_000_000:
		return strconv.Itoa(n/1_000_000) + "M"
	case n >= 1_000:
		return strconv.Itoa(n/1_000) + "k"
	default:
		return strconv.Itoa(n)
	}
}

func BenchmarkGetChainStats(b *testing.B) {
	runScaled(b, func(b *testing.B, d *DB) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			if _, err := d.GetChainStats(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetTransactionsWithCategories_latest10(b *testing.B) {
	runScaled(b, func(b *testing.B, d *DB) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			if _, err := d.GetTransactionsWithCategories(ctx, 10, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetTransactionHistory_24h(b *testing.B) {
	runScaled(b, func(b *testing.B, d *DB) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			if _, err := d.GetTransactionHistory(ctx, 3600, 24); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetContract(b *testing.B) {
	runScaled(b, func(b *testing.B, d *DB) {
		ctx := context.Background()
		hit := "0xcontract000000000000000000000000000003"
		for i := 0; i < b.N; i++ {
			if _, err := d.GetContract(ctx, hit); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetAddressStats(b *testing.B) {
	runScaled(b, func(b *testing.B, d *DB) {
		ctx := context.Background()
		// Must match seed()'s "0xaddr%036d" exactly. It was one zero short,
		// which made this benchmark time a row that does not exist -- an index
		// probe that misses, not the lookup it claims to measure.
		addr := fmt.Sprintf("0xaddr%036d", 3)
		for i := 0; i < b.N; i++ {
			if _, err := d.GetAddressStats(ctx, addr); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetTransaction_byHash(b *testing.B) {
	runScaled(b, func(b *testing.B, d *DB) {
		ctx := context.Background()
		hash := fmt.Sprintf("0xtx%062d", 3)
		for i := 0; i < b.N; i++ {
			if _, err := d.GetTransactionWithCategories(ctx, hash); err != nil {
				b.Fatal(err)
			}
		}
	})
}
