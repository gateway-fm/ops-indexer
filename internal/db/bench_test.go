package db

import (
	"context"
	"errors"
	"flag"
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
// smaller measures only the first minutes of a chain's life. Measured, the
// insert path barely notices the growth -- ingest is append-ordered, so it only
// touches the right-hand edge of each index -- but the derived-counter
// benchmarks scale almost linearly with stored history, and at a small scale
// they understate the deployed cost by about the factor they understate the
// history. Running the default needs roughly 12 GB of disk per scale; use
// BENCH_DATABASE_URL to point it at a properly sized server.
var benchScales = mustParseBenchScales(os.Getenv("BENCH_SCALES"))

// requirePinnedBenchtime fails unless -benchtime is pinned to a fixed iteration
// count (`30x`), rather than a duration.
//
// With a duration, the framework re-invokes the benchmark body at a growing b.N
// until the wall clock is filled, and every one of those attempts calls
// setupBenchDB and seed again. On testcontainers that means a brand-new
// container and a full re-seed per ramp step. That was survivable while the
// default scale was 10k; at 10M a plain `go test -bench .` will appear to hang.
// The correct invocation is cheap to state and impossible to infer from a stall,
// so state it.
func requirePinnedBenchtime(tb testing.TB) {
	tb.Helper()
	f := flag.Lookup("test.benchtime")
	if f == nil {
		return
	}
	if v := f.Value.String(); !strings.HasSuffix(v, "x") {
		tb.Fatalf("benchtime is %q, which ramps b.N and re-seeds the table at every step; "+
			"pin the iteration count instead, e.g. -benchtime 30x (see `make bench`)", v)
	}
}

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
func setupBenchDB(tb testing.TB) (*DB, func()) {
	tb.Helper()
	ctx := context.Background()

	if url := strings.TrimSpace(os.Getenv("BENCH_DATABASE_URL")); url != "" {
		pool, err := pgxpool.New(ctx, url)
		if err != nil {
			tb.Fatalf("connect to BENCH_DATABASE_URL: %v", err)
		}
		d := newBenchDB(pool)
		if err := d.Migrate(); err != nil {
			pool.Close()
			tb.Fatalf("migrate BENCH_DATABASE_URL: %v", err)
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
		tb.Skipf("skipping: could not start postgres container (is Docker running?): %v", err)
	}

	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgC.Terminate(ctx)
		tb.Fatalf("failed to get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		_ = pgC.Terminate(ctx)
		tb.Fatalf("failed to create pool: %v", err)
	}

	d := newBenchDB(pool)
	if err := d.Migrate(); err != nil {
		pool.Close()
		_ = pgC.Terminate(ctx)
		tb.Fatalf("failed to run migrations: %v", err)
	}

	return d, func() {
		pool.Close()
		_ = pgC.Terminate(ctx)
	}
}

// benchSeedStateDDL tracks how many rows seed() itself has written.
//
// Resumption cannot be derived from COUNT(*) FROM transactions, which is what
// this used to do. Every row identity here -- hash, block number, sender -- is a
// pure function of a row index, so topping up requires knowing exactly how far
// the synthetic range extends. But the benchmarks insert transactions too, at a
// different hash prefix and a different block density (250 per block against
// seed's 125), so that count drifts away from the range seed owns. The
// consequences were a foreign-key failure when the token seeder generated
// tx_hashes inside the resulting gap, a primary-key collision when the balance
// seeder restarted from holder 0, and silent holes in block numbering. Keying
// off a counter only seed() writes fixes all three at the assumption.
const benchSeedStateDDL = `
CREATE TABLE IF NOT EXISTS bench_seed_state (
    id                int PRIMARY KEY,
    seeded_txs        bigint NOT NULL DEFAULT 0,
    seeded_transfers  bigint NOT NULL DEFAULT 0,
    CONSTRAINT bench_seed_state_singleton CHECK (id = 1)
)`

// benchSeedProgress reads the singleton row, creating the table on first use.
func benchSeedProgress(tb testing.TB, d *DB) (seededTxs, seededTransfers int) {
	tb.Helper()
	ctx := context.Background()
	if _, err := d.pool.Exec(ctx, benchSeedStateDDL); err != nil {
		tb.Fatalf("create bench_seed_state: %v", err)
	}
	err := d.pool.QueryRow(ctx,
		`SELECT seeded_txs, seeded_transfers FROM bench_seed_state WHERE id = 1`,
	).Scan(&seededTxs, &seededTransfers)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		tb.Fatalf("read bench_seed_state: %v", err)
	}
	return seededTxs, seededTransfers
}

func recordBenchSeedProgress(tb testing.TB, d *DB, column string, n int) {
	tb.Helper()
	// column is one of two compile-time constants below, never user input.
	q := fmt.Sprintf(`
		INSERT INTO bench_seed_state (id, %s) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET %s = EXCLUDED.%s`, column, column, column)
	if _, err := d.pool.Exec(context.Background(), q, n); err != nil {
		tb.Fatalf("record bench_seed_state.%s: %v", column, err)
	}
}

// seed bulk-loads nTxs transactions via COPY (the batch-insert path would take
// minutes at 1M), then re-seeds chain_counters since COPY bypasses the live
// increment path.
//
// Every COPY here generates its rows one at a time through pgx.CopyFromSlice
// rather than building a [][]any first. Materialising them is what the obvious
// version does, and it costs roughly 780 bytes per transaction -- about 8 GB at
// the default 10M scale, which is more memory than the database server itself
// is usually given. Measured: 113 MB of client RSS at 100k.
func seed(tb testing.TB, d *DB, nTxs int) {
	tb.Helper()
	ctx := context.Background()

	const txsPerBlock = 125
	nBlocks := (nTxs + txsPerBlock - 1) / txsPerBlock
	now := time.Now().Unix()

	have, _ := benchSeedProgress(tb, d)
	if have >= nTxs {
		if _, err := d.pool.Exec(ctx, "ANALYZE"); err != nil {
			tb.Fatalf("analyze: %v", err)
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
		tb.Fatalf("seed blocks: %v", err)
	}

	poolSize := benchAddrPoolSize(nTxs)
	addrPool := make([]string, poolSize)
	for i := range addrPool {
		addrPool[i] = benchSeededAddr(i)
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
		tb.Fatalf("seed transactions: %v", err)
	}

	// address_stats is topped up like everything else. It used to be written
	// only on the first pass and only ever held 20 rows, which meant the table
	// was byte-for-byte identical at 100k and at 10M -- so the ON CONFLICT DO
	// UPDATE that InsertBlockDataBatch performs was always hitting a two-level
	// index that could not miss cache, at any scale. That flattered every shape
	// and specifically gutted erc20-no-address-stats, whose entire purpose is to
	// price address_stats maintenance.
	havePool := benchAddrPoolSize(have)
	if poolSize > havePool {
		if _, err := d.pool.CopyFrom(ctx,
			[]string{"address_stats"},
			[]string{"address", "tx_count", "internal_tx_count", "token_transfer_count", "first_seen", "last_seen", "is_contract"},
			pgx.CopyFromSlice(poolSize-havePool, func(j int) ([]any, error) {
				return []any{addrPool[havePool+j], int32(nTxs / poolSize), int32(0), int32(0),
					int64(0), int64(nBlocks), false}, nil
			}),
		); err != nil {
			tb.Fatalf("seed address_stats: %v", err)
		}
	}

	if firstBlock == 0 {
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
			tb.Fatalf("seed contracts: %v", err)
		}
	}

	if _, err := d.pool.Exec(ctx, `
		INSERT INTO chain_counters (name, count, updated_at) VALUES
			('blocks_total',       (SELECT COUNT(*) FROM blocks),        NOW()),
			('transactions_total', (SELECT COUNT(*) FROM transactions),  NOW()),
			('addresses_total',    (SELECT COUNT(*) FROM address_stats), NOW()),
			('tokens_total',       (SELECT COUNT(*) FROM tokens),        NOW())
		ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()`); err != nil {
		tb.Fatalf("reseed counters: %v", err)
	}

	recordBenchSeedProgress(tb, d, "seeded_txs", nTxs)

	if _, err := d.pool.Exec(ctx, "ANALYZE"); err != nil {
		tb.Fatalf("analyze: %v", err)
	}
}

// benchAddrPoolSize is how many distinct addresses a given scale seeds.
//
// It has to grow with the scale, or address_stats stays a toy table while
// transactions grows to gigabytes. One address per ten transactions is in the
// range real chains show, and it keeps the UPSERT hitting an index whose depth
// tracks the rest of the database. It also sets how many distinct senders a
// block can draw on, which is what decides the size of the AddressStats map
// InsertBlockDataBatch has to reconcile.
func benchAddrPoolSize(nTxs int) int {
	if nTxs <= 0 {
		return 0
	}
	if n := nTxs / 10; n > 1000 {
		return n
	}
	return 1000
}

// benchSeededAddr is the address at a given index in the seeded pool. Both
// seed() and the block generator derive senders from it, so a delta always
// lands on a row that already exists and takes the steady-state UPDATE branch.
func benchSeededAddr(i int) string {
	return fmt.Sprintf("0xaddr%036d", i)
}

func runScaled(b *testing.B, fn func(b *testing.B, d *DB)) {
	requirePinnedBenchtime(b)
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
