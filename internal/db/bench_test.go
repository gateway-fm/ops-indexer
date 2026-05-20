package db

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// 1M takes ~30-60s to seed and needs ~500MB RAM. Set BENCH_SCALES=10000 for
// quick checks.
var benchScales = []int{10_000, 100_000, 1_000_000}

func setupBenchDB(b *testing.B) (*DB, func()) {
	b.Helper()
	ctx := context.Background()

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

	d := &DB{pool: pool}
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
func seed(b *testing.B, d *DB, nTxs int) {
	b.Helper()
	ctx := context.Background()

	const txsPerBlock = 125
	nBlocks := (nTxs + txsPerBlock - 1) / txsPerBlock
	now := time.Now().Unix()

	rows := make([][]any, 0, nBlocks)
	for i := 0; i < nBlocks; i++ {
		ts := now - int64(2*(nBlocks-i))
		rows = append(rows, []any{
			int64(i + 1),
			fmt.Sprintf("0xblock%010d", i),
			fmt.Sprintf("0xparent%09d", i),
			ts,
			int64(21000), int64(30000000),
			int64(txsPerBlock),
		})
	}
	if _, err := d.pool.CopyFrom(ctx,
		[]string{"blocks"},
		[]string{"number", "hash", "parent_hash", "timestamp", "gas_used", "gas_limit", "transaction_count"},
		copyRows(rows),
	); err != nil {
		b.Fatalf("seed blocks: %v", err)
	}

	addrPool := make([]string, 20)
	for i := range addrPool {
		addrPool[i] = fmt.Sprintf("0xaddr%036d", i)
	}
	txRows := make([][]any, 0, nTxs)
	for i := 0; i < nTxs; i++ {
		blockNum := int64(i/txsPerBlock + 1)
		ts := now - int64(2*(nBlocks-int(blockNum)))
		from := addrPool[i%len(addrPool)]
		to := addrPool[(i+1)%len(addrPool)]
		txRows = append(txRows, []any{
			fmt.Sprintf("0xtx%062d", i),
			blockNum,
			ts,
			int64(i % txsPerBlock),
			from, to,
			"1",
			int64(21000), int64(20_000_000),
			int64(21000),
			int64(0),
			"0x",
			int64(1),
			int16(0),
		})
	}
	if _, err := d.pool.CopyFrom(ctx,
		[]string{"transactions"},
		[]string{"hash", "block_number", "block_timestamp", "tx_index", "from_address", "to_address",
			"value", "gas_used", "gas_price", "gas_limit", "tx_type", "input_data", "status", "categories"},
		copyRows(txRows),
	); err != nil {
		b.Fatalf("seed transactions: %v", err)
	}

	asRows := make([][]any, 0, len(addrPool))
	for _, a := range addrPool {
		asRows = append(asRows, []any{a, int32(nTxs / len(addrPool)), int32(0), int32(0), int64(0), int64(nBlocks), false})
	}
	if _, err := d.pool.CopyFrom(ctx,
		[]string{"address_stats"},
		[]string{"address", "tx_count", "internal_tx_count", "token_transfer_count", "first_seen", "last_seen", "is_contract"},
		copyRows(asRows),
	); err != nil {
		b.Fatalf("seed address_stats: %v", err)
	}

	cRows := make([][]any, 0, 5)
	for i := 0; i < 5; i++ {
		cRows = append(cRows, []any{
			fmt.Sprintf("0xcontract%030d", i),
			"0x60",
			fmt.Sprintf("0xtx%062d", i),
			addrPool[0],
			int64(1),
			false,
		})
	}
	if _, err := d.pool.CopyFrom(ctx,
		[]string{"contracts"},
		[]string{"address", "bytecode", "creation_tx", "creator", "block_number", "is_verified"},
		copyRows(cRows),
	); err != nil {
		b.Fatalf("seed contracts: %v", err)
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

type copyRowsSource struct {
	rows [][]any
	i    int
}

func copyRows(rows [][]any) *copyRowsSource      { return &copyRowsSource{rows: rows} }
func (s *copyRowsSource) Next() bool             { s.i++; return s.i <= len(s.rows) }
func (s *copyRowsSource) Values() ([]any, error) { return s.rows[s.i-1], nil }
func (s *copyRowsSource) Err() error             { return nil }

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
		addr := "0xaddr00000000000000000000000000000000003"
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
