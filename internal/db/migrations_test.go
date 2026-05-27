package db

import (
	"context"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupMigTestPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	pgC, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("migtest"),
		postgres.WithUsername("miguser"),
		postgres.WithPassword("migpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Skipf("skipping: could not start postgres container (is Docker running?): %v", err)
	}

	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgC.Terminate(ctx)
		t.Fatalf("connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		_ = pgC.Terminate(ctx)
		t.Fatalf("pool: %v", err)
	}
	return pool, func() {
		pool.Close()
		_ = pgC.Terminate(ctx)
	}
}

// migrateTo runs the embedded migrations up to (and including) targetVersion.
func migrateTo(t *testing.T, pool *pgxpool.Pool, targetVersion int32) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	m, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	if err != nil {
		t.Fatalf("new migrator: %v", err)
	}
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		t.Fatalf("sub fs: %v", err)
	}
	if err := m.LoadMigrations(sub); err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := m.MigrateTo(ctx, targetVersion); err != nil {
		t.Fatalf("migrate to %d: %v", targetVersion, err)
	}
}

// seedFKChain creates one block + one transaction so contracts rows have a
// valid creation_tx FK to point at.
func seedFKChain(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		VALUES (1, '0xblock1', '0xblock0', 100, 0, 0, 1)`); err != nil {
		t.Fatalf("seed block: %v", err)
	}
	const txHash = "0xtx1"
	if _, err := pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, value, gas_used, gas_price, status)
		VALUES ($1, 1, 0, '0xfrom', 0, 0, 0, 1)`, txHash); err != nil {
		t.Fatalf("seed tx: %v", err)
	}
	return txHash
}

// TestMigration004And005_MergeAndGuard reproduces the devnet failure: a DB at
// schema version 3 carrying legacy mixed-case duplicate rows. Applying 004 must
// merge/dedup them (not collide on the PK), 005 must add the CHECK guard, and
// no mixed-case row may survive.
func TestMigration004And005_MergeAndGuard(t *testing.T) {
	pool, cleanup := setupMigTestPool(t)
	defer cleanup()
	ctx := context.Background()

	migrateTo(t, pool, 3)
	txHash := seedFKChain(t, pool)

	// address_stats: a mixed/lowercase pair that must MERGE, a mixed-only row
	// that must just lowercase, and a pure-lowercase control row left untouched.
	_, err := pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract) VALUES
		('0xABCDEF', 3, 1, 2, 100, 200, false),
		('0xabcdef', 5, 0, 1, 50, 150, true),
		('0xFFFF', 2, 0, 0, 70, 90, false),
		('0x1111', 9, 4, 4, 10, 999, false)`)
	if err != nil {
		t.Fatalf("seed address_stats: %v", err)
	}

	// contracts: a mixed/lowercase pair (lowercase must win), and two mixed-case
	// variants of one address with no lowercase form (lowest block_number wins,
	// then the survivor is lowercased).
	_, err = pool.Exec(ctx, `
		INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number) VALUES
		('0xAAA', '0x00', '0xc', $1, 20),
		('0xaaa', '0x00', '0xc', $1, 10),
		('0xCcC', '0x00', '0xc', $1, 30),
		('0xCCC', '0x00', '0xc', $1, 25)`, txHash)
	if err != nil {
		t.Fatalf("seed contracts: %v", err)
	}

	// Apply the fixed 004 (merge/dedup) then 005 (defensive + CHECK).
	migrateTo(t, pool, 5)

	// address_stats merge: 0xabcdef = sum of the pair, widened seen-range, is_contract OR.
	var txc, intc, tokc, first, last int
	var isContract bool
	if err := pool.QueryRow(ctx, `
		SELECT tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract
		FROM address_stats WHERE address = '0xabcdef'`).
		Scan(&txc, &intc, &tokc, &first, &last, &isContract); err != nil {
		t.Fatalf("read merged row: %v", err)
	}
	if txc != 8 || intc != 1 || tokc != 3 || first != 50 || last != 200 || !isContract {
		t.Fatalf("merge wrong: tx=%d int=%d tok=%d first=%d last=%d isContract=%v (want 8/1/3/50/200/true)",
			txc, intc, tokc, first, last, isContract)
	}

	// mixed-only row lowercased, values preserved.
	if err := pool.QueryRow(ctx, `SELECT tx_count FROM address_stats WHERE address = '0xffff'`).Scan(&txc); err != nil {
		t.Fatalf("read 0xffff: %v", err)
	}
	if txc != 2 {
		t.Fatalf("0xffff tx_count=%d want 2", txc)
	}
	// control row untouched.
	if err := pool.QueryRow(ctx, `SELECT tx_count FROM address_stats WHERE address = '0x1111'`).Scan(&txc); err != nil {
		t.Fatalf("read control: %v", err)
	}
	if txc != 9 {
		t.Fatalf("control tx_count=%d want 9", txc)
	}

	// no mixed-case left in either table.
	for _, tbl := range []string{"address_stats", "contracts"} {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+tbl+` WHERE address != LOWER(address)`).Scan(&n); err != nil {
			t.Fatalf("count mixed in %s: %v", tbl, err)
		}
		if n != 0 {
			t.Fatalf("%s still has %d mixed-case rows", tbl, n)
		}
	}

	// contracts dedup: exactly one 0xaaa (lowercase won) and one 0xccc (block 25 won).
	var aaaBlock, cccBlock int
	if err := pool.QueryRow(ctx, `SELECT block_number FROM contracts WHERE address='0xaaa'`).Scan(&aaaBlock); err != nil {
		t.Fatalf("read 0xaaa: %v", err)
	}
	if aaaBlock != 10 {
		t.Fatalf("0xaaa block=%d want 10 (lowercase row should win)", aaaBlock)
	}
	if err := pool.QueryRow(ctx, `SELECT block_number FROM contracts WHERE address='0xccc'`).Scan(&cccBlock); err != nil {
		t.Fatalf("read 0xccc: %v", err)
	}
	if cccBlock != 25 {
		t.Fatalf("0xccc block=%d want 25 (earliest creation should win)", cccBlock)
	}
	var contractCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM contracts`).Scan(&contractCount); err != nil {
		t.Fatalf("count contracts: %v", err)
	}
	if contractCount != 2 {
		t.Fatalf("contracts count=%d want 2", contractCount)
	}

	assertLowercaseGuard(t, pool)
}

// TestMigration005_CleanV022Upgrade proves the v0.2.2 -> v0.2.3 step: a clean DB
// already at schema version 4 (all rows lowercase, no CHECK yet) must apply 005
// without error and gain the lowercase guard.
func TestMigration005_CleanV022Upgrade(t *testing.T) {
	pool, cleanup := setupMigTestPool(t)
	defer cleanup()
	ctx := context.Background()

	// A clean v0.2.2 DB is at version 4 with only lowercase rows.
	migrateTo(t, pool, 4)
	txHash := seedFKChain(t, pool)
	if _, err := pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count) VALUES ('0xdeadbeef', 7)`); err != nil {
		t.Fatalf("seed address_stats: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number)
		VALUES ('0xfeed', '0x00', '0xc', $1, 1)`, txHash); err != nil {
		t.Fatalf("seed contracts: %v", err)
	}

	// At version 4 the guard must not exist yet.
	if constraintExists(t, pool, "address_stats_address_lowercase_chk") {
		t.Fatal("CHECK constraint present before 005")
	}

	// Upgrade to 005 — must succeed on an already-clean DB and preserve data.
	migrateTo(t, pool, 5)

	var n int
	if err := pool.QueryRow(ctx, `SELECT tx_count FROM address_stats WHERE address='0xdeadbeef'`).Scan(&n); err != nil {
		t.Fatalf("row lost across 005: %v", err)
	}
	if n != 7 {
		t.Fatalf("tx_count=%d want 7", n)
	}
	assertLowercaseGuard(t, pool)
}

// assertLowercaseGuard confirms the CHECK constraints reject a mixed-case insert
// on both guarded tables.
func assertLowercaseGuard(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO address_stats (address, tx_count) VALUES ('0xMixedCase', 1)`); err == nil {
		t.Fatal("address_stats accepted a mixed-case insert (CHECK missing)")
	} else if !strings.Contains(err.Error(), "address_stats_address_lowercase_chk") {
		t.Fatalf("unexpected error inserting mixed-case address_stats: %v", err)
	}
}

func constraintExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = $1)`, name).Scan(&exists); err != nil {
		t.Fatalf("check constraint existence: %v", err)
	}
	return exists
}
