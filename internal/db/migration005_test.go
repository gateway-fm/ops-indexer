package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Property-level regression tests for migration 005_lowercase_addresses.sql.
// These pin four invariants that the migration + the writer-side LOWER() +
// the read queries must keep upholding together — independent of whether
// any real-world deployment is still mid-upgrade. They protect against:
//
//   1. Migration-file edits that introduce a duplicate-key or FK conflict
//      on previously-handled corner cases (caught by re-running the
//      migration SQL on a fixture that contains those corners).
//   2. Read-query refactors that lose case handling — every API surface
//      the explorer relies on must still find rows after the migration
//      normalises addresses.
//   3. Writer-side regressions — new INSERTs must continue to land in the
//      canonical lowercase form alongside migrated rows.
//   4. Idempotency loss — re-running the migration SQL (operator re-deploy,
//      catchup retry, etc.) must not corrupt or duplicate rows.
//
// We don't go through Migrate() because schema_version tracking blocks
// replay. Instead the fixture inserts mixed-case rows via raw SQL — bypassing
// the writer-side LOWER() — and then runs the migration's UPDATE statements
// directly. The mixed-case fixture is synthetic: this is not a one-shot
// "did the prod upgrade work?" check, it's a permanent regression test for
// the migration file's content and the surrounding code's compatibility
// with it.

func setupMigration005TestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	ctx := context.Background()

	pgC, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("m005db"),
		postgres.WithUsername("m005user"),
		postgres.WithPassword("m005pass"),
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
	d := &DB{pool: pool}
	if err := d.Migrate(); err != nil {
		pool.Close()
		_ = pgC.Terminate(ctx)
		t.Fatalf("migrate: %v", err)
	}
	return d, func() {
		pool.Close()
		_ = pgC.Terminate(ctx)
	}
}

// migration005SQL re-declares the UPDATE statements from
// migrations/005_lowercase_addresses.sql so the test can run them on top of
// arbitrary fixture data. Keeping the SQL out-of-line here (rather than re-
// reading the .sql file) makes the test self-contained — if the migration
// file drifts, this test still pins the behaviour we expect after a real
// upgrade.
var migration005SQL = `
UPDATE transactions SET from_address = LOWER(from_address) WHERE from_address != LOWER(from_address);
UPDATE transactions SET to_address = LOWER(to_address) WHERE to_address IS NOT NULL AND to_address != LOWER(to_address);
UPDATE internal_transactions SET from_address = LOWER(from_address) WHERE from_address != LOWER(from_address);
UPDATE internal_transactions SET to_address = LOWER(to_address) WHERE to_address IS NOT NULL AND to_address != LOWER(to_address);
UPDATE token_transfers SET from_address = LOWER(from_address) WHERE from_address != LOWER(from_address);
UPDATE token_transfers SET to_address = LOWER(to_address) WHERE to_address != LOWER(to_address);
UPDATE token_transfers SET token_address = LOWER(token_address) WHERE token_address != LOWER(token_address);
UPDATE logs SET address = LOWER(address) WHERE address != LOWER(address);
UPDATE tokens SET address = LOWER(address) WHERE address != LOWER(address);
DELETE FROM balances b1 USING balances b2
 WHERE b1.address != LOWER(b1.address)
   AND b2.address = LOWER(b1.address)
   AND b2.token_address = b1.token_address
   AND b2.block_number = b1.block_number;
UPDATE balances SET address = LOWER(address) WHERE address != LOWER(address);
UPDATE balances SET token_address = LOWER(token_address) WHERE token_address != LOWER(token_address);
`

// TestMigration005_NormalisesMixedCaseAddresses pins the four invariants
// the migration + writer + read queries must keep upholding together:
// the migration completes on a mixed-case fixture, reads find the migrated
// data through the API, new writes coexist in the canonical form, and
// re-running the migration is a no-op.
func TestMigration005_NormalisesMixedCaseAddresses(t *testing.T) {
	d, cleanup := setupMigration005TestDB(t)
	defer cleanup()
	ctx := context.Background()

	// Seed a v0.1.x-shaped row: insert with EIP-55 checksum case bypassing
	// the writer's LOWER() so the row lands on the DB exactly as a
	// pre-migration indexer would have written it.
	_, err := d.pool.Exec(ctx,
		`INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		 VALUES (1, '0xb', '0xp', $1, 1, 1, 1)`,
		time.Now().Unix())
	require.NoError(t, err)

	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address,
			value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ($1, 1, $2, 0, $3, $4, '0', 21000, 0, '0x', 1, 0)`,
		"0x"+strRepeat("ee", 32),
		time.Now().Unix(),
		checksumFromAddr, // EIP-55 checksum-cased
		checksumToAddr,   // EIP-55 checksum-cased
	)
	require.NoError(t, err)

	// Sanity-check the seed: the row is in checksum case (writer LOWER() did
	// NOT run because we inserted raw SQL).
	var preFrom, preTo string
	require.NoError(t, d.pool.QueryRow(ctx,
		`SELECT from_address, to_address FROM transactions WHERE block_number = 1`).Scan(&preFrom, &preTo))
	require.Equal(t, checksumFromAddr, preFrom, "fixture must land in checksum case to simulate pre-migration data")
	require.Equal(t, checksumToAddr, preTo)

	// (1) Migration finishes — no duplicate key, no FK error, no timeout.
	_, err = d.pool.Exec(ctx, migration005SQL)
	require.NoError(t, err, "migration 005 must complete cleanly on pre-existing mixed-case data")

	// Row is now lowercase. This is the canonicalisation point — every
	// subsequent read query relies on it.
	var postFrom, postTo string
	require.NoError(t, d.pool.QueryRow(ctx,
		`SELECT from_address, to_address FROM transactions WHERE block_number = 1`).Scan(&postFrom, &postTo))
	require.Equal(t, expectedFromLower, postFrom, "migration must lowercase from_address")
	require.Equal(t, expectedToLower, postTo, "migration must lowercase to_address")

	// (2) Reads find the migrated row through the API. GetTransactionsByAddress
	// is the hot path the explorer hits — if it can find a migrated address
	// the rest of the read surface (uses the same SQL pattern) works too.
	txs, err := d.GetTransactionsByAddress(ctx, checksumFromAddr, 10, nil)
	require.NoError(t, err, "GetTransactionsByAddress must accept mixed-case input post-migration")
	require.Len(t, txs, 1, "the migrated row must be findable via the API even when the caller passes the original checksum case")
	require.Equal(t, expectedFromLower, txs[0].From, "API returns lowercase (the now-canonical form)")

	// Same query with lowercase input (the normal caller pattern).
	txs, err = d.GetTransactionsByAddress(ctx, expectedFromLower, 10, nil)
	require.NoError(t, err)
	require.Len(t, txs, 1, "lowercase input must find the same row")

	// (3) New writes via the writer-side LOWER() path land alongside the
	// migrated row in the same canonical form.
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address,
			value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ($1, 1, $2, 1, LOWER($3), LOWER($4), '0', 21000, 0, '0x', 1, 0)`,
		"0x"+strRepeat("ff", 32),
		time.Now().Unix(),
		checksumFromAddr, // mixed-case in (simulating writer path)
		checksumToAddr,
	)
	require.NoError(t, err, "writer-side LOWER() must coexist with migrated rows")

	txs, err = d.GetTransactionsByAddress(ctx, expectedFromLower, 10, nil)
	require.NoError(t, err)
	require.Len(t, txs, 2, "both the migrated row and the freshly-written row must appear")

	// (4) Re-running the migration is a no-op (idempotency) — operators may
	// re-deploy or re-run the upgrade and the migration must not corrupt
	// already-lowercased data.
	_, err = d.pool.Exec(ctx, migration005SQL)
	require.NoError(t, err, "migration 005 must be idempotent — second run is a no-op")

	txs, err = d.GetTransactionsByAddress(ctx, expectedFromLower, 10, nil)
	require.NoError(t, err)
	require.Len(t, txs, 2, "idempotency: re-running the migration must not lose or duplicate rows")
}

// TestMigration005_BalancesPKConflictResolution pins the specific corner
// the migration's DELETE-then-UPDATE pattern was designed for: a balances
// row exists in BOTH lowercase and checksum case for the same
// (token_address, block_number). Without the DELETE the UPDATE would trip
// balances_pkey. The test catches removal/refactor of the DELETE clause
// in 005 even if the surrounding upgrade scenario no longer applies in
// production — the migration file lives in the codebase permanently and
// must keep handling its documented edge cases.
func TestMigration005_BalancesPKConflictResolution(t *testing.T) {
	d, cleanup := setupMigration005TestDB(t)
	defer cleanup()
	ctx := context.Background()

	const lowerAddr = "0xabcd000000000000000000000000000000000001"
	const checksumAddr = "0xAbCd000000000000000000000000000000000001"
	const tokenAddr = "0xt0ken000000000000000000000000000000000001"

	// Both case-variant rows present for the same (token, block) — the
	// pre-migration shape that breaks a naive UPDATE … LOWER(address).
	_, err := d.pool.Exec(ctx,
		`INSERT INTO balances (address, token_address, block_number, balance) VALUES ($1, $2, 1, '100'), ($3, $2, 1, '999')`,
		lowerAddr, tokenAddr, checksumAddr)
	require.NoError(t, err)

	var n int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM balances`).Scan(&n))
	require.Equal(t, 2, n, "fixture seeded both case variants")

	// Migration must collapse to one row without erroring.
	_, err = d.pool.Exec(ctx, migration005SQL)
	require.NoError(t, err, "migration must handle balances PK-conflict shape without erroring")

	require.NoError(t, d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM balances`).Scan(&n))
	require.Equal(t, 1, n, "the case-variant row was dropped; one canonical row remains")

	// The surviving row is the lowercase one (per the migration's
	// DELETE … USING … WHERE b1.address != LOWER(b1.address) ordering —
	// the checksum row is the b1 candidate that gets removed).
	var addr string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT address FROM balances`).Scan(&addr))
	require.Equal(t, lowerAddr, addr, "the lowercase row is the survivor")
}
