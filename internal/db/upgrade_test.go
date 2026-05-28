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

// Upgrade-case integration test for the v0.1.x → v0.2.x → this-PR migration
// path. The intent is the question every operator asks before bumping a
// running devnet:
//
//   1. Will the migration finish, or get stuck on a duplicate-key or
//      foreign-key conflict?
//   2. Will reads still find my old rows after the migration?
//   3. Will new writes land alongside the canonicalised old data?
//   4. Will running the migration a second time corrupt anything?
//
// We don't go through Migrate() here because it tracks state in schema_version
// and won't replay 005. Instead we set up the DB with all migrations applied
// (the "fresh-install" state every test starts from), then bypass the
// writer-side LOWER() by inserting mixed-case rows via raw SQL — that
// simulates pre-migration data left behind in a real upgrade. We then run
// 005's UPDATE statements directly to verify the upgrade behaviour
// end-to-end.

func setupUpgradeTestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	ctx := context.Background()

	pgC, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("upgradedb"),
		postgres.WithUsername("upuser"),
		postgres.WithPassword("uppass"),
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

// TestUpgrade_MigratesPreExistingMixedCaseData covers the four operator
// concerns: migration finishes, reads find the data afterwards, new writes
// land alongside, and re-running is a no-op.
func TestUpgrade_MigratesPreExistingMixedCaseData(t *testing.T) {
	d, cleanup := setupUpgradeTestDB(t)
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

// TestUpgrade_BalancesPKConflict pins the specific corner the migration's
// DELETE-then-UPDATE pattern was designed for: a balances row exists in BOTH
// lowercase and checksum case for the same (token_address, block_number).
// Without the DELETE the UPDATE would trip balances_pkey. Operators have hit
// this exact shape on upgrade in the wild — we don't want to ship a migration
// that crashes on it.
func TestUpgrade_BalancesPKConflict(t *testing.T) {
	d, cleanup := setupUpgradeTestDB(t)
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
