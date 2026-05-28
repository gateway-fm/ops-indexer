package db

import (
	"context"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupLowercaseTestDB stands up a testcontainer-backed DB for the
// writer-side normalisation tests. Mirrors the bench setup but uses
// testing.T (the bench version is testing.B).
func setupLowercaseTestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	ctx := context.Background()

	pgC, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("lcdb"),
		postgres.WithUsername("lcuser"),
		postgres.WithPassword("lcpass"),
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

// EIP-55 checksum-cased input addresses to make absolutely sure the
// writer-side LOWER() is what's normalising — without it the column
// would store these as-is and the lowercase assertions would fail.
const (
	checksumFromAddr  = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
	checksumToAddr    = "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
	checksumTokenAddr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
	expectedFromLower = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
	expectedToLower   = "0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc"
	expectedTokenLow  = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
)

// TestWriter_LowercasesTransactionAddresses pins the contract for
// InsertTransaction (single-row path): from_address / to_address must be
// stored lowercased regardless of input casing. Combined with the
// 005_lowercase_addresses migration this is what lets the read-side
// LOWER() patches be reverted in a follow-up — the data layer becomes
// canonical, plain equality queries hit the btree indexes again.
func TestWriter_LowercasesTransactionAddresses(t *testing.T) {
	d, cleanup := setupLowercaseTestDB(t)
	defer cleanup()
	ctx := context.Background()

	// Need a block row first — transactions.block_number FKs are not enforced
	// in this schema but block_timestamp denormalisation expects the parent.
	_, err := d.pool.Exec(ctx,
		"INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count) VALUES (1, '0xb', '0xp', $1, 1, 1, 1)",
		time.Now().Unix())
	require.NoError(t, err)

	tx := &types.Transaction{
		Hash:           "0x" + strRepeat("aa", 32),
		BlockNumber:    1,
		BlockTimestamp: uint64(time.Now().Unix()),
		TxIndex:        0,
		From:           checksumFromAddr,
		To:             ptrString(checksumToAddr),
		Value:          "0",
		GasUsed:        21000,
		GasPrice:       20_000_000_000,
		TxType:         0,
		InputData:      "0x",
		Status:         1,
	}
	require.NoError(t, d.InsertTransaction(ctx, tx))

	var fromStored, toStored string
	require.NoError(t, d.pool.QueryRow(ctx,
		"SELECT from_address, to_address FROM transactions WHERE hash = $1",
		tx.Hash).Scan(&fromStored, &toStored))

	if fromStored != expectedFromLower {
		t.Errorf("from_address stored as %q, want lowercased %q (writer-side LOWER() not applied)", fromStored, expectedFromLower)
	}
	if toStored != expectedToLower {
		t.Errorf("to_address stored as %q, want lowercased %q (writer-side LOWER() not applied)", toStored, expectedToLower)
	}
}

// TestWriter_LowercasesLogAndTransferAddresses covers the parallel
// normalisation for logs.address, token_transfers.{token_address,
// from_address, to_address}, internal_transactions.{from_address,
// to_address}. Single-row INSERT path; the batch path is exercised by
// the same SQL strings so this is sufficient to pin the contract.
func TestWriter_LowercasesLogAndTransferAddresses(t *testing.T) {
	d, cleanup := setupLowercaseTestDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := d.pool.Exec(ctx,
		"INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count) VALUES (1, '0xb', '0xp', $1, 1, 1, 1)",
		time.Now().Unix())
	require.NoError(t, err)

	// logs.tx_hash and token_transfers.tx_hash both FK to transactions.hash —
	// stub a parent tx so the inserts succeed.
	parentTxHash := "0x" + strRepeat("cc", 32)
	_, err = d.pool.Exec(ctx,
		"INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type) VALUES ($1, 1, 0, '0x0000000000000000000000000000000000000000', '0x0000000000000000000000000000000000000000', '0', 0, 0, '0x', 1, 0)",
		parentTxHash)
	require.NoError(t, err)

	ts := uint64(time.Now().Unix())
	logEntry := &types.Log{
		TxHash:      "0x" + strRepeat("cc", 32),
		LogIndex:    0,
		Address:     checksumFromAddr,
		BlockNumber: 1,
		Timestamp:   &ts,
	}
	require.NoError(t, d.InsertLog(ctx, logEntry))

	var logAddr string
	require.NoError(t, d.pool.QueryRow(ctx,
		"SELECT address FROM logs WHERE tx_hash = $1 AND log_index = 0",
		logEntry.TxHash).Scan(&logAddr))
	if logAddr != expectedFromLower {
		t.Errorf("logs.address stored as %q, want %q", logAddr, expectedFromLower)
	}

	transfer := &types.TokenTransfer{
		TxHash:       "0x" + strRepeat("cc", 32),
		LogIndex:     1,
		TokenAddress: checksumTokenAddr,
		From:         checksumFromAddr,
		To:           checksumToAddr,
		Value:        "100",
		BlockNumber:  1,
		Timestamp:    &ts,
		TransferType: "erc20",
	}
	require.NoError(t, d.InsertTokenTransfer(ctx, transfer))

	var tt struct {
		token, from, to string
	}
	require.NoError(t, d.pool.QueryRow(ctx,
		"SELECT token_address, from_address, to_address FROM token_transfers WHERE tx_hash = $1 AND log_index = 1",
		transfer.TxHash).Scan(&tt.token, &tt.from, &tt.to))
	if tt.token != expectedTokenLow {
		t.Errorf("token_transfers.token_address: %q, want %q", tt.token, expectedTokenLow)
	}
	if tt.from != expectedFromLower {
		t.Errorf("token_transfers.from_address: %q, want %q", tt.from, expectedFromLower)
	}
	if tt.to != expectedToLower {
		t.Errorf("token_transfers.to_address: %q, want %q", tt.to, expectedToLower)
	}

	gas := uint64(21000)
	itx := &types.InternalTransaction{
		TxHash:       "0x" + strRepeat("cc", 32),
		BlockNumber:  1,
		TraceAddress: "0",
		From:         checksumFromAddr,
		To:           ptrString(checksumToAddr),
		Value:        "0",
		Gas:          &gas,
		CallType:     "CALL",
		Timestamp:    &ts,
	}
	require.NoError(t, d.InsertInternalTransaction(ctx, itx))

	var iFrom, iTo string
	require.NoError(t, d.pool.QueryRow(ctx,
		"SELECT from_address, to_address FROM internal_transactions WHERE tx_hash = $1 AND trace_address = '0'",
		itx.TxHash).Scan(&iFrom, &iTo))
	if iFrom != expectedFromLower {
		t.Errorf("internal_transactions.from_address: %q, want %q", iFrom, expectedFromLower)
	}
	if iTo != expectedToLower {
		t.Errorf("internal_transactions.to_address: %q, want %q", iTo, expectedToLower)
	}
}

// strRepeat avoids pulling in strings just for the helper.
func strRepeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

func ptrString(v string) *string { return &v }
