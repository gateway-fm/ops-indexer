package db

import (
	"context"
	"io/fs"
	"testing"

	"github.com/jackc/tern/v2/migrate"
)

func TestMigration009WidensIngestCounters(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	for _, col := range []struct{ table, column string }{
		{"tokens", "holder_count"},
		{"tokens", "transfer_count"},
		{"address_stats", "tx_count"},
		{"address_stats", "internal_tx_count"},
		{"address_stats", "token_transfer_count"},
	} {
		var dataType string
		if err := d.pool.QueryRow(ctx,
			`SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
			col.table, col.column).Scan(&dataType); err != nil {
			t.Fatalf("%s.%s: %v", col.table, col.column, err)
		}
		if dataType != "bigint" {
			t.Errorf("%s.%s is %s, want bigint", col.table, col.column, dataType)
		}
	}

	if _, err := d.pool.Exec(ctx,
		`INSERT INTO tokens (address, symbol, token_type, block_number, transfer_count) VALUES ('0x01', 'T', 'ERC20', 1, 2147483647)`); err != nil {
		t.Fatalf("seed token: %v", err)
	}
	var after int64
	if err := d.pool.QueryRow(ctx,
		`UPDATE tokens SET transfer_count = transfer_count + 1 WHERE address = '0x01' RETURNING transfer_count`).Scan(&after); err != nil {
		t.Fatalf("increment past the INT ceiling: %v", err)
	}
	if after != 2147483648 {
		t.Fatalf("transfer_count %d, want 2147483648", after)
	}
}

func TestMigration009RollsBackToInt(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	migrator, err := migrate.NewMigrator(ctx, conn.Conn(), "schema_version")
	if err != nil {
		t.Fatal(err)
	}
	migrationsFS, err := fs.Sub(migrations, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err := migrator.LoadMigrations(migrationsFS); err != nil {
		t.Fatal(err)
	}

	if err := migrator.MigrateTo(ctx, 8); err != nil {
		t.Fatalf("migrate down to 8: %v", err)
	}
	var dataType string
	if err := d.pool.QueryRow(ctx,
		`SELECT data_type FROM information_schema.columns WHERE table_name = 'address_stats' AND column_name = 'tx_count'`).Scan(&dataType); err != nil {
		t.Fatal(err)
	}
	if dataType != "integer" {
		t.Fatalf("address_stats.tx_count is %s after rollback, want integer", dataType)
	}

	if err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("migrate back up: %v", err)
	}
}
