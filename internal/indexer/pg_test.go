package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/gateway-fm/chain-indexer/internal/db"
)

// setupPostgres starts a migrated database; sql is a second pool for the test's own statements.
func setupPostgres(t *testing.T) (store *db.DB, sql *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	c, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"), postgres.WithUsername("testuser"), postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(30*time.Second)))
	if err != nil {
		t.Skipf("skipping: could not start postgres container (is Docker running?): %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	store, err = db.New(url)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	require.NoError(t, store.Migrate())
	sql, err = pgxpool.New(ctx, url)
	require.NoError(t, err)
	t.Cleanup(sql.Close)
	return store, sql
}
