package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 025 gives up its lock while a reader holds address_stats; the startup migration waits and runs it again, and
// reaches the latest version once the reader is done.
func TestMigrateSchemaRetriesAMigrationThatGaveUpALock(t *testing.T) {
	pool, _ := emptyTestDB(t)
	ctx := context.Background()
	migrateTo(t, pool, 24)
	migrateLockRetryWait, migrateLockRetryMaxWait = 200*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() { migrateLockRetryWait, migrateLockRetryMaxWait = 2*time.Second, 30*time.Second })

	holder, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = holder.Exec(ctx, `SELECT count(*) FROM address_stats`)
	require.NoError(t, err)
	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(3 * time.Second)
		_ = holder.Rollback(ctx)
	}()
	defer func() { <-released }()

	start := time.Now()
	require.NoError(t, (&DB{pool: pool}).MigrateSchema())
	require.Greater(t, time.Since(start), 2*time.Second, "it waited for the reader")
	var hasColumn bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_name = 'address_stats' AND column_name = 'sent_count')`).Scan(&hasColumn))
	require.True(t, hasColumn)
}
