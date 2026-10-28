package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// The history-DDL gate is held while the statements under it run. With pool_max_conns=1 and COMMIT_PIPELINE=1,
// which Validate accepts, the gate must not take the pool's only connection: the startup Reconcile releases an
// abandoned cut under it, and its reads would wait for a second connection for good.
func TestStartupReconcileRunsOnAPoolOfOne(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	cfg := d.pool.Config().Copy()
	cfg.MaxConns = 1
	one, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer one.Close()
	small := &DB{pool: one, HiddenTxTypes: []int{}}

	runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	require.NoError(t, small.Reconcile(runCtx), "Reconcile with a pool of one connection")
}
