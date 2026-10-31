package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// The ledger's lookup indexes are not built by the migrations: the span rebase and the verification wait for them, and
// EnsureLedgerIndexes builds them, replacing one a stopped build left invalid.
func TestTheLedgerIndexesAreBuiltAfterStartupAndTheirReadersWaitForThem(t *testing.T) {
	pool, _ := emptyTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	d := &DB{pool: pool}
	require.NoError(t, d.MigrateSchema())
	require.NoError(t, d.Reconcile(ctx))
	ready, err := d.LedgerIndexesReady(ctx)
	require.NoError(t, err)
	require.False(t, ready, "the migrations build neither index")

	for n := 1; n <= 150; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xempty' || g, '0xempty' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(151, 13000) g`)
	require.NoError(t, err)
	cfg := LedgerConfig{SpanBlocks: 100, FirstBlock: 1, WorkMem: "64MB"}
	closed, err := d.CloseLedgerSpans(ctx, cfg, 1)
	require.NoError(t, err)
	require.Equal(t, 1, closed, "spans close without the indexes")
	_, err = d.CloseLedgerSpans(ctx, cfg, 1)
	require.NoError(t, err)
	unrebased := func() int {
		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ledger_spans WHERE rebased_at IS NULL`).Scan(&n))
		return n
	}
	require.Equal(t, 2, unrebased(), "the span rebase waits for the indexes")

	verifyCtx, stopVerify := context.WithCancel(ctx)
	verified := make(chan struct{})
	go func() {
		defer close(verified)
		d.RunVerify(verifyCtx, VerifyConfig{RecountInterval: 50 * time.Millisecond, WorkMem: "64MB"})
	}()
	time.Sleep(500 * time.Millisecond)
	checked := func() int {
		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ledger_spans WHERE checked_at IS NOT NULL`).Scan(&n))
		return n
	}
	require.Zero(t, checked(), "the verification waits for the indexes")

	require.NoError(t, d.EnsureLedgerIndexes(ctx))
	ready, err = d.LedgerIndexesReady(ctx)
	require.NoError(t, err)
	require.True(t, ready)
	require.Eventually(t, func() bool { return checked() > 0 }, 20*time.Second, 50*time.Millisecond,
		"the verification starts once they are built")
	stopVerify()
	<-verified
	_, err = d.CloseLedgerSpans(ctx, cfg, 1)
	require.NoError(t, err)
	require.LessOrEqual(t, unrebased(), 1, "the waiting spans are rebased")

	_, err = pool.Exec(ctx, `UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'ledger_address_address'::regclass`)
	require.NoError(t, err, "a build stopped part way")
	ready, err = d.LedgerIndexesReady(ctx)
	require.NoError(t, err)
	require.False(t, ready)
	require.NoError(t, d.EnsureLedgerIndexes(ctx))
	ready, err = d.LedgerIndexesReady(ctx)
	require.NoError(t, err)
	require.True(t, ready, "the invalid index is replaced")
}

// indexer_ledger_indexes_ready follows the indexes: 0 while one is missing, 1 once they are built.
func TestTheLedgerIndexesReadyGaugeFollowsTheIndexes(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	gauge := func() float64 {
		families, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, f := range families {
			if f.GetName() == "indexer_ledger_indexes_ready" {
				return f.GetMetric()[0].GetGauge().GetValue()
			}
		}
		return -1
	}
	_, err := d.pool.Exec(ctx, `DROP INDEX ledger_address_address`)
	require.NoError(t, err)
	ready, err := d.LedgerIndexesReady(ctx)
	require.NoError(t, err)
	require.False(t, ready)
	require.Equal(t, float64(0), gauge())
	require.NoError(t, d.EnsureLedgerIndexes(ctx))
	require.Equal(t, float64(1), gauge())
}

// A shutdown during a ledger index build stops it: the build returns, the pool closes at once, and the index the build
// left invalid is replaced by the next start's.
func TestAShutdownDuringALedgerIndexBuildClosesThePoolAndTheNextStartRebuildsIt(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	connString := d.pool.Config().ConnString()
	_, err := d.pool.Exec(ctx, `DROP INDEX ledger_address_address`)
	require.NoError(t, err)

	writer, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer writer.Close(ctx)
	held, err := writer.Begin(ctx)
	require.NoError(t, err)
	_, err = held.Exec(ctx, `LOCK TABLE ledger_address IN ROW EXCLUSIVE MODE`)
	require.NoError(t, err, "a writer the concurrent build must wait out")

	runCtx, shutdown := context.WithCancel(ctx)
	built := make(chan struct{})
	go func() {
		defer close(built)
		d.RunLedgerIndexes(runCtx, time.Minute)
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		require.NoError(t, writer.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'ledger_address_address')`).
			Scan(&waiting))
		return waiting
	}, 20*time.Second, 50*time.Millisecond, "the build has started and waits for the writer")

	shutdown()
	select {
	case <-built:
	case <-time.After(5 * time.Second):
		t.Fatal("the build did not return on shutdown")
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		d.Close()
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the pool did not close while the build's writer was still open")
	}

	var valid bool
	require.NoError(t, held.QueryRow(ctx, `
		SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'ledger_address_address'`).
		Scan(&valid))
	require.False(t, valid, "the stopped build left its index invalid")
	require.NoError(t, held.Rollback(ctx))

	pool, err := pgxpool.New(ctx, connString)
	require.NoError(t, err)
	defer pool.Close()
	next := &DB{pool: pool}
	require.NoError(t, next.EnsureLedgerIndexes(ctx))
	ready, err := next.LedgerIndexesReady(ctx)
	require.NoError(t, err)
	require.True(t, ready, "the next start replaces the invalid index")
}
