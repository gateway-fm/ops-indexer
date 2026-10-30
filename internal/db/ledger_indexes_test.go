package db

import (
	"context"
	"testing"
	"time"

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
