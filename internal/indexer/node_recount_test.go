package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
)

// newTestIndexer runs no daily_stats advance of its own: tests advance it where production would.
func newTestIndexer(store Database, chain RPCClient) *Indexer {
	idx := NewWithConfig(store, chain, time.Second, 0, &Config{
		RPCWorkers: 4, RPCRateLimit: 1000, TokenMetadataWorkers: 2, TraceWorkers: 2, TraceRateLimit: 1000,
		DailyStatsMinInterval: time.Hour,
	})
	idx.dailyStats = nil
	return idx
}

// ingestChain writes [0, busy) through the head path and the empty blocks up to head directly, then closes every
// span below the reorg floor.
func ingestChain(t *testing.T, idx *Indexer, store *db.DB, sql *pgxpool.Pool, chain *fakeChain) {
	t.Helper()
	ctx := context.Background()
	for n := uint64(0); n < chain.busy; n++ {
		require.NoError(t, idx.processBlock(ctx, n))
	}
	_, err := sql.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT b, 'filler' || b, 'filler' || (b - 1), 1700000000 + 60 * b + b % 7, 0, 30000000, 0 FROM generate_series($1::bigint, $2::bigint) b`,
		chain.busy, chain.head)
	require.NoError(t, err)
	_, err = store.CloseLedgerSpans(ctx, db.LedgerConfig{SpanBlocks: 1000}, 1_000)
	require.NoError(t, err)
}

func TestNodeRecountSettlesAPrunedSpanAndRepairsItsRows(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx := context.Background()
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 14_000, busy: 1_000}
	idx := newTestIndexer(store, chain)
	ingestChain(t, idx, store, sql, chain)
	_, err := sql.Exec(ctx, `UPDATE prune_state SET pruned_through = 999`)
	require.NoError(t, err)
	counter := idx.newSpanCounter(1_000_000)

	c, ok, err := idx.RecountPrunedSpan(ctx, store, counter, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, db.SpanCheck{Lo: 0, Hi: 1000, State: "ok"}, c,
		"counted from the node with the writer's own builder, the span equals what was counted from its rows")

	read := func() (txs, sent, transfers int64) {
		require.NoError(t, sql.QueryRow(ctx, `
			SELECT (SELECT value FROM ledger_chain WHERE lo = 0 AND name = 'transactions_total'),
			       (SELECT sent FROM ledger_address WHERE lo = 0 AND address = lower($1)),
			       (SELECT transfers FROM ledger_token WHERE lo = 0 AND token = lower($2))`,
			chainSender.Hex(), chainToken.Hex()).Scan(&txs, &sent, &transfers))
		return
	}
	want0, want1, want2 := read()
	require.Equal(t, [3]int64{1000, 1000, 600}, [3]int64{want0, want1, want2}, "1,000 txs all sent by the sender; 500 mints + 100 self-transfers")

	_, err = sql.Exec(ctx, `
		UPDATE ledger_chain SET value = value + 1 WHERE lo = 0 AND name = 'transactions_total';
		UPDATE ledger_address SET sent = sent - 1 WHERE lo = 0 AND address = '0x00000000000000000000000000000000000000a1';
		UPDATE ledger_token SET supply = supply + 1 WHERE lo = 0;
		UPDATE ledger_spans SET checked_at = NULL WHERE lo = 0`)
	require.NoError(t, err)
	c, _, err = idx.RecountPrunedSpan(ctx, store, counter, true)
	require.NoError(t, err)
	require.Equal(t, db.SpanCheck{Lo: 0, Hi: 1000, Drifted: 3, State: "repaired"}, c, "each pruned row off by one is found")
	g0, g1, g2 := read()
	require.Equal(t, [3]int64{want0, want1, want2}, [3]int64{g0, g1, g2}, "and replaced from the node")

	calls := chain.calls
	_, _, err = idx.RecountPrunedSpan(ctx, store, counter, true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, chain.calls-calls, 2*1000, "a recount reads every block of the span and its receipts")
}

func TestNodeRecountRateLimitsTheNode(t *testing.T) {
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 100, busy: 100}
	idx := newTestIndexer(new(MockDatabase), chain)
	counter := idx.newSpanCounter(200)
	start := time.Now()
	_, err := counter.count(context.Background(), 0, 50)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 450*time.Millisecond, "100 block and receipt reads at 200/s")
}
