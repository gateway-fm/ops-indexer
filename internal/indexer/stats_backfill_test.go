package indexer

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

const backfillStart = 1_200

var testBackfill = StatsBackfillConfig{RPS: 1_000_000, Workers: 4, BatchBlocks: 70, SpanBlocks: 500, Retry: time.Millisecond}

// counterRows is every counter-backed row the read API serves from, as text, in a stable order.
func counterRows(t *testing.T, sql *pgxpool.Pool) []string {
	t.Helper()
	rows, err := sql.Query(context.Background(), `
		SELECT 'chain ' || name || ' ' || count FROM chain_counters
		UNION ALL
		SELECT 'token ' || address || ' ' || transfer_count || ' ' || COALESCE(total_supply::TEXT, '-') || ' ' || block_number
		       || ' ' || COALESCE(creation_tx, '-') || ' ' || token_type FROM tokens
		UNION ALL
		SELECT 'address ' || address || ' ' || tx_count || ' ' || sent_count || ' ' || internal_tx_count || ' '
		       || token_transfer_count || ' ' || first_seen || ' ' || last_seen || ' ' || is_contract FROM address_stats
		UNION ALL
		SELECT 'contract ' || address || ' ' || block_number FROM contracts
		UNION ALL
		SELECT 'nft ' || token_address || ' ' || token_id || ' ' || owner FROM nft_tokens
		ORDER BY 1`)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

func indexBlocks(t *testing.T, idx *Indexer, from, to uint64) {
	t.Helper()
	for n := from; n <= to; n++ {
		require.NoError(t, idx.processBlock(context.Background(), n))
	}
}

func fullSync(t *testing.T, head uint64) []string {
	t.Helper()
	store, sql := setupPostgres(t)
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: head, busy: head + 1}
	indexBlocks(t, newTestIndexer(store, chain), 0, head)
	return counterRows(t, sql)
}

// backfilled indexes [backfillStart, head] with the head path while the backfill counts [0, backfillStart),
// resuming in a fresh indexer half way; sabotage runs on each batch before it is committed.
func backfilled(t *testing.T, head uint64, sabotage func(*db.BackfillBatch)) (*db.DB, *pgxpool.Pool, *fakeChain) {
	t.Helper()
	ctx := context.Background()
	store, sql := setupPostgres(t)
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: head, busy: head + 1, failOnce: map[uint64]bool{400: true}}
	require.NoError(t, store.InitStatsBackfill(ctx, backfillStart-1, uint64(blockTime(backfillStart-1))))
	head1 := newTestIndexer(store, chain)
	indexBlocks(t, head1, backfillStart, backfillStart+300)

	b := head1.newBackfiller(sabotaged{store, sabotage}, testBackfill)
	failures := 0
	for steps := 0; steps < 8; {
		_, err := b.step(ctx)
		if err != nil {
			failures++
			require.Less(t, failures, 3, "the injected failure is retried: %v", err)
			continue
		}
		steps++
	}
	indexBlocks(t, head1, backfillStart+301, head-100)

	fresh := newTestIndexer(store, chain).newBackfiller(sabotaged{store, sabotage}, testBackfill)
	for {
		done, err := fresh.step(ctx)
		require.NoError(t, err)
		if done {
			break
		}
	}
	indexBlocks(t, newTestIndexer(store, chain), head-99, head)
	return store, sql, chain
}

type sabotaged struct {
	*db.DB
	fn func(*db.BackfillBatch)
}

func (s sabotaged) CommitBackfillBatch(ctx context.Context, b *db.BackfillBatch) error {
	if s.fn != nil {
		s.fn(b)
	}
	return s.DB.CommitBackfillBatch(ctx, b)
}

func TestStatsBackfillEqualsAFullSyncFromGenesis(t *testing.T) {
	const head = 2_000
	want := fullSync(t, head)
	store, sql, chain := backfilled(t, head, nil)
	ctx := context.Background()

	require.Equal(t, want, counterRows(t, sql), "every counter-backed row equals a full sync from genesis")
	s, err := store.StatsBackfillState(ctx)
	require.NoError(t, err)
	require.Equal(t, db.BackfillState{Exists: true, Complete: true, Target: backfillStart - 1, Through: backfillStart - 1}, s)

	var spans []string
	rows, err := sql.Query(ctx, `SELECT lo || '-' || hi || ' ' || source FROM ledger_spans ORDER BY lo`)
	require.NoError(t, err)
	for rows.Next() {
		var sp string
		require.NoError(t, rows.Scan(&sp))
		spans = append(spans, sp)
	}
	rows.Close()
	require.Equal(t, []string{"0-500 node", "500-1000 node", "1000-1200 node"}, spans, "the grid, cut at START_BLOCK")

	for b := 0; b < 256; b++ {
		drifts, err := store.CheckRunning(ctx, b, false)
		require.NoError(t, err)
		require.Empty(t, drifts, "running counters = ledger rows + open rows (bucket %d)", b)
	}
	counter := newTestIndexer(store, chain).newSpanCounter(1_000_000)
	for range 3 {
		c, ok, err := newTestIndexer(store, chain).RecountPrunedSpan(ctx, store, counter, false)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "ok", c.State, "the node recount of a backfilled span agrees with it: [%d, %d)", c.Lo, c.Hi)
	}
	_, err = sql.Exec(ctx, `UPDATE ledger_verify_state SET value = 1 WHERE name = 'running_cycles'`)
	require.NoError(t, err)
	checks, err := newTestIndexer(store, chain).newChainChecker(store, ChainCheckConfig{RPS: 1_000_000, Sample: 50}).pass(ctx)
	require.NoError(t, err)
	require.Equal(t, ChainCheckResult{BlocksApplicable: true, SupplyChecked: 1, NoncesChecked: 1, SpanChecked: true}, checks,
		"blocks counted from genesis = head + 1, supply and the sender's nonce agree with the node")

	var keys int64
	require.NoError(t, sql.QueryRow(ctx, `SELECT count(*) FROM stats_backfill_balance_keys`).Scan(&keys))
	require.Equal(t, int64(2), keys, "one balance key per (token, address) touched below START_BLOCK, not one per transfer")
}

func TestStatsBackfillThatDropsABlocksAddressDeltasFails(t *testing.T) {
	const head = 2_000
	want := fullSync(t, head)
	dropped := false
	store, sql, _ := backfilled(t, head, func(b *db.BackfillBatch) {
		if b.From <= 700 && 700 < b.To && !dropped {
			dropped = true
			delete(b.Shared.AddressStats, strings.ToLower(chainRecipient.Hex()))
		}
	})
	require.True(t, dropped)
	require.NotEqual(t, want, counterRows(t, sql), "the counters differ from a full sync")
	var found int
	for b := 0; b < 256; b++ {
		drifts, err := store.CheckRunning(context.Background(), b, false)
		require.NoError(t, err)
		found += len(drifts)
	}
	require.NotZero(t, found, "the running check finds the dropped deltas")
}

func TestStatsBackfillRefusesABatchTwice(t *testing.T) {
	store, _ := setupPostgres(t)
	ctx := context.Background()
	require.NoError(t, store.InitStatsBackfill(ctx, 99, 1))
	batch := func() *db.BackfillBatch {
		return &db.BackfillBatch{From: 0, To: 10, Shared: &db.BlockData{AddressStats: map[string]*db.AddressStatsDelta{}},
			Spans: map[uint64]*db.SpanTotals{0: db.NewSpanTotals()}, SpanHi: map[uint64]uint64{0: 100}}
	}
	require.NoError(t, store.CommitBackfillBatch(ctx, batch()))
	require.ErrorIs(t, store.CommitBackfillBatch(ctx, batch()), db.ErrBackfillNotNext, "a batch applied twice would count twice")
}

// Block n is stamped 1_700_000_000 + n: blocks 0..6,399 are on 2023-11-14, 6,400..92,799 on 2023-11-15.
func TestPrepareStatsBackfillNeedsTheRangeAndItsDayFinal(t *testing.T) {
	store, _ := setupPostgres(t)
	ctx := context.Background()
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 20_000}

	require.ErrorContains(t, PrepareStatsBackfill(ctx, store, chain, 10_000), "within the reorg floor")
	require.ErrorContains(t, PrepareStatsBackfill(ctx, store, chain, 7_000), "whole day is final",
		"the floor (block 7,200) is on 2023-11-15 too")
	require.NoError(t, PrepareStatsBackfill(ctx, store, chain, 6_000))
	s, err := store.StatsBackfillState(ctx)
	require.NoError(t, err)
	require.Equal(t, db.BackfillState{Exists: true, Target: 5_999, Through: -1}, s)
	wm, ts, err := store.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Equal(t, [2]int64{5_999, 1_700_005_999}, [2]int64{int64(wm), ts}, "pruned from day one")
	require.NoError(t, PrepareStatsBackfill(ctx, store, chain, 6_000), "a prepared backfill is resumed, not restarted")
}

func TestStatsBackfillReadsEachTouchedBalanceOnce(t *testing.T) {
	ctx := context.Background()
	store, sql := setupPostgres(t)
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 2_000, busy: 2_001,
		balances: map[string]*big.Int{chainRecipient.Hex(): big.NewInt(7007)}}
	require.NoError(t, store.InitStatsBackfill(ctx, backfillStart-1, uint64(blockTime(backfillStart-1))))
	idx := newTestIndexer(store, chain)
	indexBlocks(t, idx, backfillStart, 1500)
	require.NoError(t, store.InsertBalancesBatch(ctx, []*types.Balance{{Address: strings.ToLower(chainSender.Hex()),
		TokenAddress: strings.ToLower(chainToken.Hex()), BlockNumber: 1500, Balance: "9"}}), "the head path's balance worker")

	b := idx.newBackfiller(store, testBackfill)
	var err error
	for done := false; !done; {
		done, err = b.step(ctx)
		require.NoError(t, err)
	}
	read, err := b.readBalances(ctx, balanceKeysPerRead)
	require.NoError(t, err)
	require.Equal(t, 2, read)
	require.Equal(t, 2, chain.balanceReads, "one balanceOf per key, not one per transfer (600 below START_BLOCK)")
	read, err = b.readBalances(ctx, balanceKeysPerRead)
	require.NoError(t, err)
	require.Zero(t, read)

	rows, err := sql.Query(ctx, `SELECT address || ' ' || balance || ' ' || block_number FROM token_balances_current ORDER BY address`)
	require.NoError(t, err)
	var got []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		got = append(got, s)
	}
	rows.Close()
	require.Equal(t, []string{
		"0x00000000000000000000000000000000000000a1 9 1500",
		"0x00000000000000000000000000000000000000b2 7007 1198",
	}, got, "the recipient's balance at latest, at its newest backfill block; the sender's head-path row (block 1,500) wins")
	var holders int64
	require.NoError(t, sql.QueryRow(ctx, `SELECT holder_count FROM tokens`).Scan(&holders))
	require.Equal(t, int64(2), holders, "holder_count follows the current balances")
}
