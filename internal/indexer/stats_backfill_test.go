package indexer

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
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

func fullSync(t *testing.T, head uint64) (counters, days []string) {
	t.Helper()
	store, sql := setupPostgres(t)
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: head, busy: head + 1}
	indexBlocks(t, newTestIndexer(store, chain), 0, head)
	advanceDailyStats(t, store, sql, -1)
	return counterRows(t, sql), backfillDailyRows(t, sql)
}

// advanceDailyStats starts the incremental aggregate after block from (if it has not started) and advances it to
// the stored head.
func advanceDailyStats(t *testing.T, store *db.DB, sql *pgxpool.Pool, from int64) {
	t.Helper()
	ctx := context.Background()
	_, err := sql.Exec(ctx, `INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, $1) ON CONFLICT (id) DO NOTHING`, from)
	require.NoError(t, err)
	for {
		before, err := store.AdvanceDailyStats(ctx)
		require.NoError(t, err)
		after, err := store.AdvanceDailyStats(ctx)
		require.NoError(t, err)
		if after == before {
			return
		}
	}
}

func backfillDailyRows(t *testing.T, sql *pgxpool.Pool) []string {
	t.Helper()
	rows, err := sql.Query(context.Background(), `
		SELECT concat_ws(' ', date, total_blocks, total_transactions, total_gas_used, avg_gas_price, successful_txs, failed_txs,
		       active_addresses, new_addresses, avg_block_time, avg_block_size, new_contracts, token_transfer_count,
		       cumulative_transactions, cumulative_addresses, cumulative_contracts)
		FROM daily_stats ORDER BY date`)
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
	advanceDailyStats(t, store, sql, 1_546)

	fresh := newTestIndexer(store, chain).newBackfiller(sabotaged{store, sabotage}, testBackfill)
	for {
		done, err := fresh.step(ctx)
		require.NoError(t, err)
		if done {
			break
		}
	}
	indexBlocks(t, newTestIndexer(store, chain), head-99, head)
	advanceDailyStats(t, store, sql, 1_546)
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
	want, wantDays := fullSync(t, head)
	store, sql, chain := backfilled(t, head, nil)
	ctx := context.Background()

	require.Equal(t, want, counterRows(t, sql), "every counter-backed row equals a full sync from genesis")
	require.Len(t, wantDays, 3)
	require.Equal(t, wantDays, backfillDailyRows(t, sql),
		"daily_stats too: a day before START_BLOCK's, START_BLOCK's day from both sides, and a day after it")
	s, err := store.StatsBackfillState(ctx)
	require.NoError(t, err)
	s.LastTs = nil
	require.Equal(t, db.BackfillState{Exists: true, Complete: true, BalancesPending: true, Target: backfillStart - 1,
		Through: backfillStart - 1}, s, "every block counted; the touched balances are not read here")

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
	want, _ := fullSync(t, head)
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

// With one block a minute, blocks 5,867..7,306 share a day with the floor block 7,200 (head 20,000 − 12,800); block
// 5,000 is on the day before.
func TestPrepareStatsBackfillNeedsTheRangeAndItsDayFinal(t *testing.T) {
	store, _ := setupPostgres(t)
	ctx := context.Background()
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 20_000}

	require.ErrorContains(t, PrepareStatsBackfill(ctx, store, chain, 10_000, false), "within the reorg floor")
	require.ErrorContains(t, PrepareStatsBackfill(ctx, store, chain, 7_000, false), "whole day is final")
	require.NoError(t, PrepareStatsBackfill(ctx, store, chain, 5_000, false))
	s, err := store.StatsBackfillState(ctx)
	require.NoError(t, err)
	require.Equal(t, db.BackfillState{Exists: true, Target: 4_999, Through: -1}, s)
	wm, ts, err := store.PrunedThrough(ctx)
	require.NoError(t, err)
	require.Equal(t, [2]int64{4_999, int64(blockTime(4_999))}, [2]int64{int64(wm), ts}, "pruned from day one")
	require.NoError(t, PrepareStatsBackfill(ctx, store, chain, 5_000, false), "a prepared backfill is resumed, not restarted")
}

func TestStatsBackfillReadsEachTouchedBalanceOnce(t *testing.T) {
	ctx := context.Background()
	store, sql := setupPostgres(t)
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 2_000, busy: 2_001,
		balances: map[string]*big.Int{chainRecipient.Hex(): big.NewInt(7007)}}
	require.NoError(t, store.InitStatsBackfill(ctx, backfillStart-1, uint64(blockTime(backfillStart-1))))
	idx := newTestIndexer(store, chain)
	indexBlocks(t, idx, backfillStart, 1600)
	for range 2 {
		_, err := store.AdvanceDailyStats(ctx)
		require.NoError(t, err, "the aggregate starts on the day after START_BLOCK's (block 1,547)")
	}
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

// overlapChain holds each balanceOf until another is in flight (or a second passes) and records the most at once.
type overlapChain struct {
	*fakeChain
	mu       sync.Mutex
	inFlight int
	most     int
	arrived  chan struct{}
}

func (c *overlapChain) CallContract(ctx context.Context, to common.Address, data []byte) ([]byte, error) {
	c.mu.Lock()
	c.inFlight++
	c.most = max(c.most, c.inFlight)
	c.mu.Unlock()
	select {
	case c.arrived <- struct{}{}:
	case <-c.arrived:
	case <-time.After(time.Second):
	}
	defer func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}()
	return c.fakeChain.CallContract(ctx, to, data)
}

func TestStatsBackfillReadsBalancesWithItsWorkers(t *testing.T) {
	ctx := context.Background()
	store, sql := setupPostgres(t)
	balances := map[string]*big.Int{}
	for i := int64(1); i <= 8; i++ {
		a := common.BytesToAddress(big.NewInt(i).Bytes())
		balances[a.Hex()] = big.NewInt(i)
		_, err := sql.Exec(ctx, `INSERT INTO stats_backfill_balance_keys VALUES ($1, $2, 1)`,
			strings.ToLower(chainToken.Hex()), strings.ToLower(a.Hex()))
		require.NoError(t, err)
	}
	chain := &overlapChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 2_000, busy: 2_001,
		balances: balances}, arrived: make(chan struct{})}
	b := newTestIndexer(store, chain).newBackfiller(store, testBackfill)

	read, err := b.readBalances(ctx, balanceKeysPerRead)
	require.NoError(t, err)
	require.Equal(t, 8, read)
	require.GreaterOrEqual(t, chain.most, 2, "balanceOf reads overlap, up to STATS_BACKFILL_WORKERS")
	require.LessOrEqual(t, chain.most, testBackfill.Workers)
	var n int
	require.NoError(t, sql.QueryRow(ctx, `SELECT count(*) FROM token_balances_current WHERE balance > 0`).Scan(&n))
	require.Equal(t, 8, n, "every key read is written")
}

// failingChain answers balanceOf for some holders with an error instead of a balance.
type failingChain struct {
	*fakeChain
	errs map[common.Address]error
}

func (c *failingChain) CallContract(ctx context.Context, to common.Address, data []byte) ([]byte, error) {
	if err := c.errs[common.BytesToAddress(data[4:])]; err != nil {
		return nil, err
	}
	return c.fakeChain.CallContract(ctx, to, data)
}

func TestStatsBackfillKeepsABalanceWhoseReadFailedInTransit(t *testing.T) {
	ctx := context.Background()
	store, sql := setupPostgres(t)
	limited, timedOut, reverted, ok := common.HexToAddress("0xa1"), common.HexToAddress("0xa2"),
		common.HexToAddress("0xa3"), common.HexToAddress("0xa4")
	for _, a := range []common.Address{limited, timedOut, reverted, ok} {
		_, err := sql.Exec(ctx, `INSERT INTO stats_backfill_balance_keys VALUES ($1, $2, 1)`,
			strings.ToLower(chainToken.Hex()), strings.ToLower(a.Hex()))
		require.NoError(t, err)
	}
	chain := &failingChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 2_000, busy: 2_001,
		balances: map[string]*big.Int{limited.Hex(): big.NewInt(5), timedOut.Hex(): big.NewInt(6), ok.Hex(): big.NewInt(7)}},
		errs: map[common.Address]error{
			limited:  &rpclient.RPCError{Code: 429, Message: "Too Many Requests"},
			timedOut: context.DeadlineExceeded,
			reverted: &rpclient.RPCError{Code: 3, Message: "execution reverted"},
		}}
	b := newTestIndexer(store, chain).newBackfiller(store, testBackfill)

	read, err := b.readBalances(ctx, balanceKeysPerRead)
	require.Error(t, err, "a pass with failed reads backs off")
	require.Equal(t, 4, read)
	left := func() []string {
		rows, err := sql.Query(ctx, `SELECT address FROM stats_backfill_balance_keys ORDER BY address`)
		require.NoError(t, err)
		got, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)
		return got
	}
	require.Equal(t, []string{strings.ToLower(limited.Hex()), strings.ToLower(timedOut.Hex())}, left(),
		"the rate-limited and timed-out keys stay for the next pass; the reverted one (not a token) is dropped")

	delete(chain.errs, limited)
	delete(chain.errs, timedOut)
	_, err = b.readBalances(ctx, balanceKeysPerRead)
	require.NoError(t, err)
	require.Empty(t, left())
	var n int
	require.NoError(t, sql.QueryRow(ctx, `SELECT count(*) FROM token_balances_current`).Scan(&n))
	require.Equal(t, 3, n, "every holder whose read failed in transit is written on the next pass")
}

func TestStatsBackfillReadsPastKeysThatKeepFailing(t *testing.T) {
	ctx := context.Background()
	store, sql := setupPostgres(t)
	stuck1, stuck2, holder := common.HexToAddress("0xa1"), common.HexToAddress("0xa2"), common.HexToAddress("0xa3")
	for _, a := range []common.Address{stuck1, stuck2, holder} {
		_, err := sql.Exec(ctx, `INSERT INTO stats_backfill_balance_keys VALUES ($1, $2, 1)`,
			strings.ToLower(chainToken.Hex()), strings.ToLower(a.Hex()))
		require.NoError(t, err)
	}
	down := &rpclient.HTTPStatusError{Code: 503}
	chain := &failingChain{fakeChain: &fakeChain{MockRPCClient: new(MockRPCClient), head: 2_000, busy: 2_001,
		balances: map[string]*big.Int{holder.Hex(): big.NewInt(7)}}, errs: map[common.Address]error{stuck1: down, stuck2: down}}
	b := newTestIndexer(store, chain).newBackfiller(store, testBackfill)

	for range 3 {
		_, _ = b.readBalances(ctx, 2)
	}
	var n int
	require.NoError(t, sql.QueryRow(ctx, `SELECT count(*) FROM token_balances_current`).Scan(&n))
	require.Equal(t, 1, n, "a window full of keys that keep failing does not hold back the keys after it")
}

// refusingCompletion fails every completion, as daily_stats_backfill_complete did with the aggregate started on
// START_BLOCK's day.
type refusingCompletion struct{ *db.DB }

func (refusingCompletion) CompleteStatsBackfill(context.Context) error {
	return errors.New("the incremental aggregate covers START_BLOCK's day")
}

func TestStatsBackfillReadsBalancesWhileAStepFails(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	require.NoError(t, store.InitStatsBackfill(ctx, backfillStart-1, uint64(blockTime(backfillStart-1))))
	_, err := sql.Exec(ctx, `UPDATE stats_backfill SET through = target`)
	require.NoError(t, err)
	_, err = sql.Exec(ctx, `INSERT INTO stats_backfill_balance_keys VALUES ($1, $2, 1)`,
		strings.ToLower(chainToken.Hex()), strings.ToLower(chainRecipient.Hex()))
	require.NoError(t, err)
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 2_000, busy: 2_001,
		balances: map[string]*big.Int{chainRecipient.Hex(): big.NewInt(7)}}
	idx := newTestIndexer(store, chain)

	go idx.RunStatsBackfill(ctx, refusingCompletion{store}, testBackfill)
	require.Eventually(t, func() bool {
		var n int
		return sql.QueryRow(ctx, `SELECT count(*) FROM token_balances_current`).Scan(&n) == nil && n == 1
	}, 90*time.Second, 50*time.Millisecond, "the balances are read although the step keeps failing")
}

// waitingCompletion answers every completion with a wait for catch-up and counts the calls.
type waitingCompletion struct {
	*db.DB
	calls atomic.Int32
}

func (w *waitingCompletion) CompleteStatsBackfill(context.Context) error {
	w.calls.Add(1)
	return db.ErrBackfillWaitsForCatchup
}

func TestStatsBackfillDrainsWhileTheCompletionWaits(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	require.NoError(t, store.InitStatsBackfill(ctx, backfillStart-1, uint64(blockTime(backfillStart-1))))
	_, err := sql.Exec(ctx, `UPDATE stats_backfill SET through = target`)
	require.NoError(t, err)
	_, err = sql.Exec(ctx, `
		INSERT INTO stats_backfill_balance_keys
		SELECT $1, '0x' || lpad(to_hex(g), 40, '0'), 1 FROM generate_series(1, 2500) g`, strings.ToLower(chainToken.Hex()))
	require.NoError(t, err)
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 2_000, busy: 2_001, balances: map[string]*big.Int{}}
	store2 := &waitingCompletion{DB: store}
	cfg := testBackfill
	cfg.Retry = time.Hour

	go newTestIndexer(store, chain).RunStatsBackfill(ctx, store2, cfg)
	require.Eventually(t, func() bool {
		var n int
		return sql.QueryRow(ctx, `SELECT count(*) FROM stats_backfill_balance_keys`).Scan(&n) == nil && n == 0
	}, 90*time.Second, 50*time.Millisecond, "the keys drain while the completion waits for catch-up, without a retry pause per window")
	require.EqualValues(t, 1, store2.calls.Load(), "a waiting completion is asked again only after a minute")
	require.Eventually(t, func() bool { return gauge(t, "indexer_stats_backfill_waiting_for_catchup") == 1 },
		10*time.Second, 50*time.Millisecond, "the waiting gauge is up")
}

// gauge is the current value of a gauge in the default registry.
func gauge(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("no gauge %s", name)
	return 0
}

// A FORCE_REINDEX wipe of a database whose stats backfill had completed prepares a new backfill for the new chain,
// rather than leave the new chain's totals without [0, START_BLOCK) or keep the old chain's.
func TestAWipeForReindexPreparesTheStatsBackfillAgain(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx := context.Background()
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 20_000}
	require.NoError(t, PrepareStatsBackfill(ctx, store, chain, 5_000, false))
	_, err := sql.Exec(ctx, `UPDATE stats_backfill SET through = target, completed_at = now(), daily_done = true`)
	require.NoError(t, err, "the old chain's backfill had completed")

	idx := NewWithConfig(store, chain, time.Second, 5_000, &Config{RPCWorkers: 1, RPCRateLimit: 100})
	require.NoError(t, idx.wipeForReindex(ctx, 5_000))
	s, err := store.StatsBackfillState(ctx)
	require.NoError(t, err)
	require.Equal(t, db.BackfillState{Exists: true, Target: 4_999, Through: -1}, s, "a new backfill, from the start")
}

// A FORCE_REINDEX wipe stopped part way, after it removed the stats backfill and before it prepared the new one: the
// next start finishes the wipe in its reconcile, and the backfill is prepared after that, not before it.
func TestAStartAfterAWipeStoppedPartWayHasItsStatsBackfill(t *testing.T) {
	store, sql := setupPostgres(t)
	ctx := context.Background()
	chain := &fakeChain{MockRPCClient: new(MockRPCClient), head: 20_000}
	require.NoError(t, PrepareStatsBackfill(ctx, store, chain, 5_000, false))
	_, err := sql.Exec(ctx, `CREATE TABLE `+db.WipeMarkTable+` (started_at TIMESTAMPTZ NOT NULL DEFAULT now())`)
	require.NoError(t, err, "the wipe had started")
	_, err = sql.Exec(ctx, `TRUNCATE stats_backfill`)
	require.NoError(t, err, "and removed the backfill when it stopped")

	require.NoError(t, ReconcileForStart(ctx, store, chain, 5_000, true, false))
	s, err := store.StatsBackfillState(ctx)
	require.NoError(t, err)
	require.Equal(t, db.BackfillState{Exists: true, Target: 4_999, Through: -1}, s, "the backfill is prepared after the wipe")
	var mark *string
	require.NoError(t, sql.QueryRow(ctx, `SELECT to_regclass($1)::text`, db.WipeMarkTable).Scan(&mark))
	require.Nil(t, mark, "the wipe finished")
}
