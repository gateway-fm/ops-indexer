package db

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

const rebaseHot = "0x00000000000000000000000000000000000000aa"

func rebaseNewAddress(n int) string { return fmt.Sprintf("0x%040x", 0x1000+n) }

// rebaseBlock is block n: the hot address sends to the block's new address, which sends back; each sender's tx is
// sent, each address counts both, as the indexer writes them.
func rebaseBlock(n int) *BlockData {
	fresh := rebaseNewAddress(n)
	hot, back := rebaseHot, rebaseHot
	return &BlockData{
		Block: &types.Block{Number: uint64(n), Hash: fmt.Sprintf("0xrb%062x", n), ParentHash: fmt.Sprintf("0xrb%062x", n-1),
			Timestamp: uint64(1_700_000_000 + n), GasLimit: 30_000_000, TransactionCount: 2},
		Transactions: []*types.Transaction{
			{Hash: fmt.Sprintf("0xrt%061x0", n), BlockNumber: uint64(n), From: hot, To: &fresh, Value: "0", Status: 1},
			{Hash: fmt.Sprintf("0xrt%061x1", n), BlockNumber: uint64(n), TxIndex: 1, From: fresh, To: &back, Value: "0", Status: 1},
		},
		AddressStats: map[string]*AddressStatsDelta{
			rebaseHot: {Address: rebaseHot, TxCountDelta: 2, SentDelta: 1, BlockNumber: uint64(n)},
			fresh:     {Address: fresh, TxCountDelta: 2, SentDelta: 1, BlockNumber: uint64(n)},
		},
	}
}

// requireAddressStatsIsTheTruth compares every address_stats row with ledger_address_truth, and addresses_total with
// the rows. A row whose address has no truth fails: an address no canonical block has seen has no row.
func requireAddressStatsIsTheTruth(t *testing.T, d *DB) {
	t.Helper()
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, `
		WITH a AS (SELECT array_agg(address) AS all FROM address_stats)
		SELECT concat_ws(' ', s.address, s.tx_count, s.sent_count, s.internal_tx_count, s.token_transfer_count,
		                 s.first_seen, s.last_seen),
		       CASE WHEN t.address IS NULL THEN s.address || ' has no truth' ELSE
		       concat_ws(' ', s.address, t.txs, t.sent, t.internal, t.transfers, t.first_seen, t.last_seen) END
		FROM address_stats s LEFT JOIN (SELECT * FROM a, ledger_address_truth(a.all)) t ON t.address = s.address
		ORDER BY s.address`)
	require.NoError(t, err)
	type pair struct{ got, want string }
	all, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (pair, error) {
		var p pair
		return p, r.Scan(&p.got, &p.want)
	})
	require.NoError(t, err)
	require.NotEmpty(t, all)
	for _, p := range all {
		require.Equal(t, p.want, p.got)
	}
	var total, counted int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count FROM chain_counters WHERE name = 'addresses_total'), (SELECT count(*) FROM address_stats)`).
		Scan(&total, &counted))
	require.Equal(t, counted, total, "addresses_total counts each row once")
}

// A rebase racing ingest on a hot address, and on addresses ingest is about to create, loses
// no delta, counts no row twice, and every batch finishes (REPEATABLE READ never did under this load).
func TestRebaseRacingIngestLosesNoDeltaAndFinishes(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	const blocks = 300
	var written atomic.Int64
	wrote := make(chan error, 1)
	go func() {
		for n := 1; n <= blocks; n++ {
			if err := d.InsertBlockDataBatch(ctx, rebaseBlock(n)); err != nil {
				wrote <- err
				return
			}
			written.Store(int64(n))
		}
		wrote <- nil
	}()
	rebases := 0
	for running := true; running; {
		select {
		case err := <-wrote:
			require.NoError(t, err)
			running = false
		default:
		}
		next := int(written.Load())
		addresses := []string{rebaseHot}
		for n := max(1, next-3); n <= min(blocks, next+3); n++ {
			addresses = append(addresses, rebaseNewAddress(n))
		}
		_, err := d.RebaseAddresses(ctx, addresses, addresses)
		require.NoError(t, err, "a rebase never fails for a conflict with ingest")
		rebases++
	}
	require.Greater(t, rebases, 10, "the rebases ran during ingest")
	requireAddressStatsIsTheTruth(t, d)
}

// An address seen only in a reverted block keeps its row, with nothing counted and never seen.
func TestRebaseOfAForkOnlyAddress(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(1)))
	require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(2)))
	require.NoError(t, d.DeleteBlock(ctx, 2))
	total := addressesTotal(t, d)
	both := []string{rebaseHot, rebaseNewAddress(2)}
	res, err := d.RebaseAddresses(ctx, both, both)
	require.NoError(t, err)
	require.Equal(t, RebaseResult{}, res, "the revert already left both addresses as the truth has them; no row is created for the fork-only one")
	var exists bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM address_stats WHERE address = $1)`,
		rebaseNewAddress(2)).Scan(&exists))
	require.False(t, exists, "seen only on the reverted fork: no row")
	require.Equal(t, total, addressesTotal(t, d))
	requireAddressStatsIsTheTruth(t, d)
}

// A row whose address no canonical block has seen, whatever left it there, is removed and leaves addresses_total.
func TestRebaseRemovesARowWithNoTruth(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(1)))
	stray := rebaseNewAddress(9)
	_, err := d.pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, sent_count, first_seen, last_seen, is_contract, updated_at)
		VALUES ($1, 3, 0, 0, 1, 1, 1, false, NOW())`, stray)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `UPDATE chain_counters SET count = count + 1 WHERE name = 'addresses_total'`)
	require.NoError(t, err)
	total := addressesTotal(t, d)

	res, err := d.RebaseAddresses(ctx, []string{rebaseHot, stray}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, res.Removed)
	require.Zero(t, res.Changed)
	require.NotNil(t, res.FirstSeenFrom, "the removed row's first_seen leaves the days' address columns")
	require.EqualValues(t, 1, *res.FirstSeenFrom)
	require.Equal(t, total-1, addressesTotal(t, d))
	requireAddressStatsIsTheTruth(t, d)
}

func addressesTotal(t *testing.T, d *DB) int64 {
	t.Helper()
	var n int64
	require.NoError(t, d.pool.QueryRow(context.Background(),
		`SELECT count FROM chain_counters WHERE name = 'addresses_total'`).Scan(&n))
	return n
}

// The rebase locks its rows before it counts the truth: when it corrects a row a block write holds, the write commits
// first and is counted once, not overwritten by a truth counted without it.
func TestRebaseCountsTheTruthAfterTakingTheRows(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 5; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 5 WHERE address = $1`, rebaseHot)
	require.NoError(t, err, "a tail error for the rebase to correct")
	w, err := d.BeginBlockData(ctx, rebaseBlock(6))
	require.NoError(t, err)
	defer w.tx.Rollback(ctx)
	require.NoError(t, writeSharedRows(ctx, w.tx, sharedRows{data: w.data, blocks: w.deltaBlocks, txs: w.deltaTxs,
		transfers: w.deltaTransfers, tokenDeltas: w.tokenDeltas, seedCountsOwnRows: true}))

	rebased := make(chan error, 1)
	go func() {
		_, err := d.RebaseAddresses(ctx, []string{rebaseHot}, nil)
		rebased <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%address_stats%')`).
			Scan(&waiting))
		return waiting
	}, 10*time.Second, 10*time.Millisecond, "the rebase waits for the block write's row")
	require.NoError(t, w.tx.Commit(ctx))
	require.NoError(t, <-rebased)
	requireAddressStatsIsTheTruth(t, d)
}

// Whatever went wrong in the running counters of a span's addresses is set right when the span closes. A
// delta applied twice, a block written without its deltas and a wrong subtraction all end equal to the ledger's count,
// and an address seen only on a reverted fork has no row.
func TestClosingASpanRebasesItsAddresses(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 600; n++ {
		data := rebaseBlock(n)
		if n == 20 {
			data.SkipAddressStats = true
		}
		require.NoError(t, d.InsertBlockDataBatch(ctx, data))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 2, sent_count = sent_count + 1 WHERE address = $1`,
		rebaseNewAddress(10))
	require.NoError(t, err, "block 10's deltas applied twice")
	_, err = d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count - 1 WHERE address = $1`, rebaseNewAddress(30))
	require.NoError(t, err, "a subtraction of the wrong block")
	require.NoError(t, d.DeleteBlock(ctx, 300))
	replacement := rebaseBlock(300)
	other := fmt.Sprintf("0x%040x", 0x2000+300)
	replacement.Block.Hash = "0xrbfork300"
	replacement.Transactions[0].Hash, replacement.Transactions[1].Hash = "0xrtfork300a", "0xrtfork300b"
	replacement.Transactions[0].To, replacement.Transactions[1].From = &other, other
	delete(replacement.AddressStats, rebaseNewAddress(300))
	replacement.AddressStats[other] = &AddressStatsDelta{Address: other, TxCountDelta: 2, SentDelta: 1, BlockNumber: 300}
	require.NoError(t, d.InsertBlockDataBatch(ctx, replacement))
	_, err = d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xempty' || g, '0xempty' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(601, 14000) g`)
	require.NoError(t, err, "empty blocks, so the first spans are final")

	closed, err := d.CloseLedgerSpans(ctx, LedgerConfig{SpanBlocks: 500, FirstBlock: 1, WorkMem: "64MB"}, 2)
	require.NoError(t, err)
	require.Equal(t, 2, closed, "blocks 1-999 closed; every error above is in them")
	requireAddressStatsIsTheTruth(t, d)
	var missing int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT count(*) FROM (SELECT DISTINCT address FROM ledger_address) l
		WHERE NOT EXISTS (SELECT 1 FROM address_stats a WHERE a.address = l.address)`).Scan(&missing))
	require.Zero(t, missing, "block 20's new address, written without its deltas, has a row")
	var forkOnly bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM address_stats WHERE address = $1)`,
		rebaseNewAddress(300)).Scan(&forkOnly))
	require.False(t, forkOnly, "seen only on the reverted fork")
}

// When a rebase moves a first_seen, the days' address columns are recounted, and a recount that fails is
// redone by the next close, though every span is marked rebased by then.
func TestDaysRecountSurvivesAFailedFirstAttempt(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 600; n++ {
		data := rebaseBlock(n)
		data.SkipAddressStats = n == 20
		require.NoError(t, d.InsertBlockDataBatch(ctx, data))
	}
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xempty' || g, '0xempty' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(601, 14000) g`)
	require.NoError(t, err)
	advanceUntilStable(t, d)
	cfg := LedgerConfig{SpanBlocks: 500, FirstBlock: 1, WorkMem: "64MB"}

	held, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = held.Exec(ctx, `SELECT pg_advisory_xact_lock(4670, 1)`)
	require.NoError(t, err)
	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = d.CloseLedgerSpans(short, cfg, 1)
	cancel()
	require.Error(t, err, "the days' recount waits for daily_stats_advance's lock past the deadline")
	require.NoError(t, held.Rollback(ctx))

	_, err = d.CloseLedgerSpans(ctx, cfg, 1)
	require.NoError(t, err)
	requireAddressColumnsFollowTheBlocks(t, d)
}
