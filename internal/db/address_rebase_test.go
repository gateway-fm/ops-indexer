package db

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// A rebase that corrects a row a block write holds, uncommitted, counts without that block, waits for the write to
// apply, and adds its correction on top: the block is counted once, not overwritten by a truth counted without it.
func TestRebaseCorrectsARowAWriteHolds(t *testing.T) {
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
	}, 10*time.Second, 10*time.Millisecond, "the rebase's apply waits for the block write's row")
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
	defer held.Rollback(ctx)
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

// A batch's writes take its rows in byte order, ingest's, inserting a missing row in its turn: an existing
// row ingest holds, sorting before a missing one, is waited for, and the missing one is then added to, not deadlocked
// over.
func TestRebaseTakesItsRowsInIngestsOrder(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	existing, missing := "0x0000000000000000000000000000000000000001", "0x0000000000000000000000000000000000000002"
	_, err := d.pool.Exec(ctx, `INSERT INTO address_stats (address, tx_count, first_seen, last_seen) VALUES ($1, 1, 1, 1)`, existing)
	require.NoError(t, err)

	ingest, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	defer ingest.Rollback(ctx)
	_, err = ingest.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 1 WHERE address = $1`, existing)
	require.NoError(t, err)

	rebased := make(chan error, 1)
	go func() {
		_, err := d.RebaseAddresses(ctx, []string{existing, missing}, []string{missing})
		rebased <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%address_stats%')`).
			Scan(&waiting))
		return waiting
	}, 10*time.Second, 10*time.Millisecond, "the rebase waits for ingest's row")
	_, err = ingest.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count, first_seen, last_seen) VALUES ($1, 1, 2, 2)
		ON CONFLICT (address) DO UPDATE SET tx_count = address_stats.tx_count + 1`, missing)
	require.NoError(t, err, "ingest writes the next address in its order")
	require.NoError(t, ingest.Commit(ctx))
	require.NoError(t, <-rebased, "no deadlock")
}

// A batch counted its truth holding its rows, so on a large database, where a count over the open tail took ~100 s,
// every block that touched one of the batch's addresses waited for it and ingest stopped. The count no longer holds a
// row: a block written meanwhile commits at once, and is still counted once, for the hot address it corrects and for
// the address it creates.
func TestRebaseCountsWithoutHoldingItsRows(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 5; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 5 WHERE address = $1`, rebaseHot)
	require.NoError(t, err, "a tail error for the rebase to correct")
	var def string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT pg_get_functiondef('ledger_address_truth(text[])'::regprocedure)`).Scan(&def))
	body := strings.Index(def, "AS $")
	require.Positive(t, body)
	open := body + 4 + strings.Index(def[body+4:], "$") + 1
	_, err = d.pool.Exec(ctx, def[:open]+"\n    SELECT pg_sleep(2);"+def[open:])
	require.NoError(t, err)

	rebased := make(chan error, 1)
	go func() {
		_, err := d.RebaseAddresses(ctx, []string{rebaseHot, rebaseNewAddress(6)}, []string{rebaseHot, rebaseNewAddress(6)})
		rebased <- err
	}()
	require.Eventually(t, func() bool {
		var counting bool
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'PgSleep')`).Scan(&counting))
		return counting
	}, 10*time.Second, 10*time.Millisecond, "the rebase is counting")
	began := time.Now()
	require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(6)))
	require.Less(t, time.Since(began), time.Second, "the block's write did not wait for the count")
	require.NoError(t, <-rebased)
	requireAddressStatsIsTheTruth(t, d)
}

// slowTruth makes ledger_address_truth sleep for the given seconds first, inside the caller's snapshot, so a test can
// act while a rebase counts.
func slowTruth(t *testing.T, d *DB, seconds int) {
	t.Helper()
	ctx := context.Background()
	var def string
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT pg_get_functiondef('ledger_address_truth(text[])'::regprocedure)`).Scan(&def))
	body := strings.Index(def, "AS $")
	require.Positive(t, body)
	open := body + 4 + strings.Index(def[body+4:], "$") + 1
	_, err := d.pool.Exec(ctx, def[:open]+fmt.Sprintf("\n    SELECT pg_sleep(%d);", seconds)+def[open:])
	require.NoError(t, err)
}

// Two rebases of one address that both read before either applied (the closer's and the running check's,
// which run apart) each added the same correction, and left the inverse error. Rebases now take a lock of their own
// around read and apply, so the second reads after the first applied.
func TestTwoRebasesOfOneAddressCorrectItOnce(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 5; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 5 WHERE address = $1`, rebaseHot)
	require.NoError(t, err, "15 against a truth of 10")
	slowTruth(t, d, 2)

	rebased := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := d.RebaseAddresses(ctx, []string{rebaseHot}, nil)
			rebased <- err
		}()
	}
	require.NoError(t, <-rebased)
	require.NoError(t, <-rebased)
	requireAddressStatsIsTheTruth(t, d)
}

// repair-address-stats: its corrections are staged at one snapshot and applied by later runs, and in this
// version rebases run in between, so a stage left by an earlier run may hold corrections a rebase has applied since.
// A new run stages again instead of applying it.
func TestRepairAddressStatsStagesAgainAfterARebase(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 5; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 5 WHERE address = $1`, rebaseHot)
	require.NoError(t, err)
	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err, "an earlier run staged the correction and stopped")
	_, err = d.RebaseAddresses(ctx, []string{rebaseHot}, nil)
	require.NoError(t, err, "a span close corrected the row since")

	_, err = d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err, "the next run stages again")
	for done := false; !done; {
		_, _, done, err = d.ApplyAddressStatsRepairBatch(ctx, 100)
		require.NoError(t, err)
	}
	requireAddressStatsIsTheTruth(t, d)
}

// A first_seen already too low (1, against a first block of 4) stays put when catch-up stores the address's
// block 2 during the count, since ingest only lowers it; the rebase then wrote the truth it had counted, 4, though the
// truth was 2 by then. A first_seen the rebase corrects is now counted again, under the row's lock.
func TestRebaseCorrectsFirstSeenWhenAnOlderBlockArrivesDuringTheCount(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 4; n <= 6; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET first_seen = 1 WHERE address = $1`, rebaseHot)
	require.NoError(t, err, "first_seen too low")
	slowTruth(t, d, 2)

	rebased := make(chan error, 1)
	go func() {
		_, err := d.RebaseAddresses(ctx, []string{rebaseHot}, nil)
		rebased <- err
	}()
	require.Eventually(t, func() bool {
		var counting bool
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'PgSleep')`).Scan(&counting))
		return counting
	}, 10*time.Second, 10*time.Millisecond, "the rebase is counting")
	require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(2)), "catch-up stores an older block")
	require.NoError(t, <-rebased)
	requireAddressStatsIsTheTruth(t, d)
}

// queryCounter counts the statements whose SQL contains match.
type queryCounter struct {
	match string
	n     atomic.Int64
}

func (c *queryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, c.match) {
		c.n.Add(1)
	}
	return ctx
}

func (c *queryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// A span's rebase counted its addresses' truth batch by batch through each table's address index, ~0.1 s
// an address on a large database, so a span of a million addresses took about a day and held up every later close. A span is
// now counted in one set-based statement: no batch reads the truth address by address, and the error is still set
// right.
func TestClosingASpanCountsItsTruthInOneStatement(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 600; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET tx_count = tx_count + 5 WHERE address = $1`, rebaseHot)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xempty' || g, '0xempty' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(601, 14000) g`)
	require.NoError(t, err)

	counter := &queryCounter{match: "ledger_address_truth($1::TEXT[])"}
	cfg := d.pool.Config()
	cfg.ConnConfig.Tracer = counter
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer traced.Close()
	closed, err := (&DB{pool: traced}).CloseLedgerSpans(ctx, LedgerConfig{SpanBlocks: 500, FirstBlock: 1, WorkMem: "64MB"}, 2)
	require.NoError(t, err)
	require.Equal(t, 2, closed)
	require.Zero(t, counter.n.Load(), "no batch counted the truth address by address")
	requireAddressStatsIsTheTruth(t, d)
}

// A row with zero counts whose address no canonical block has seen (one an older build's rebase left), recorded by a
// revert, equals an empty truth column by column; the span rebase stages it with the reverted addresses and removes it.
func TestClosingASpanRemovesAZeroRowWithNoTruth(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 10; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	stray := rebaseNewAddress(99)
	_, err := d.pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, sent_count, is_contract, updated_at)
		VALUES ($1, 0, 0, 0, 0, false, NOW())`, stray)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `UPDATE chain_counters SET count = count + 1 WHERE name = 'addresses_total'`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `INSERT INTO ledger_reverted_address (address) VALUES ($1)`, stray)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xempty' || g, '0xempty' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(11, 14000) g`)
	require.NoError(t, err, "empty blocks, so the first span is final")

	closed, err := d.CloseLedgerSpans(ctx, LedgerConfig{SpanBlocks: 500, FirstBlock: 1, WorkMem: "64MB"}, 1)
	require.NoError(t, err)
	require.Equal(t, 1, closed)
	var exists bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM address_stats WHERE address = $1)`, stray).Scan(&exists))
	require.False(t, exists, "no canonical block has seen it")
	requireAddressStatsIsTheTruth(t, d)
}

// A span's rebase counts, per column, the corrections it staged, so its log line says what was wrong.
func TestSpanRebaseCountsItsCorrectionsByColumn(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 600; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `UPDATE address_stats SET sent_count = 0 WHERE address = $1`, rebaseNewAddress(10))
	require.NoError(t, err, "a sender whose sent_count was never counted")
	_, err = d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xempty' || g, '0xempty' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(601, 14000) g`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `SELECT ledger_close_span(1, 500)`)
	require.NoError(t, err)

	_, err = d.pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, sent_count, is_contract, updated_at)
		VALUES ($1, 2, 0, 0, 1, false, NOW())`, rebaseNewAddress(9999))
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `INSERT INTO ledger_reverted_address (address) VALUES ($1)`, rebaseNewAddress(9999))
	require.NoError(t, err, "a row no canonical block has seen, recorded by a revert")

	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	staged, err := stageSpanRebase(ctx, conn.Conn(), 1)
	require.NoError(t, err)
	require.Equal(t, spanCorrections{Staged: 2, Txs: 1, Sent: 2, NoTruth: 1}, staged)
}

// A span's staging reads each history table above the closed spans once: under load the tail is the open partition,
// and reading transactions twice through its block_number index cost seven of the statement's thirteen minutes.
func TestSpanStagingReadsEachTailTableOnce(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 600; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	var plan []map[string]any
	require.NoError(t, d.pool.QueryRow(ctx, `EXPLAIN (FORMAT JSON) `+stageSpanSelect, int64(1), int64(500)).Scan(&plan))
	scans := map[string]int{}
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		if rel, ok := node["Relation Name"].(string); ok {
			scans[rel]++
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			walk(child.(map[string]any))
		}
	}
	walk(plan[0]["Plan"].(map[string]any))
	for _, table := range []string{"transactions", "token_transfers", "internal_transactions"} {
		read := false
		for rel, n := range scans {
			if strings.HasPrefix(rel, table+"_") || rel == table {
				read = true
				require.Equal(t, 1, n, "scans of %s", rel)
			}
		}
		require.True(t, read, "the tail reads %s", table)
	}
}

// A span's staging counts the rows above the closed spans as ledger_address_truth does: self-sends, self-transfers,
// mints from the zero address, internal calls and contract creations in the tail.
func TestSpanStagingCountsTheTailAsTheTruthDoes(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 600; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, rebaseBlock(n)))
	}
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xempty' || g, '0xempty' || (g - 1), 1700000000 + g, 0, 30000000, 0 FROM generate_series(601, 14000) g`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `SELECT ledger_close_span(1, 500)`)
	require.NoError(t, err)
	a, b, c := rebaseNewAddress(10), rebaseNewAddress(20), rebaseNewAddress(30)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ('tail1', 550, 1700000550, 5, '`+a+`', '`+a+`', '0', 0, 0, '0x', 1, 2),
		       ('tail2', 551, 1700000551, 5, '`+a+`', '`+b+`', '0', 0, 0, '0x', 1, 106),
		       ('tail3', 552, 1700000552, 5, '`+c+`', NULL, '0', 0, 0, '0x', 1, 0);
		INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number, timestamp)
		VALUES ('tail1', 10, '0x70c1', '`+a+`', '`+a+`', 1, 550, 1700000550),
		       ('tail1', 11, '0x70c1', '`+b+`', '`+c+`', 1, 550, 1700000550),
		       ('tail2', 12, '0x70c1', '`+c+`', '`+a+`', 1, 551, 1700000551),
		       ('tail2', 13, '0x70c1', '0x0000000000000000000000000000000000000000', '`+b+`', 1, 551, 1700000551);
		INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value, call_type)
		VALUES ('tail1', 550, '0', '`+a+`', '`+a+`', '0', 'call'),
		       ('tail2', 551, '0', '`+a+`', '`+b+`', '0', 'call'),
		       ('tail3', 552, '0', '`+c+`', NULL, '0', 'create');
		INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number)
		VALUES ('`+b+`', '00', '`+c+`', 'tail3', 552)`)
	require.NoError(t, err)

	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	staged, err := stageSpanRebase(ctx, conn.Conn(), 1)
	require.NoError(t, err)
	require.EqualValues(t, 3, staged.Staged)
	var wrong int
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT count(*) FROM address_rebase_stage s
		FULL JOIN ledger_address_truth(ARRAY[$1, $2, $3]) t USING (address)
		WHERE (s.want_txs, s.want_sent, s.want_internal, s.want_transfers, s.want_first, s.want_last)
		      IS DISTINCT FROM (t.txs, t.sent, t.internal, t.transfers, t.first_seen, t.last_seen)`, a, b, c).Scan(&wrong))
	require.Zero(t, wrong)
}
