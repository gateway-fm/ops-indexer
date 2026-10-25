package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// A database whose running counters agree with its rows (startup re-seed, address_stats rebuilt) and whose spans
// below the reorg floor are closed.
func verifiedDB(t *testing.T) (*DB, func()) {
	t.Helper()
	d, cleanup := setupTestDB(t)
	seedHistory(t, d, 0, 14_000, 1)
	require.NoError(t, d.Migrate())
	require.NoError(t, d.RebuildAddressStats(context.Background()))
	closeLedger(t, d)
	return d, cleanup
}

func checkAllBuckets(t *testing.T, d *DB, repair bool) []Drift {
	t.Helper()
	var all []Drift
	seen := map[string]bool{}
	for b := 0; b < addressBuckets; b++ {
		drifts, err := d.CheckRunning(context.Background(), b, repair)
		require.NoError(t, err)
		for _, dr := range drifts {
			if name := driftNames([]Drift{dr})[0]; !seen[name] {
				seen[name] = true
				all = append(all, dr)
			}
		}
	}
	return all
}

func recountSpan(t *testing.T, d *DB, lo int64, repair bool) SpanCheck {
	t.Helper()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `UPDATE ledger_spans SET checked_at = CASE WHEN lo = $1 THEN NULL ELSE now() END`, lo)
	require.NoError(t, err)
	c, ok, err := d.RecountRetainedSpan(ctx, repair, "64MB")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, lo, c.Lo)
	return c
}

func TestVerifyFindsNothingOnAConsistentDatabase(t *testing.T) {
	d, cleanup := verifiedDB(t)
	defer cleanup()
	require.Equal(t, SpanCheck{Lo: 0, Hi: 1000, State: "ok"}, recountSpan(t, d, 0, true))
	require.Empty(t, checkAllBuckets(t, d, false))
}

func TestRecountRepairsACorruptedSpanOnceThenQuarantines(t *testing.T) {
	d, cleanup := verifiedDB(t)
	defer cleanup()
	ctx := context.Background()
	corrupt := func() {
		_, err := d.pool.Exec(ctx, `UPDATE ledger_token SET transfers = transfers + 1 WHERE lo = 0`)
		require.NoError(t, err)
	}
	storedTransfers := func() (n int64) {
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT transfers FROM ledger_token WHERE lo = 0`).Scan(&n))
		return n
	}

	corrupt()
	require.Equal(t, SpanCheck{Lo: 0, Hi: 1000, Drifted: 1, State: "drift"}, recountSpan(t, d, 0, false))
	require.Equal(t, int64(1001), storedTransfers(), "no repair without repair on")

	tx, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SELECT ledger_stage_from_rows(0, 1000); SELECT ledger_span_settle(0, true)`)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx), "killed mid-repair")
	require.Equal(t, int64(1001), storedTransfers())

	require.Equal(t, SpanCheck{Lo: 0, Hi: 1000, Drifted: 1, State: "repaired"}, recountSpan(t, d, 0, true))
	require.Equal(t, int64(1000), storedTransfers(), "replaced from the span's rows, once")
	var logged int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM ledger_repairs WHERE lo = 0`).Scan(&logged))
	require.Equal(t, int64(1), logged)
	require.Equal(t, SpanCheck{Lo: 0, Hi: 1000, State: "repaired"}, recountSpan(t, d, 0, true))
	require.Empty(t, checkAllBuckets(t, d, false), "the running counters never moved")

	corrupt()
	require.Equal(t, SpanCheck{Lo: 0, Hi: 1000, Drifted: 1, State: "quarantined"}, recountSpan(t, d, 0, true))
	require.Equal(t, int64(1001), storedTransfers(), "a drift that comes back is not repaired again")
	n, err := d.QuarantinedCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestRunningCheckRepairsDriftedCountersOnceThenQuarantines(t *testing.T) {
	d, cleanup := verifiedDB(t)
	defer cleanup()
	ctx := context.Background()
	drift := func() {
		_, err := d.pool.Exec(ctx, `UPDATE tokens SET transfer_count = transfer_count - 1 WHERE address = $1`, retentionToken)
		require.NoError(t, err)
		_, err = d.pool.Exec(ctx, `
			UPDATE chain_counters SET count = count + 1 WHERE name = 'transactions_total';
			UPDATE address_stats SET token_transfer_count = token_transfer_count + 1, first_seen = 5 WHERE address = '0xb'`)
		require.NoError(t, err)
	}
	read := func() (txs, transfers, addrTransfers, firstSeen int64) {
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT (SELECT count FROM chain_counters WHERE name = 'transactions_total'),
			       (SELECT transfer_count FROM tokens WHERE address = $1),
			       (SELECT token_transfer_count FROM address_stats WHERE address = '0xb'),
			       (SELECT first_seen FROM address_stats WHERE address = '0xb')`, retentionToken).
			Scan(&txs, &transfers, &addrTransfers, &firstSeen))
		return
	}
	want0, want1, want2, want3 := read()

	drift()
	found := checkAllBuckets(t, d, false)
	require.ElementsMatch(t, []string{"chain transactions_total count", "token " + retentionToken + " transfers",
		"address 0xb transfers", "address 0xb first_seen"}, driftNames(found))

	tx, err := d.pool.Begin(ctx)
	require.NoError(t, err)
	for _, dr := range found {
		apply, err := recordRepair(ctx, tx, dr)
		require.NoError(t, err)
		if apply && dr.Family != "address" {
			require.NoError(t, applyRunning(ctx, tx, dr))
		}
	}
	require.NoError(t, tx.Rollback(ctx), "killed mid-repair")

	checkAllBuckets(t, d, true)
	g0, g1, g2, g3 := read()
	require.Equal(t, [4]int64{want0, want1, want2, want3}, [4]int64{g0, g1, g2, g3}, "repaired once, nothing counted twice")
	require.Empty(t, checkAllBuckets(t, d, false))

	drift()
	checkAllBuckets(t, d, true)
	g0, _, _, _ = read()
	require.Equal(t, want0+1, g0, "a drift that comes back is not repaired again")
	n, err := d.QuarantinedCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), n, "the chain counter, the token and the address are quarantined")
}

func driftNames(drifts []Drift) []string {
	out := make([]string, len(drifts))
	for i, dr := range drifts {
		out[i] = dr.Family + " " + dr.Key + " " + dr.Counter
	}
	return out
}

func TestRunningCheckCountsOpenRowsAboveTheLedger(t *testing.T) {
	d, cleanup := verifiedDB(t)
	defer cleanup()
	ctx := context.Background()
	var closedThrough int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT ledger_closed_through()`).Scan(&closedThrough))
	require.Equal(t, int64(1000), closedThrough)
	require.NoError(t, pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM ledger_spans`)
		return err
	}))
	require.Empty(t, checkAllBuckets(t, d, false), "with no span closed every row is open, and the totals still agree")
}

// The running repair moves a counter by (expected - stored), not to expected, so a delta ingest
// commits between the check and the repair is kept.
func TestRunningRepairKeepsADeltaCommittedAfterTheCheck(t *testing.T) {
	d, cleanup := verifiedDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `UPDATE chain_counters SET count = count + 1 WHERE name = 'transactions_total'`)
	require.NoError(t, err)
	var drift *Drift
	for _, dr := range checkAllBuckets(t, d, false) {
		if dr.Family == "chain" && dr.Key == "transactions_total" {
			dr := dr
			drift = &dr
		}
	}
	require.NotNil(t, drift, "the corrupted counter is found")

	seedHistory(t, d, 14_001, 14_001, 1)
	_, err = d.pool.Exec(ctx, `UPDATE chain_counters SET count = count + 1 WHERE name = 'transactions_total'`)
	require.NoError(t, err)

	require.NoError(t, d.repairDrifts(ctx, []Drift{*drift}))
	var counter, rows int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count FROM chain_counters WHERE name = 'transactions_total'), (SELECT count(*) FROM transactions)`).Scan(&counter, &rows))
	require.Equal(t, rows, counter, "the repair keeps the transaction ingested after the check")
}
