package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func addressStatsLines(t *testing.T, d *DB) []string {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `
		SELECT concat_ws(' ', address, tx_count, sent_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract)
		FROM address_stats ORDER BY address`)
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

// A catch-up that wrote blocks [100, 600) without address_stats (retention off, before this branch) leaves every
// address they touch low, and an address seen only there missing. The reconciliation repairs it in place, bucket by
// bucket, survives a kill mid-bucket and a stop between buckets, and ends exactly at a full recount.
func TestAddressReconciliationRepairsACatchupGap(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedLedgerEdgeCases(t, d, 3)
	require.NoError(t, d.RebuildAddressStats(ctx))
	require.NoError(t, d.Migrate())
	want := addressStatsLines(t, d)

	_, err := d.pool.Exec(ctx, `
		UPDATE address_stats a SET tx_count = a.tx_count - g.txs, sent_count = a.sent_count - g.sent,
		       internal_tx_count = a.internal_tx_count - g.internal, token_transfer_count = a.token_transfer_count - g.transfers
		FROM (SELECT address, count(DISTINCT tx) AS txs, sum(sent) AS sent, sum(internal) AS internal, sum(transfers) AS transfers
		      FROM ledger_address_contributions(100, 600) GROUP BY address) g
		WHERE a.address = g.address`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `DELETE FROM address_stats WHERE address = '0xc301'`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `UPDATE chain_counters SET count = count - 1 WHERE name = 'addresses_total'`)
	require.NoError(t, err)
	require.NotEqual(t, want, addressStatsLines(t, d))
	closeLedger(t, d)

	for range 100 {
		_, _, done, err := d.ReconcileAddressBucket(ctx)
		require.NoError(t, err)
		require.False(t, done)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, _, err = d.ReconcileAddressBucket(cancelled)
	require.Error(t, err, "killed mid-bucket")

	for {
		_, _, done, err := d.ReconcileAddressBucket(ctx)
		require.NoError(t, err)
		if done {
			break
		}
	}
	require.Empty(t, rowDiff(want, addressStatsLines(t, d)), "address_stats equals a full recount")
	for b := 0; b < addressBuckets; b++ {
		drifts, err := d.CheckRunning(ctx, b, false)
		require.NoError(t, err)
		require.Empty(t, drifts, "bucket %d", b)
	}
	var addresses, rows int64
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT (SELECT count FROM chain_counters WHERE name = 'addresses_total'), (SELECT count(*) FROM address_stats)`).
		Scan(&addresses, &rows))
	require.Equal(t, rows, addresses, "the restored address is counted")
}

func TestAddressReconciliationWaitsForAShortOpenTail(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 14_000, 1)
	require.False(t, d.reconcileAddresses(ctx, VerifyConfig{Repair: true, MaxOpenTail: 3_000}),
		"no span closed: the tail is all 14,001 blocks")
	var next int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT value FROM ledger_verify_state WHERE name = 'address_reconcile_next'`).Scan(&next))
	require.Zero(t, next, "nothing reconciled")
}

// A close leaves PruneFloorGap blocks open, so with short spans the tail is never as short as three spans; the check
// and the reconciliation must still run once the closer has caught up.
func TestTheOpenTailACloseLeavesIsShortEnoughForTheCheck(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 14_000, 1)
	closed, err := d.CloseLedgerSpans(ctx, LedgerConfig{SpanBlocks: 1_000}, 100)
	require.NoError(t, err)
	require.Equal(t, 1, closed, "only [0, 1000) is PruneFloorGap below the head")
	require.True(t, d.tailShort(ctx, VerifyConfig{MaxOpenTail: MaxOpenTail(1_000)}))
}

func TestRebuildAddressStatsRefusesALiveTable(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 10, 1)
	require.NoError(t, d.RebuildAddressStats(ctx))
	require.ErrorIs(t, d.RebuildAddressStats(ctx), ErrAddressStatsFilled, "never TRUNCATE a table ingest writes")
}

// rowDiff lists the rows only one side holds, marked "-" (want) or "+" (got).
func rowDiff(want, got []string) []string {
	in := func(rows []string) map[string]bool {
		m := map[string]bool{}
		for _, r := range rows {
			m[r] = true
		}
		return m
	}
	w, g := in(want), in(got)
	var diff []string
	for _, r := range want {
		if !g[r] {
			diff = append(diff, "- "+r)
		}
	}
	for _, r := range got {
		if !w[r] {
			diff = append(diff, "+ "+r)
		}
	}
	return diff
}
