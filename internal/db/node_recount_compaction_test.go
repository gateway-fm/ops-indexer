package db

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// spanTotalsFromLedger is what a correct node recount of span lo finds: its own ledger rows.
func spanTotalsFromLedger(t *testing.T, d *DB, lo int64) *SpanTotals {
	t.Helper()
	ctx := context.Background()
	s := NewSpanTotals()
	require.NoError(t, d.pool.QueryRow(ctx, `
		SELECT COALESCE(sum(value) FILTER (WHERE name = 'blocks_total'), 0)::BIGINT,
		       COALESCE(sum(value) FILTER (WHERE name = 'transactions_total'), 0)::BIGINT,
		       COALESCE(sum(value) FILTER (WHERE name = 'transfers_total'), 0)::BIGINT
		FROM ledger_chain WHERE lo = $1`, lo).Scan(&s.Blocks, &s.Transactions, &s.Transfers))
	rows, err := d.pool.Query(ctx, `SELECT token, transfers, supply::TEXT FROM ledger_token WHERE lo = $1`, lo)
	require.NoError(t, err)
	for rows.Next() {
		var token, supply string
		var transfers int64
		require.NoError(t, rows.Scan(&token, &transfers, &supply))
		v, ok := new(big.Int).SetString(supply, 10)
		require.True(t, ok)
		s.Tokens[token] = &TokenTotals{Transfers: transfers, Supply: v}
	}
	rows, err = d.pool.Query(ctx, `SELECT address, txs, sent, internal, transfers, first_seen, last_seen FROM ledger_address WHERE lo = $1`, lo)
	require.NoError(t, err)
	for rows.Next() {
		var addr string
		a := &AddressTotals{}
		var first, last int64
		require.NoError(t, rows.Scan(&addr, &a.Txs, &a.Sent, &a.Internal, &a.Transfers, &first, &last))
		a.FirstSeen, a.LastSeen = uint64(first), uint64(last)
		s.Addresses[addr] = a
	}
	return s
}

// The node recount picks a pruned span, counts it from the node for as long as its rate allows, then settles it by
// lo. Compaction can merge that span into a wider one meanwhile; the settle then compares the merged span's rows with
// the first span's count and, with repair on, replaces them: the pruned totals of the other spans leave the ledger.
func TestANodeRecountLeavesThePrunedTotalsOfASpanCompactionMergedDuringTheCount(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedHistory(t, d, 0, 16_000, 1)
	require.NoError(t, d.Migrate())
	require.NoError(t, d.RebuildAddressStats(ctx))
	closeLedger(t, d)
	_, err := d.pool.Exec(ctx, `UPDATE prune_state SET pruned_through = 2999`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `UPDATE ledger_spans SET check_state = 'ok', checked_at = now() - interval '1 hour' * (10 - lo / 1000)`)
	require.NoError(t, err)
	var before int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count FROM prune_offsets WHERE kind = 'chain' AND key = 'transactions_total'`).Scan(&before))

	lo, hi, ok, err := d.NextPrunedSpan(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, [2]int64{0, 1000}, [2]int64{lo, hi})
	counted := spanTotalsFromLedger(t, d, lo)

	_, merged, err := d.CompactLedger(ctx, 3)
	require.NoError(t, err)
	require.True(t, merged, "the closer's compaction runs while the recount counts")

	_, err = d.SettleNodeSpan(ctx, lo, hi, counted, true)
	require.NoError(t, err)
	var after int64
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count FROM prune_offsets WHERE kind = 'chain' AND key = 'transactions_total'`).Scan(&after))
	require.Equal(t, before, after, "a correct count of [0, 1000) does not move the pruned totals of [0, 3000)")
}
