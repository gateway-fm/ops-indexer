//go:build faults

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

// countersExact compares every delta-kept counter with a recount, and address_stats with the repair's recount.
func countersExact(t *testing.T, url, label string) {
	t.Helper()
	admin := faultkit.Conn(t, url)
	ctx := context.Background()
	var bad []string
	rows, err := admin.Query(ctx, `
		SELECT 'blocks_total' WHERE (SELECT count FROM chain_counters WHERE name = 'blocks_total') <> (SELECT count(*) FROM blocks)
		UNION ALL SELECT 'transactions_total' WHERE (SELECT count FROM chain_counters WHERE name = 'transactions_total') <> (SELECT count(*) FROM transactions)
		UNION ALL SELECT 'transfers_total' WHERE (SELECT count FROM chain_counters WHERE name = 'transfers_total') <> (SELECT count(*) FROM token_transfers)
		UNION ALL SELECT 'addresses_total' WHERE (SELECT count FROM chain_counters WHERE name = 'addresses_total') <> (SELECT count(*) FROM address_stats)
		UNION ALL SELECT 'tokens_total' WHERE (SELECT count FROM chain_counters WHERE name = 'tokens_total') <> (SELECT count(*) FROM tokens)
		UNION ALL SELECT 'transfer_count ' || t.address FROM tokens t
		          WHERE t.transfer_count <> (SELECT count(*) FROM token_transfers WHERE token_address = t.address)`)
	require.NoError(t, err)
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		bad = append(bad, s)
	}
	rows.Close()
	d, err := db.New(url)
	require.NoError(t, err)
	defer d.Close()
	s, err := d.StageAddressStatsRepair(ctx, true)
	require.NoError(t, err)
	t.Logf("%s: counters off=%v address_stats wrong=%d", label, bad, s.Changes)
	require.Empty(t, bad, label)
	require.Zero(t, s.Changes, "%s: address_stats equals the recount", label)
}

// Block writes whose COMMIT ack is lost, or that fail before committing, while the indexer ingests (catch-up on a
// fresh start, then realtime after a restart): no counter and no address_stats row is counted twice or dropped.
func TestFaultBlockWriteFaultsKeepCountersExact(t *testing.T) {
	for aname, action := range map[string]faultkit.Action{"lost-ack": faultkit.LostAck, "fail-commit": faultkit.FailCommit, "kill-before": faultkit.KillBefore} {
		t.Run(aname, func(t *testing.T) {
			url := faultkit.NewDB(t)
			proxy := faultkit.NewProxy(t, url)
			d, err := db.New(proxy.URL())
			require.NoError(t, err)
			defer d.Close()
			require.NoError(t, d.Migrate())
			node := faultkit.NewNode(t, 200)
			r := proxy.Arm(faultkit.Rule{Match: "INSERT INTO address_stats (address, tx_count, internal_tx_count", Action: action, Times: 8})
			ingest(t, d, node, 200, url)
			t.Logf("fresh start: %d faults fired", proxy.Fired(r))
			countersExact(t, url, aname+" fresh start")

			node.SetHead(400)
			r = proxy.Arm(faultkit.Rule{Match: "INSERT INTO address_stats (address, tx_count, internal_tx_count", Action: action, Times: 8})
			d2, err := db.New(proxy.URL())
			require.NoError(t, err)
			defer d2.Close()
			require.NoError(t, d2.Migrate())
			ingest(t, d2, node, 400, url)
			t.Logf("restart: %d faults fired", proxy.Fired(r))
			countersExact(t, url, aname+" after restart")
		})
	}
}
