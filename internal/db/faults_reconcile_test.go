//go:build faults

package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/faultkit"
)

// faultDB is a migrated database reached through a fault proxy, and its direct URL.
func faultDB(t *testing.T) (*DB, *faultkit.Proxy, string) {
	t.Helper()
	url := faultkit.NewDB(t)
	proxy := faultkit.NewProxy(t, url)
	d := faultOpen(t, proxy.URL())
	require.NoError(t, d.Migrate())
	return d, proxy, url
}

func faultOpen(t *testing.T, url string) *DB {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &DB{pool: pool}
}

type faultCounters struct {
	retentionTotals
	holders int64
}

func faultRead(t *testing.T, d *DB) faultCounters {
	t.Helper()
	var c faultCounters
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT (SELECT count FROM chain_counters WHERE name = 'blocks_total'),
		       (SELECT count FROM chain_counters WHERE name = 'transactions_total'),
		       (SELECT count FROM chain_counters WHERE name = 'transfers_total'),
		       transfer_count, total_supply::TEXT, holder_count
		FROM tokens WHERE address = $1`, retentionToken).Scan(&c.blocks, &c.txs, &c.transfers, &c.tokenTransfers, &c.supply, &c.holders))
	return c
}

// A pruned database in state (d) of the re-seed test (the legacy range past the watermark, still attached), with
// every counter the re-seed owns knocked off; the want is what an exact recount gives.
func faultPrunedWithWrongCounters(t *testing.T) (*DB, *faultkit.Proxy, string, faultCounters) {
	t.Helper()
	d, proxy, url := faultDB(t)
	ctx := context.Background()
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	seedHistory(t, d, 0, 40_001, 1)
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 100_001, cfg)
	seedHistory(t, d, 40_002, 80_001, 1)
	closeLedger(t, d)
	_, err := d.pool.Exec(ctx, `SELECT prune_advance('legacy')`)
	require.NoError(t, err)
	require.NoError(t, d.Reconcile(ctx))
	want := faultRead(t, d)
	require.Equal(t, wantTotals(80_002), want.retentionTotals)
	_, err = d.pool.Exec(ctx, `
		UPDATE chain_counters SET count = count + 1000;
		UPDATE tokens SET transfer_count = transfer_count + 5, total_supply = total_supply + 9, holder_count = holder_count + 3;
		DELETE FROM token_balances_current`)
	require.NoError(t, err)
	return d, proxy, url, want
}

// Each statement of the startup reconcile, faulted, then a restart with the fault gone: the faulted start returns an
// error (main exits, the pod restarts) or leaves the counters exact, and the next start leaves them exact.
func TestFaultReconcileFaultAtEachStatementThenRestart(t *testing.T) {
	steps := map[string]string{
		"list-pruned":    "FROM history_partitions p CROSS JOIN unnest(history_tables())",
		"count-blocks":   `SELECT COUNT(*) FROM "blocks"`,
		"count-legacy":   `SELECT COUNT(*) FROM "transactions_legacy"`,
		"chain-write":    "INSERT INTO chain_counters (name, count, updated_at) VALUES",
		"token-totals":   "SELECT token_address, SUM(cnt)::BIGINT",
		"token-write":    "transfer_count = s.cnt",
		"token-zero":     "UPDATE tokens SET transfer_count = 0",
		"tbc-reconcile":  "INSERT INTO token_balances_current (token_address, address, balance, block_number)",
		"holders-reseed": "Startup re-seed of tokens.holder_count",
	}
	actions := map[string]faultkit.Action{"lost-ack": faultkit.LostAck, "fail-commit": faultkit.FailCommit, "kill-before": faultkit.KillBefore}
	for sname, match := range steps {
		for aname, action := range actions {
			t.Run(sname+"/"+aname, func(t *testing.T) {
				_, proxy, _, want := faultPrunedWithWrongCounters(t)
				ctx := context.Background()
				r := proxy.Arm(faultkit.Rule{Match: match, Action: action})
				first := faultOpen(t, proxy.URL())
				err := first.Reconcile(ctx)
				fired := proxy.Fired(r)
				proxy.Disarm()
				t.Logf("%s %s: fired=%d reconcile err=%v", sname, aname, fired, err)
				require.Equal(t, 1, fired, "the fault fired")
				if proxy.Inapplicable(r) {
					t.Skip("the statement runs outside a transaction: there is no COMMIT to fail")
				}
				if err == nil {
					require.Equal(t, want, faultRead(t, faultOpen(t, proxy.URL())), "a start that reports success leaves exact counters")
				}
				restart := faultOpen(t, proxy.URL())
				require.NoError(t, restart.Reconcile(ctx), "the next start, the fault gone")
				require.Equal(t, want, faultRead(t, restart))
				var tbc int64
				require.NoError(t, restart.pool.QueryRow(ctx, `SELECT count(*) FROM token_balances_current`).Scan(&tbc))
				require.EqualValues(t, 1, tbc, "token_balances_current rebuilt")
			})
		}
	}
}

// The startup reconcile with a consumer's long REPEATABLE READ reader on a history table and on tokens: it does not
// wait on it, and counts exactly.
func TestFaultReconcileUnderALongReader(t *testing.T) {
	_, proxy, url, want := faultPrunedWithWrongCounters(t)
	end1 := faultkit.LongReader(t, url, "transactions")
	end2 := faultkit.LongReader(t, url, "tokens")
	defer end1()
	defer end2()
	d := faultOpen(t, proxy.URL())
	require.NoError(t, d.Reconcile(context.Background()))
	require.Equal(t, want, faultRead(t, d))
}

// Migrations and the index builds of startup, killed or with a lost ack, then a restart.
func TestFaultStartupIndexBuildsFaultThenRestart(t *testing.T) {
	for _, step := range []struct{ name, match string }{
		{"tx-hash-indexes", "CREATE INDEX ON"},
		{"contracts-block-index", "idx_contracts_block_number"},
	} {
		for aname, action := range map[string]faultkit.Action{"lost-ack": faultkit.LostAck, "kill-before": faultkit.KillBefore} {
			t.Run(step.name+"/"+aname, func(t *testing.T) {
				d, proxy, _ := faultDB(t)
				ctx := context.Background()
				seedHistory(t, d, 0, 2_000, 1)
				r := proxy.Arm(faultkit.Rule{Match: step.match, Action: action})
				first := faultOpen(t, proxy.URL())
				var err error
				if step.name == "tx-hash-indexes" {
					err = first.EnsureTxHashIndexes(ctx)
				} else {
					err = first.EnsureContractsBlockIndex(ctx)
				}
				t.Logf("%s %s: fired=%d err=%v", step.name, aname, proxy.Fired(r), err)
				proxy.Disarm()
				restart := faultOpen(t, proxy.URL())
				require.NoError(t, restart.Migrate())
				if step.name == "tx-hash-indexes" {
					require.NoError(t, restart.EnsureTxHashIndexes(ctx), "the next start builds them")
				} else {
					require.NoError(t, restart.EnsureContractsBlockIndex(ctx), "the next start builds it")
				}
				var invalid int
				require.NoError(t, restart.pool.QueryRow(ctx, `SELECT count(*) FROM pg_index WHERE NOT indisvalid`).Scan(&invalid))
				require.Zero(t, invalid, "no invalid index left behind")
			})
		}
	}
}

// Startup's partitions-ahead after a cut, killed or with a lost ack, then a restart: the partitions exist once, and
// a block above the old newest partition is stored.
func TestFaultStartupPartitionsAheadFaultThenRestart(t *testing.T) {
	for aname, action := range map[string]faultkit.Action{"lost-ack": faultkit.LostAck, "kill-before": faultkit.KillBefore} {
		t.Run(aname, func(t *testing.T) {
			d, proxy, _ := faultDB(t)
			ctx := context.Background()
			cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
			seedHistory(t, d, 0, 40_001, 1)
			require.NoError(t, d.retentionPass(ctx, cfg))
			seedHistory(t, d, 40_002, 45_000, 1)
			r := proxy.Arm(faultkit.Rule{Match: "history_partitions_ensure", Action: action})
			_, err := faultOpen(t, proxy.URL()).EnsurePartitionsAhead(ctx, 45_000, 2_000)
			require.Equal(t, 1, proxy.Fired(r))
			require.Error(t, err)
			proxy.Disarm()
			restart := faultOpen(t, proxy.URL())
			_, err = restart.EnsurePartitionsAhead(ctx, 45_000, 2_000)
			require.NoError(t, err)
			require.Equal(t, []string{"legacy:44001:attached", "p44001:46001:attached", "p46001:48001:attached", "p48001:50001:attached"},
				partitionRanges(t, restart))
			seedHistory(t, restart, 49_000, 49_000, 1)
		})
	}
}
