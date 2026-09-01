package db

import (
	"context"
	"testing"
)

func TestBenchmarkHarnessMeasuresRealWork(t *testing.T) {
	const scale = 2_000

	d, cleanup := setupBenchDB(t)
	defer cleanup()
	seed(t, d, scale)

	ctx := context.Background()

	t.Run("seed populates every table the benchmarks read", func(t *testing.T) {
		for _, table := range []string{"blocks", "transactions", "address_stats", "contracts"} {
			if n := countRows(t, d, table); n == 0 {
				t.Errorf("seed left %s empty; every benchmark reading it measures nothing", table)
			}
		}
	})

	t.Run("address cardinality scales with the seed", func(t *testing.T) {
		want := benchAddrPoolSize(scale)
		if got := countRows(t, d, "address_stats"); got < want {
			t.Errorf("address_stats has %d rows, want at least the %d-address pool: "+
				"a fixed-size pool prices address_stats maintenance against a toy table", got, want)
		}
	})

	t.Run("every shape writes the tables it claims", func(t *testing.T) {
		for _, shape := range blockShapes {
			t.Run(shape.name, func(t *testing.T) {
				var head uint64
				if err := d.pool.QueryRow(ctx,
					`SELECT COALESCE(MAX(number), 0) FROM blocks`).Scan(&head); err != nil {
					t.Fatalf("read head: %v", err)
				}
				seededTxs, _ := benchSeedProgress(t, d)

				want := map[string]bool{"blocks": true, "transactions": true}
				want["logs"] = shape.logsPerTx > 0
				want["token_transfers"] = shape.transfersPerTx > 0
				want["internal_transactions"] = shape.internalPerTx > 0
				want["address_stats"] = !shape.skipAddressStats

				before := map[string]int{}
				for table := range want {
					before[table] = countRows(t, d, table)
				}

				batch := makeBenchBlockData(head+1, shape, benchAddrPoolSize(seededTxs))

				if !shape.skipAddressStats {
					if got := len(batch.AddressStats); got < 400 {
						t.Errorf("block presents %d distinct address deltas for %d transactions, want >=400",
							got, benchTxsPerBlock)
					}
				}

				if err := d.InsertBlockDataBatch(ctx, batch); err != nil {
					t.Fatalf("InsertBlockDataBatch: %v", err)
				}

				for table, expected := range want {
					delta := countRows(t, d, table) - before[table]
					if expected && delta == 0 {
						t.Errorf("%s unchanged: this shape claims to exercise it, so the benchmark "+
							"is timing an INSERT branch that never runs", table)
					}
					if !expected && delta != 0 {
						t.Errorf("%s gained %d rows but this shape should not write it", table, delta)
					}
				}
			})
		}
	})

	t.Run("hashes are production length and unique per shape", func(t *testing.T) {
		const realHashLen = 66
		for _, shape := range blockShapes {
			batch := makeBenchBlockData(1, shape, benchAddrPoolSize(scale))
			seen := map[string]bool{}
			for _, tx := range batch.Transactions {
				if len(tx.Hash) != realHashLen {
					t.Errorf("%s: hash %q is %d chars, want %d; a short key understates index size",
						shape.name, tx.Hash, len(tx.Hash), realHashLen)
					break
				}
				if seen[tx.Hash] {
					t.Errorf("%s: duplicate hash %q -- ON CONFLICT DO NOTHING would time a no-op",
						shape.name, tx.Hash)
					break
				}
				seen[tx.Hash] = true
			}
		}
	})

	t.Run("RefreshTokenStats has history to walk and writes a result", func(t *testing.T) {
		token := benchSeedTokenHistory(t, d)
		if err := d.RefreshTokenStats(ctx, token); err != nil {
			t.Fatalf("RefreshTokenStats: %v", err)
		}
		var transferCount, holderCount int64
		if err := d.pool.QueryRow(ctx,
			`SELECT COALESCE(transfer_count, 0), COALESCE(holder_count, 0) FROM tokens WHERE address = $1`,
			token).Scan(&transferCount, &holderCount); err != nil {
			t.Fatalf("read refreshed token: %v", err)
		}
		if transferCount == 0 || holderCount == 0 {
			t.Errorf("refreshed token has transfer_count=%d holder_count=%d; RefreshTokenStats returns "+
				"nil early when the token row is absent, so zero is what a skipped refresh looks like",
				transferCount, holderCount)
		}
	})

	t.Run("read benchmarks return rows", func(t *testing.T) {
		txs, err := d.GetTransactionsWithCategories(ctx, 10, nil)
		if err != nil {
			t.Fatalf("GetTransactionsWithCategories: %v", err)
		}
		if len(txs) == 0 {
			t.Error("GetTransactionsWithCategories returned no rows: with a nil HiddenTxTypes this " +
				"degenerates to a full scan matching nothing")
		}

		stats, err := d.GetAddressStats(ctx, benchSeededAddr(3))
		if err != nil {
			t.Fatalf("GetAddressStats: %v", err)
		}
		if stats.TxCount == 0 {
			t.Error("GetAddressStats returned an empty struct: it yields a zero value and a nil error " +
				"for a missing row, so the benchmark would be timing a miss")
		}

		contract, err := d.GetContract(ctx, "0xcontract000000000000000000000000000003")
		if err != nil {
			t.Fatalf("GetContract: %v", err)
		}
		if contract == nil || contract.Address == "" {
			t.Error("GetContract returned nothing: the benchmark is timing a miss")
		}

		if _, err := d.GetChainStats(ctx); err != nil {
			t.Fatalf("GetChainStats: %v", err)
		}
	})

	t.Run("BlockCycle does not extend the token it refreshes", func(t *testing.T) {
		batch := makeBenchBlockData(1, blockShape{
			name: "erc20", logsPerTx: 1, transfersPerTx: 1, gasPerTx: 65_000,
		}, benchAddrPoolSize(scale))
		for _, tr := range batch.Transfers {
			if tr.TokenAddress == benchRefreshToken {
				t.Fatalf("block writes transfers against %s, the token BlockCycle refreshes; "+
					"per-iteration refresh cost would grow during the run", tr.TokenAddress)
			}
		}
	})
}

func countRows(tb testing.TB, d *DB, table string) int {
	tb.Helper()
	var n int
	if err := d.pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		tb.Fatalf("count %s: %v", table, err)
	}
	return n
}
