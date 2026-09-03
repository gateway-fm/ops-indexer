package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

// Derived-counter benchmarks.
//
// BenchmarkInsertBlockDataBatch measures the inserts. This file measures what
// runs immediately after them in the indexer's per-block loop: RefreshTokenStats,
// once per token touched by the block. The two together are the real per-block
// cost, and the split matters because they scale on different axes:
//
//   - inserts scale with rows written per block, and barely at all with how much
//     history is already stored;
//   - RefreshTokenStats scales with the history of the token being refreshed,
//     and not at all with how many rows this block wrote.
//
// That second axis is why a benchmark that only covers inserts cannot detect a
// change to the derived-counter path, in either direction.
//
// RefreshTokenStats used to issue three history-proportional statements per
// ERC-20 token -- a COUNT(*) over token_transfers, a holder count over
// balances, and a mints-minus-burns SUM inside the UPDATE. Two of those are
// gone: transfer_count and total_supply are now maintained by deltas in
// InsertBlockDataBatch, so they cost nothing per block and scale with the block
// rather than with history (PRST-4493).
//
// What remains on this axis is holder_count, one indexed count over
// token_balances_current, which is unbounded by the block being processed
// because it scales with the token's holders. It is the number these
// benchmarks now exist to watch.

// benchHoldersPerTransfer and benchSnapshotsPerHolder shape the balances history
// the holder_count query has to walk. One balance row per holder would let
// DISTINCT ON stop immediately; real chains carry many snapshots per holder
// because a row is written per address per block the balance changed in.
const (
	benchHoldersPerTransfer = 10 // 1 holder per 10 transfers
	benchSnapshotsPerHolder = 2
)

// BenchmarkRefreshTokenStats measures one RefreshTokenStats call against a token
// whose history is the size of the seeded scale.
//
// Seeding all n transfers onto a single token models a chain with one dominant
// ERC-20 -- which is both the load-test shape and the worst case, and makes the
// number directly comparable to BenchmarkInsertBlockDataBatch at the same scale.
func BenchmarkRefreshTokenStats(b *testing.B) {
	runScaled(b, func(b *testing.B, d *DB) {
		ctx := context.Background()
		b.StopTimer()
		token := benchSeedTokenHistory(b, d)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			if err := d.RefreshTokenStats(ctx, token); err != nil {
				b.Fatalf("RefreshTokenStats: %v", err)
			}
		}
	})
}

// BenchmarkBlockCycle measures what the indexer actually does per block:
// InsertBlockDataBatch followed by RefreshTokenStats for each token the block
// touched. This is the number to watch when changing the ingest path -- neither
// half alone predicts it.
func BenchmarkBlockCycle(b *testing.B) {
	shape := blockShape{name: "erc20", logsPerTx: 1, transfersPerTx: 1, gasPerTx: 65_000}

	runScaled(b, func(b *testing.B, d *DB) {
		ctx := context.Background()
		b.StopTimer()

		token := benchSeedTokenHistory(b, d)

		var head uint64
		if err := d.pool.QueryRow(ctx, `SELECT COALESCE(MAX(number), 0) FROM blocks`).Scan(&head); err != nil {
			b.Fatalf("read seeded head: %v", err)
		}
		seededTxs, _ := benchSeedProgress(b, d)
		poolSize := benchAddrPoolSize(seededTxs)
		batches := make([]*BlockData, b.N)
		for i := range batches {
			batches[i] = makeBenchBlockData(head+uint64(i)+1, shape, poolSize)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			if err := d.InsertBlockDataBatch(ctx, batches[i]); err != nil {
				b.Fatalf("InsertBlockDataBatch: %v", err)
			}
			// The indexer refreshes once per distinct token in the block. The
			// block's own transfers are written against benchTransferToken while
			// this refreshes benchRefreshToken, so the history being measured
			// stays the size of the seeded scale for every iteration instead of
			// growing by 250 rows each time round.
			if err := d.RefreshTokenStats(ctx, token); err != nil {
				b.Fatalf("RefreshTokenStats: %v", err)
			}
		}

		b.StopTimer()
		if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
			blocks := float64(b.N)
			txs := blocks * benchTxsPerBlock
			b.ReportMetric(blocks/elapsed, "blocks/s")
			b.ReportMetric(txs/elapsed, "tx/s")
			b.ReportMetric(txs*float64(shape.gasPerTx)/elapsed, "gas/s")
		}
	})
}

// benchSeedTokenHistory gives one ERC-20 token a transfer and balance history
// sized to the transactions already seeded, and returns its address. Transfers
// hang off the seeded transactions because token_transfers.tx_hash is a foreign
// key into transactions(hash).
func benchSeedTokenHistory(tb testing.TB, d *DB) string {
	tb.Helper()
	ctx := context.Background()

	token := benchRefreshToken

	// The target is the number of rows seed() wrote, NOT COUNT(*) FROM
	// transactions. The two diverge as soon as a benchmark inserts anything, and
	// since each transfer hangs off the tx_hash at its own row index, counting
	// the benchmark's rows made this generate hashes seed() never wrote -- which
	// the token_transfers.tx_hash foreign key rejected, failing the whole COPY.
	nTxs, have := benchSeedProgress(tb, d)
	if nTxs == 0 {
		tb.Fatal("no seeded transactions to hang token transfers off")
	}

	// The token row must exist or RefreshTokenStats returns early and the
	// benchmark silently measures a single indexed lookup.
	if _, err := d.pool.Exec(ctx, `
		INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number)
		VALUES ($1, 'BENCH', 'Benchmark Token', 18, 'ERC20', 1)
		ON CONFLICT (address) DO NOTHING`, token); err != nil {
		tb.Fatalf("seed token: %v", err)
	}

	if have >= nTxs {
		return token
	}

	burn := "0x0000000000000000000000000000000000000000"
	// Generated one row at a time, for the same reason seed() does: at the
	// default 10M scale a materialised slice of these is several GB of client
	// memory that the benchmark has no other use for.
	if _, err := d.pool.CopyFrom(ctx,
		[]string{"token_transfers"},
		[]string{"tx_hash", "log_index", "token_address", "from_address", "to_address",
			"value", "block_number", "transfer_type", "token_type"},
		pgx.CopyFromSlice(nTxs-have, func(j int) ([]any, error) {
			i := have + j
			from := fmt.Sprintf("0xholder%035d", i/benchHoldersPerTransfer)
			to := from
			// A slice of transfers are mints from the zero address, so the
			// total_supply subquery in the UPDATE has non-trivial work rather
			// than summing a column of zeroes.
			if i%100 == 0 {
				from = burn
			}
			return []any{
				fmt.Sprintf("0xtx%062d", i), // seeded transaction hash
				0,
				token,
				from, to,
				"1000000000000000",
				int64(i/125 + 1),
				"transfer", "ERC20",
			}, nil
		}),
	); err != nil {
		tb.Fatalf("seed token_transfers: %v", err)
	}

	// The token_transfers COPY above bypasses InsertBlockDataBatch, which is
	// what maintains tokens.transfer_count and tokens.total_supply by delta.
	// Nothing recomputes them from history any more, so without this the token
	// row would report zero transfers against millions of seeded rows -- and
	// any benchmark or test reading those counters would be reading a value the
	// seeder never wrote. Same failure the token_balances_current reseed below
	// exists to prevent, one table over.
	if _, err := d.pool.Exec(ctx, `
		UPDATE tokens t SET
			transfer_count = s.cnt,
			total_supply = s.supply
		FROM (
			SELECT COUNT(*) AS cnt,
			       COALESCE(
			           SUM(CASE WHEN token_type = 'ERC20' AND from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
			         - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END),
			           0) AS supply
			FROM token_transfers WHERE token_address = $1
		) s
		WHERE t.address = $1`, token); err != nil {
		tb.Fatalf("seed token transfer counters: %v", err)
	}

	// holder_count walks every balance row for the token via DISTINCT ON, so
	// the number of snapshots per holder is part of the cost, not just the
	// number of holders.
	// Balances resume too. Generating from holder 0 every time collided with the
	// balances primary key (address, token_address, block_number) the moment a
	// larger scale ran against a database that already held a smaller one.
	nHolders := nTxs / benchHoldersPerTransfer
	haveHolders := have / benchHoldersPerTransfer
	if nHolders > haveHolders {
		if _, err := d.pool.CopyFrom(ctx,
			[]string{"balances"},
			[]string{"address", "token_address", "block_number", "balance"},
			pgx.CopyFromSlice((nHolders-haveHolders)*benchSnapshotsPerHolder, func(j int) ([]any, error) {
				h := haveHolders + j/benchSnapshotsPerHolder
				s := j % benchSnapshotsPerHolder
				return []any{
					fmt.Sprintf("0xholder%035d", h),
					token,
					int64(h*benchSnapshotsPerHolder + s + 1),
					"1000000000000000",
				}, nil
			}),
		); err != nil {
			tb.Fatalf("seed balances: %v", err)
		}
	}

	// The balances COPY above bypasses InsertBalancesBatch, which is what keeps
	// token_balances_current in step. holder_count reads that table, so without
	// this the derived-counter benchmarks would time a count over an empty
	// table and report a holder count of zero -- a benchmark measuring nothing.
	// TestBenchmarkHarnessMeasuresRealWork is what catches it.
	//
	// Idempotent, and scoped to this token, so it composes with the incremental
	// balance seeding above rather than rebuilding the whole table per top-up.
	if _, err := d.pool.Exec(ctx, `
		INSERT INTO token_balances_current (token_address, address, balance, block_number)
		SELECT DISTINCT ON (token_address, address) token_address, address, balance, block_number
		FROM balances
		WHERE token_address = $1
		ORDER BY token_address, address, block_number DESC
		ON CONFLICT (token_address, address) DO UPDATE
			SET balance = EXCLUDED.balance,
				block_number = EXCLUDED.block_number
			WHERE EXCLUDED.block_number >= token_balances_current.block_number`, token); err != nil {
		tb.Fatalf("seed token_balances_current: %v", err)
	}

	recordBenchSeedProgress(tb, d, "seeded_transfers", nTxs)

	if _, err := d.pool.Exec(ctx, "ANALYZE token_transfers, balances, tokens, token_balances_current"); err != nil {
		tb.Fatalf("analyze: %v", err)
	}
	return token
}
