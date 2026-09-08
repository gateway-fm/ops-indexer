package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHolderCount_IncrementalAgreesWithAbsolute pins the PRST-4669 invariant:
// after every InsertBalancesBatch, tokens.holder_count equals the absolute
// count over token_balances_current. The fixture drives every membership
// transition -- new positive, positive to zero, zero back to positive, only
// ever zero, a stale block the guard must reject, a same-block re-delivery --
// out of block order, because that is how the balance workers deliver them.
//
// Needs a database (setupBenchDB / BENCH_DATABASE_URL); skipped otherwise.
func TestHolderCount_IncrementalAgreesWithAbsolute(t *testing.T) {
	d, cleanup := setupBenchDB(t)
	defer cleanup()
	ctx := context.Background()

	f, drop := newBalFixture(t, d, 4669, 4_669_100_000)
	defer drop()
	// The delta lands on the tokens row, so the fixture needs one.
	_, err := d.pool.Exec(ctx, `
		INSERT INTO tokens (address, symbol, name, decimals, token_type, block_number, holder_count)
		VALUES ($1, 'HC', 'holder count fixture', 18, 'ERC20', $2, 0)
		ON CONFLICT (address) DO UPDATE SET holder_count = 0`, f.token, f.base)
	require.NoError(t, err)
	defer func() { _, _ = d.pool.Exec(ctx, `DELETE FROM tokens WHERE address = $1`, f.token) }()

	stored := func() int64 {
		var n int64
		require.NoError(t, d.pool.QueryRow(ctx,
			`SELECT holder_count FROM tokens WHERE address = $1`, f.token).Scan(&n))
		return n
	}
	check := func(step string, want int64) {
		t.Helper()
		abs, err := d.countTokenHolders(ctx, f.token)
		require.NoError(t, err)
		require.Equal(t, want, abs, "%s: absolute count", step)
		require.Equal(t, abs, stored(), "%s: incremental holder_count must equal the absolute count", step)
		require.Equal(t, f.holdersViaHistory(t, d), abs, "%s: cache vs history", step)
	}

	// 1 and 3 become holders; 2 is written at zero (never a holder); 4 zero.
	f.writeBal(t, d,
		[3]int64{3, 3, 500},
		[3]int64{1, 1, 1000},
		[3]int64{2, 2, 0},
		[3]int64{4, 1, 0},
	)
	check("initial", 2)

	// 3 goes to zero at a later block: decrement.
	f.writeBal(t, d, [3]int64{3, 4, 0})
	check("3 to zero", 1)

	// Stale write for 3 (block 2 < 4): guard rejects, count unchanged.
	f.writeBal(t, d, [3]int64{3, 2, 900})
	check("stale rejected", 1)

	// 3 comes back at block 5: increment. 2 becomes a holder: increment.
	f.writeBal(t, d, [3]int64{3, 5, 7}, [3]int64{2, 5, 1})
	check("3 back, 2 in", 3)

	// Same-block re-delivery with a different value: last write wins, no
	// membership change, count unchanged.
	f.writeBal(t, d, [3]int64{2, 5, 2})
	check("re-delivery", 3)

	// Two rows for one key in one batch collapse to the highest block.
	f.writeBal(t, d, [3]int64{1, 6, 0}, [3]int64{1, 7, 5})
	check("collapsed run", 3)

	// Everyone out.
	f.writeBal(t, d, [3]int64{1, 8, 0}, [3]int64{2, 8, 0}, [3]int64{3, 8, 0})
	check("all zero", 0)

	// The re-seed is a no-op on a correct value and repairs a wrong one.
	_, err = d.pool.Exec(ctx, `UPDATE tokens SET holder_count = 41 WHERE address = $1`, f.token)
	require.NoError(t, err)
	require.NoError(t, d.ReseedHolderCounts(ctx))
	require.Equal(t, int64(0), stored(), "re-seed must repair a wrong holder_count")
	_ = fmt.Sprintf
}
