package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Writing a stored block again with rows it was missing changes its day's totals, so the day is recounted at the
// next advance, as for a block stored late.
func TestDailyStatsRecountsADayWhenAStoredBlockGainsRows(t *testing.T) {
	for _, c := range []struct {
		name    string
		block   int
		partial func(*BlockData)
	}{
		{"header stored without its transactions", 50, func(b *BlockData) { b.Transactions, b.Transfers = nil, nil }},
		{"transactions stored without their transfers", 49, func(b *BlockData) { b.Transfers = nil }},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			for n := 1; n <= lateBlocksTotal; n++ {
				b := lateBlock(n)
				if n == c.block {
					c.partial(b)
				}
				require.NoError(t, d.InsertBlockDataBatch(ctx, b))
			}
			advanceUntilStable(t, d)

			full := lateBlock(c.block)
			require.NotEmpty(t, full.Transactions)
			require.NotEmpty(t, full.Transfers)
			require.NoError(t, d.InsertBlockDataBatch(ctx, full))
			advanceUntilStable(t, d)

			requireDaysEqualRecompute(t, d, c.name)
		})
	}
}
