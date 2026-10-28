package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A chain reset (FORCE_REINDEX) must also reset the incremental daily_stats state, or the re-indexed
// chain gets no daily_stats until it passes the old chain's height.
func TestWipeAllDataResetsIncrementalDailyStats(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	midnight := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC).Unix()
	seed := func(from, to int, chain string) {
		for n := from; n <= to; n++ {
			_, err := d.pool.Exec(ctx, `
				INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
				VALUES ($1, $2, $3, $4, 21000, 30000000, 1)`,
				n, fmt.Sprintf("0x%s%062x", chain, n), fmt.Sprintf("0x%s%062x", chain, n-1), midnight+int64(n)*3)
			require.NoError(t, err)
			_, err = d.pool.Exec(ctx, `
				INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value,
					gas_used, gas_price, nonce, tx_type, input_data, status)
				VALUES ($1, $2, $3, 0, $4, NULL, 0, 21000, 1000, 0, 0, '0x', 1)`,
				fmt.Sprintf("0x%s%062x", chain, 1000+n), n, midnight+int64(n)*3, fmt.Sprintf("0x%040x", n%7+1))
			require.NoError(t, err)
		}
	}
	seed(1, 40, "aa")
	_, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)

	require.NoError(t, d.WipeAllData(ctx))
	seed(1, 10, "bb")
	upto, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 10, upto, "the re-indexed chain is aggregated from its own blocks")

	day := time.Unix(midnight, 0).UTC()
	want, err := d.ComputeDailyStats(ctx, day)
	require.NoError(t, err)
	got, err := d.GetDailyStatsForDate(ctx, day)
	require.NoError(t, err)
	require.NotNil(t, got, "the re-indexed chain has its daily_stats row")
	require.Equal(t, want.TotalBlocks, got.TotalBlocks)
	require.Equal(t, want.TotalTransactions, got.TotalTransactions)
	require.Equal(t, want.CumulativeTransactions, got.CumulativeTransactions)
}
