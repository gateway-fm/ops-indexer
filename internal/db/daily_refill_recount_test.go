package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// PRST-5436, a production deployment after the refill: Oct 18 and 19 kept their old address columns, and Oct 17 counted their new
// addresses as its own, because the recount only placed days with a recorded first block and ended a day at the
// next recorded one. Two refilled days with untouched days between: every day's new_* and cumulative_* equal a
// recount from address_stats over the true day boundaries.
func TestRefillRecountsEveryDaysAddressColumns(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	day := func(i int) time.Time { return time.Date(2026, 10, 16+i, 0, 0, 0, 0, time.UTC) }
	lengths := []uint64{100, 60, 75, 90, 40}
	firsts := []uint64{1000}
	for _, n := range lengths {
		firsts = append(firsts, firsts[len(firsts)-1]+n)
	}
	watermark := firsts[len(firsts)-1] - 1
	for i, n := range lengths {
		_, err := d.pool.Exec(ctx, `INSERT INTO daily_stats (date, total_blocks) VALUES ($1, $2)`, day(i), int64(n))
		require.NoError(t, err)
		var first any = int64(firsts[i])
		if i == 1 || i == 2 {
			first = nil
		}
		_, err = d.pool.Exec(ctx, `INSERT INTO daily_stats_acc (date, first_block, blocks) VALUES ($1, $2, $3)`, day(i), first, int64(n))
		require.NoError(t, err)
	}
	_, err := d.pool.Exec(ctx, `INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, $1)`, int64(watermark))
	require.NoError(t, err)
	for k := uint64(0); k < 300; k++ {
		seen := 990 + k*13%380
		_, err := d.pool.Exec(ctx, `INSERT INTO address_stats (address, first_seen, last_seen, is_contract) VALUES ($1, $2, $2, $3)`,
			fmt.Sprintf("0x%040x", k), int64(seen), k%7 == 0)
		require.NoError(t, err)
	}

	refilled := []RefillDay{
		{Date: day(0), FirstBlock: firsts[0], LastBlock: firsts[1] - 1, Blocks: int64(lengths[0])},
		{Date: day(3), FirstBlock: firsts[3], LastBlock: firsts[4] - 1, Blocks: int64(lengths[3])},
	}
	rows, err := d.WriteRefilledDailyStats(ctx, refilled, false)
	require.NoError(t, err)
	require.Len(t, rows, 5)

	for i, r := range rows {
		lo, hi := firsts[i], firsts[i+1]
		var want [4]int64
		require.NoError(t, d.pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE first_seen >= $1 AND first_seen < $2), count(*) FILTER (WHERE is_contract AND first_seen >= $1 AND first_seen < $2),
			       count(*) FILTER (WHERE first_seen < $2), count(*) FILTER (WHERE is_contract AND first_seen < $2)
			FROM address_stats`, int64(lo), int64(hi)).Scan(&want[0], &want[1], &want[2], &want[3]))
		require.Equal(t, want, [4]int64{int64(r.NewAddresses), int64(r.NewContracts), r.CumulativeAddresses, r.CumulativeContracts},
			"%s: new and cumulative addresses and contracts over [%d, %d)", r.Date, lo, hi)
	}
	var recorded int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM daily_stats_acc WHERE first_block IS NOT NULL`).Scan(&recorded))
	require.Equal(t, 5, recorded, "the inferred first blocks are recorded for later advances")
}

// Migration 021 counts each stored day's new and cumulative addresses from the address_stats it finds, and
// the rebuild after the initial catch-up then rewrites address_stats. The days follow the rebuilt address_stats, so
// every upgrade route ends with the same days.
func TestAddressStatsRebuildRecountsTheDaysAddressColumns(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	day := func(i int) time.Time { return time.Date(2026, 10, 1+i, 0, 0, 0, 0, time.UTC) }
	firstSeen := []int64{2, 5, 12, 19, 20, 21, 25, 30}
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT n, 'b' || n, 'b' || (n - 1), $1 + 8640 * (n - 1), 0, 30000000, 0 FROM generate_series(1, 30) n`, day(0).Unix())
	require.NoError(t, err)
	for k, b := range firstSeen {
		addr := fmt.Sprintf("0x%040x", k+1)
		_, err := d.pool.Exec(ctx, `
			INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
			VALUES ($1, $2, 0, $3, $3, 0, 21000, 1, '0x', 1, 0)`, fmt.Sprintf("0xt%d", k), b, addr)
		require.NoError(t, err)
		_, err = d.pool.Exec(ctx, `INSERT INTO address_stats (address, tx_count, first_seen, last_seen) VALUES ($1, 1, 1, 1)`, addr)
		require.NoError(t, err)
	}
	for i := 0; i < 3; i++ {
		_, err := d.pool.Exec(ctx, `INSERT INTO daily_stats (date, total_blocks, new_addresses, cumulative_addresses) VALUES ($1, 10, 8, 8)`, day(i))
		require.NoError(t, err)
		_, err = d.pool.Exec(ctx, `INSERT INTO daily_stats_acc (date, first_block, blocks) VALUES ($1, $2, 10)`, day(i), 1+10*i)
		require.NoError(t, err)
	}
	_, err = d.pool.Exec(ctx, `INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, 30) ON CONFLICT (id) DO UPDATE SET last_block = 30`)
	require.NoError(t, err)

	require.NoError(t, d.RebuildAddressStats(ctx))
	for i, want := range [][2]int{{2, 2}, {3, 5}, {3, 8}} {
		var n, cum int
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT new_addresses, cumulative_addresses FROM daily_stats WHERE date = $1`, day(i)).Scan(&n, &cum))
		require.Equal(t, want, [2]int{n, cum}, "%s: new and cumulative addresses", day(i).Format("2006-01-02"))
	}
}
