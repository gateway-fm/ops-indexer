package db

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// lateBlockWithAddresses is lateBlock with its senders and recipients written to address_stats, a fifth of the
// senders as contracts.
func lateBlockWithAddresses(n int) *BlockData {
	data := lateBlock(n)
	data.SkipAddressStats = false
	data.AddressStats = map[string]*AddressStatsDelta{}
	for _, tx := range data.Transactions {
		addresses := []string{tx.From}
		if tx.To != nil {
			addresses = append(addresses, *tx.To)
		}
		for _, address := range addresses {
			if delta, ok := data.AddressStats[address]; ok {
				delta.TxCountDelta++
				continue
			}
			data.AddressStats[address] = &AddressStatsDelta{Address: address, TxCountDelta: 1, BlockNumber: uint64(n),
				IsContract: address[len(address)-1] == '5'}
		}
	}
	return data
}

// Under 021, a day's address columns count the addresses first seen from its first block up to the next
// day's first block. With late blocks a day's first block moves down, and the day before ends there, so both days'
// columns follow the stored blocks, whatever order they arrived in.
func TestDailyStatsAddressColumnsFollowLateBlocks(t *testing.T) {
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			order := append([]int{lateBlocksTotal - 1, lateBlocksTotal}, rand.New(rand.NewSource(seed)).Perm(lateBlocksTotal-2)...)
			for k, n := range order {
				if k >= 2 {
					n++
				}
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlockWithAddresses(n)))
				if k%7 == 1 {
					_, err := d.AdvanceDailyStats(ctx)
					require.NoError(t, err)
				}
			}
			advanceUntilStable(t, d)

			requireAddressColumnsFollowTheBlocks(t, d)
			requireDaysEqualRecompute(t, d, "the other columns")
		})
	}
}

// A day whose first blocks are pruned is not recounted from the blocks left: its row keeps the whole day's counts.
func TestDailyStatsKeepsADayWhoseFirstBlocksArePruned(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= lateBlocksTotal; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	var before []string
	require.NoError(t, pgxCollectText(ctx, d, `SELECT concat_ws(' ', date, total_blocks, total_transactions, cumulative_transactions) FROM daily_stats ORDER BY date`, &before))

	for _, table := range []string{"token_transfers", "transactions", "blocks"} {
		column := "block_number"
		if table == "blocks" {
			column = "number"
		}
		_, err := d.pool.Exec(ctx, `DELETE FROM `+table+` WHERE `+column+` <= 45`)
		require.NoError(t, err)
	}
	_, err := d.pool.Exec(ctx, `UPDATE prune_state SET pruned_through = 45 WHERE id = 1`)
	require.NoError(t, err)

	restarted := &DB{pool: d.pool}
	advanceUntilStable(t, restarted)
	var after []string
	require.NoError(t, pgxCollectText(ctx, d, `SELECT concat_ws(' ', date, total_blocks, total_transactions, cumulative_transactions) FROM daily_stats ORDER BY date`, &after))
	require.Equal(t, before, after, "blocks 31-45 of the second day are pruned; its row and the first day's stay whole")
}

func pgxCollectText(ctx context.Context, d *DB, query string, out *[]string) error {
	rows, err := d.pool.Query(ctx, query)
	if err != nil {
		return err
	}
	*out, err = pgx.CollectRows(rows, pgx.RowTo[string])
	return err
}

// requireAddressColumnsFollowTheBlocks compares every day's address columns with 021's definition, counted from the
// stored blocks and address_stats: a day's addresses are those first seen from its first block up to the next day's.
func requireAddressColumnsFollowTheBlocks(t *testing.T, d *DB) {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `
		WITH spans AS (
			SELECT date, lo, COALESCE(lead(lo) OVER (ORDER BY date), (SELECT max(number) + 1 FROM blocks)) AS hi
			FROM (SELECT daily_stats_day(timestamp) AS date, min(number) AS lo FROM blocks GROUP BY 1) f
		)
		SELECT s.date::TEXT,
		       count(a.address) FILTER (WHERE a.first_seen >= s.lo AND a.first_seen < s.hi),
		       count(a.address) FILTER (WHERE a.is_contract AND a.first_seen >= s.lo AND a.first_seen < s.hi),
		       count(a.address) FILTER (WHERE a.first_seen < s.hi),
		       count(a.address) FILTER (WHERE a.is_contract AND a.first_seen < s.hi),
		       d.new_addresses, d.new_contracts, d.cumulative_addresses, d.cumulative_contracts
		FROM spans s CROSS JOIN address_stats a LEFT JOIN daily_stats d ON d.date = s.date
		GROUP BY s.date, d.new_addresses, d.new_contracts, d.cumulative_addresses, d.cumulative_contracts
		ORDER BY s.date`)
	require.NoError(t, err)
	defer rows.Close()
	days := 0
	for rows.Next() {
		var date string
		var want [4]int64
		var got [4]*int64
		require.NoError(t, rows.Scan(&date, &want[0], &want[1], &want[2], &want[3], &got[0], &got[1], &got[2], &got[3]))
		require.NotNil(t, got[0], "no row for %s", date)
		require.Equal(t, want, [4]int64{*got[0], *got[1], *got[2], *got[3]},
			"%s: new addresses, new contracts, cumulative addresses, cumulative contracts", date)
		days++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 4, days)
}

// With no rebuild after catch-up, repair-address-stats is what corrects address_stats a skipped catch-up
// left short, and it moves first_seen and creates rows. The days' address columns follow it once it finishes.
func TestRepairAddressStatsRecountsTheDaysAddressColumns(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= lateBlocksTotal; n++ {
		data := lateBlockWithAddresses(n)
		data.SkipAddressStats = n < 60
		require.NoError(t, d.InsertBlockDataBatch(ctx, data))
	}
	advanceUntilStable(t, d)
	_, err := d.StageAddressStatsRepair(ctx, false)
	require.NoError(t, err)
	for done := false; !done; {
		_, _, done, err = d.ApplyAddressStatsRepairBatch(ctx, 7)
		require.NoError(t, err)
	}
	requireAddressColumnsFollowTheBlocks(t, d)
}
