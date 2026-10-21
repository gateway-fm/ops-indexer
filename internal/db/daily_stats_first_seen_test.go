package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// seedHistory stamps block b at 1700000000 + b: blocks 0..6,399 are on 2023-11-14, 6,400.. on 2023-11-15.
// address_stats.first_seen is a block number.
func seedFirstSeen(t *testing.T, d *DB) {
	t.Helper()
	seedHistory(t, d, 0, 10_000, 1)
	_, err := d.pool.Exec(context.Background(), `
		INSERT INTO address_stats (address, tx_count, first_seen, last_seen, is_contract)
		VALUES ('0xa', 1, 100, 100, true), ('0xb', 1, 6399, 6399, false),
		       ('0xc', 1, 6400, 6400, false), ('0xd', 1, 9000, 9000, true)
		ON CONFLICT (address) DO UPDATE SET first_seen = EXCLUDED.first_seen, is_contract = EXCLUDED.is_contract`)
	require.NoError(t, err)
}

type dayAddresses struct{ newAddresses, newContracts, cumulativeAddresses, cumulativeContracts int64 }

func dailyAddresses(t *testing.T, d *DB, date string) dayAddresses {
	t.Helper()
	var r dayAddresses
	require.NoError(t, d.pool.QueryRow(context.Background(), `
		SELECT new_addresses, new_contracts, cumulative_addresses, cumulative_contracts FROM daily_stats WHERE date = $1`, date).
		Scan(&r.newAddresses, &r.newContracts, &r.cumulativeAddresses, &r.cumulativeContracts))
	return r
}

var (
	firstDay  = dayAddresses{newAddresses: 2, newContracts: 1, cumulativeAddresses: 2, cumulativeContracts: 1}
	secondDay = dayAddresses{newAddresses: 2, newContracts: 1, cumulativeAddresses: 4, cumulativeContracts: 2}
)

func TestDailyStatsCountAddressesByTheDayOfTheirFirstBlock(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	seedFirstSeen(t, d)
	_, err := d.pool.Exec(ctx, `UPDATE daily_stats_watermark SET last_block = -1`)
	require.NoError(t, err)

	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.Equal(t, firstDay, dailyAddresses(t, d, "2023-11-14"))
	require.Equal(t, secondDay, dailyAddresses(t, d, "2023-11-15"))

	for date, want := range map[string]dayAddresses{"2023-11-14": firstDay, "2023-11-15": secondDay} {
		day, _ := time.Parse("2006-01-02", date)
		s, err := d.ComputeDailyStats(ctx, day)
		require.NoError(t, err)
		require.Equal(t, want, dayAddresses{int64(s.NewAddresses), int64(s.NewContracts), int64(s.CumulativeAddresses), int64(s.CumulativeContracts)},
			"ComputeDailyStats %s", date)
	}
}

func TestMigration021CorrectsDailyStatsAddressColumns(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	m, release := testMigrator(t, d)
	defer release()
	require.NoError(t, m.MigrateTo(ctx, 20))
	seedFirstSeen(t, d)
	_, err := d.pool.Exec(ctx, `
		UPDATE daily_stats_watermark SET last_block = -1;
		SELECT daily_stats_advance()`)
	require.NoError(t, err)
	require.Equal(t, dayAddresses{0, 0, 4, 2}, dailyAddresses(t, d, "2023-11-14"), "the defect: block numbers compared with epoch seconds")

	require.NoError(t, m.Migrate(ctx))
	require.Equal(t, firstDay, dailyAddresses(t, d, "2023-11-14"))
	require.Equal(t, secondDay, dailyAddresses(t, d, "2023-11-15"))
}

func TestMigration021RefusesOncePruned(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	m, release := testMigrator(t, d)
	defer release()
	require.NoError(t, m.MigrateTo(ctx, 20))
	_, err := d.pool.Exec(ctx, `UPDATE prune_state SET pruned_through = 1000 WHERE id = 1`)
	require.NoError(t, err)
	require.ErrorContains(t, m.Migrate(ctx), "history at or below block 1000 is pruned")
}
