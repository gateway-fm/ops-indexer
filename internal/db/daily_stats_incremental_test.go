package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The incrementally maintained rows must equal what ComputeDailyStats returns for the same day, after
// advances that split days at arbitrary blocks, across midnight, and after a reverted block is re-indexed.
func TestDailyStatsIncrementalEqualsRecompute(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	midnight := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC).Unix()
	// Blocks 1..40, 3 s apart, the 20th lands just before midnight.
	ts := func(n int) int64 { return midnight - 20*3 + int64(n)*3 }
	seedBlock := func(n int) {
		var size any = 400 + n*7
		if n%9 == 0 {
			size = nil
		}
		_, err := d.pool.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, miner, size,
				difficulty, total_difficulty, nonce, extra_data, state_root, transactions_root, receipts_root)
			VALUES ($1, $2, $3, $4, $5, 30000000, $6, '0xminer', $7, '0', '0', '0x0', '', '', '', '')`,
			n, fmt.Sprintf("0xblk%061x", n), fmt.Sprintf("0xblk%061x", n-1), ts(n), 21000*(n%4+1), n%4, size)
		require.NoError(t, err)
		for i := 0; i < n%4; i++ {
			hash := fmt.Sprintf("0x%060x%04x", n, i)
			from := fmt.Sprintf("0x%040x", 1+(n*7+i)%11)
			var to any = fmt.Sprintf("0x%040x", 100+(n+i)%5)
			if (n+i)%6 == 0 {
				to = nil
			}
			_, err := d.pool.Exec(ctx, `
				INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value,
					gas_used, gas_price, nonce, tx_type, input_data, status)
				VALUES ($1, $2, $3, $4, $5, $6, 0, 21000, $7, 0, 0, '0x', $8)`,
				hash, n, ts(n), i, from, to, 1_000_000_007+int64(n*i*13), (n+i)%5/4^1)
			require.NoError(t, err)
			if i == 0 {
				_, err := d.pool.Exec(ctx, `
					INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value,
						block_number, timestamp, transfer_type, token_type)
					VALUES ($1, 0, '0xtoken', $2, '0xrecipient', 1, $3, $4, 'transfer', 'ERC20')`, hash, from, n, ts(n))
				require.NoError(t, err)
			}
		}
	}
	equalsRecompute := func(label string) {
		t.Helper()
		for _, day := range []time.Time{time.Unix(midnight-86400, 0).UTC(), time.Unix(midnight, 0).UTC()} {
			want, err := d.ComputeDailyStats(ctx, day)
			require.NoError(t, err)
			got, err := d.GetDailyStatsForDate(ctx, day)
			require.NoError(t, err)
			require.NotNil(t, got, "%s: no row for %s", label, want.Date)
			require.Equal(t, *want, *got, "%s: %s", label, want.Date)
		}
	}

	for n := 1; n <= 13; n++ {
		seedBlock(n)
	}
	upto, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 13, upto)

	for n := 14; n <= 27; n++ {
		seedBlock(n)
	}
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	for n := 28; n <= 40; n++ {
		if n == 33 {
			continue // a gap: the watermark must stop at 32
		}
		seedBlock(n)
	}
	upto, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 32, upto)
	seedBlock(33)
	upto, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 40, upto)
	equalsRecompute("after advancing in four steps across midnight")

	// Revert the top three blocks, as a reorg does, then index a different block 38 and the rest again.
	for n := 40; n >= 38; n-- {
		require.NoError(t, d.DeleteBlock(ctx, uint64(n)))
	}
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	equalsRecompute("after a revert of 38..40")
	for n := 38; n <= 40; n++ {
		seedBlock(n)
	}
	_, err = d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	equalsRecompute("after re-indexing 38..40")
}
