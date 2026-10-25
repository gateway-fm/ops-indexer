package db

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// lateBlocksMidnight is the start of the third of the four days the late-block fixture spans.
var lateBlocksMidnight = time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC).Unix()

const lateBlocksTotal = 120

// lateBlock is block n of 120 spread over four UTC days, 30 a day, with 0-3 transactions, one token transfer
// on the first of them, a NULL recipient on some and a failed status on some.
func lateBlock(n int) *BlockData {
	ts := uint64(lateBlocksMidnight - 2*86400 + int64(n-1)*(86400/30) + 7)
	data := &BlockData{
		Block: &types.Block{Number: uint64(n), Hash: fmt.Sprintf("0xlate%059x", n), ParentHash: fmt.Sprintf("0xlate%059x", n-1),
			Timestamp: ts, GasUsed: uint64(21000 * (n%4 + 1)), GasLimit: 30_000_000, TransactionCount: n % 4, Size: uint64(400 + n*7)},
		SkipAddressStats: true,
	}
	for i := 0; i < n%4; i++ {
		hash := fmt.Sprintf("0x%060x%04x", n, i)
		var to *string
		if (n+i)%6 != 0 {
			recipient := fmt.Sprintf("0x%040x", 100+(n+i)%5)
			to = &recipient
		}
		from := fmt.Sprintf("0x%040x", 1+(n*7+i)%11)
		data.Transactions = append(data.Transactions, &types.Transaction{Hash: hash, BlockNumber: uint64(n), BlockTimestamp: ts,
			TxIndex: i, From: from, To: to, Value: "0", GasUsed: 21000, GasPrice: uint64(1_000_000_007 + n*i*13),
			InputData: "0x", Status: (n+i)%5/4 ^ 1})
		if i == 0 {
			data.Transfers = append(data.Transfers, &types.TokenTransfer{TxHash: hash, TokenAddress: "0xtoken", From: from,
				To: "0xrecipient", Value: "1", BlockNumber: uint64(n), Timestamp: &ts, TransferType: "transfer", TokenType: "ERC20"})
		}
	}
	return data
}

// requireDaysEqualRecompute compares every day of the fixture with ComputeDailyStats, on the columns blocks and
// transactions determine. The address columns read address_stats, which these blocks do not write.
func requireDaysEqualRecompute(t *testing.T, d *DB, label string) {
	t.Helper()
	ctx := context.Background()
	for day := 0; day < 4; day++ {
		date := time.Unix(lateBlocksMidnight+int64(day-2)*86400, 0).UTC()
		want, err := d.ComputeDailyStats(ctx, date)
		require.NoError(t, err)
		got, err := d.GetDailyStatsForDate(ctx, date)
		require.NoError(t, err)
		require.NotNil(t, got, "%s: no row for %s", label, want.Date)
		want.NewAddresses, want.NewContracts, want.CumulativeAddresses, want.CumulativeContracts = 0, 0, 0, 0
		got.NewAddresses, got.NewContracts, got.CumulativeAddresses, got.CumulativeContracts = 0, 0, 0, 0
		require.Equal(t, *want, *got, "%s: %s", label, want.Date)
	}
}

func advanceUntilStable(t *testing.T, d *DB) {
	t.Helper()
	for {
		before, err := d.AdvanceDailyStats(context.Background())
		require.NoError(t, err)
		after, err := d.AdvanceDailyStats(context.Background())
		require.NoError(t, err)
		if after == before {
			return
		}
	}
}

// A block stored below the incremental watermark was never counted. Realtime stores the head first
// on a fresh sync, the first advance starts at the head's day, and catch-up then stores every earlier block below
// the watermark: day 1 came out at 1-2 blocks. Every block now counts in its day, and in every later day's
// cumulative_transactions, exactly once, whatever order the blocks commit in.
func TestDailyStatsCountsEveryBlockOnceInAnyCommitOrder(t *testing.T) {
	ascending := func(from, to int) []int {
		var out []int
		for n := from; n <= to; n++ {
			out = append(out, n)
		}
		return out
	}
	headFirst := func(rest []int) []int { return append([]int{lateBlocksTotal - 1, lateBlocksTotal}, rest...) }
	var descendingChunks []int
	for hi := lateBlocksTotal - 2; hi >= 1; hi -= 25 {
		descendingChunks = append(descendingChunks, ascending(max(1, hi-24), hi)...)
	}
	orders := map[string][]int{
		"in order":                          ascending(1, lateBlocksTotal),
		"head first, then catch-up upwards": headFirst(ascending(1, lateBlocksTotal-2)),
		"head first, then catch-up in descending chunks": headFirst(descendingChunks),
	}
	for seed := int64(1); seed <= 3; seed++ {
		shuffled := ascending(1, lateBlocksTotal)
		rand.New(rand.NewSource(seed)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		orders[fmt.Sprintf("shuffled, seed %d", seed)] = shuffled
	}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			d, cleanup := setupTestDB(t)
			defer cleanup()
			ctx := context.Background()
			for k, n := range order {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
				if k%7 == 1 {
					_, err := d.AdvanceDailyStats(ctx)
					require.NoError(t, err)
				}
			}
			advanceUntilStable(t, d)
			requireDaysEqualRecompute(t, d, name)
		})
	}
}

// A restart forgets which days took late blocks; the first advance after it finds them from the stored blocks.
func TestDailyStatsCountsLateBlocksStoredBeforeARestart(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := lateBlocksTotal - 5; n <= lateBlocksTotal; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	for n := 1; n < lateBlocksTotal-5; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}

	restarted := &DB{pool: d.pool}
	advanceUntilStable(t, restarted)
	requireDaysEqualRecompute(t, restarted, "after a restart")
}

// Four writers store the blocks in interleaved order while advances run between and during their commits.
func TestDailyStatsCountsEveryBlockOnceWithConcurrentWritersAndAdvances(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	order := rand.New(rand.NewSource(7)).Perm(lateBlocksTotal)
	var writers sync.WaitGroup
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for k := w; k < len(order); k += 4 {
				require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(order[k]+1)))
			}
		}(w)
	}
	stop := make(chan struct{})
	advancing := make(chan struct{})
	go func() {
		defer close(advancing)
		for {
			select {
			case <-stop:
				return
			default:
				_, err := d.AdvanceDailyStats(ctx)
				require.NoError(t, err)
			}
		}
	}()
	writers.Wait()
	close(stop)
	<-advancing
	advanceUntilStable(t, d)
	requireDaysEqualRecompute(t, d, "concurrent writers")
}

// A reorg that reverts blocks which arrived below the watermark subtracts them, and their replacements count once.
func TestDailyStatsRevertsLateBlocks(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := lateBlocksTotal - 1; n <= lateBlocksTotal; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	for n := 1; n < lateBlocksTotal-1; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	for n := lateBlocksTotal; n >= 50; n-- {
		require.NoError(t, d.DeleteBlock(ctx, uint64(n)))
	}
	advanceUntilStable(t, d)
	for n := 50; n <= lateBlocksTotal; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	requireDaysEqualRecompute(t, d, "after reverting and re-indexing 50..120")
}

// 012 sets a new day's count of earlier transactions from the day before only if that day had one before
// the advance, so when one advance creates several days, every day after the first missed the transactions of the
// days before it in cumulative_transactions.
func TestDailyStatsCumulativeTransactionsWhenOneAdvanceCrossesSeveralDays(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for n := 1; n <= 10; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	advanceUntilStable(t, d)
	for n := 11; n <= lateBlocksTotal; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, lateBlock(n)))
	}
	upto, err := d.AdvanceDailyStats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, lateBlocksTotal, upto, "one advance over three days")
	requireDaysEqualRecompute(t, d, "after one advance over three days")
}

// requireStoredDaysEqualRecompute compares every day that holds a stored block with ComputeDailyStats, on the
// columns blocks and transactions determine.
func requireStoredDaysEqualRecompute(t *testing.T, d *DB, label string) {
	t.Helper()
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, `SELECT DISTINCT timestamp / 86400 FROM blocks ORDER BY 1`)
	require.NoError(t, err)
	days, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	require.NoError(t, err)
	for _, day := range days {
		date := time.Unix(day*86400, 0).UTC()
		want, err := d.ComputeDailyStats(ctx, date)
		require.NoError(t, err)
		got, err := d.GetDailyStatsForDate(ctx, date)
		require.NoError(t, err)
		require.NotNil(t, got, "%s: no row for %s", label, want.Date)
		want.NewAddresses, want.NewContracts, want.CumulativeAddresses, want.CumulativeContracts = 0, 0, 0, 0
		got.NewAddresses, got.NewContracts, got.CumulativeAddresses, got.CumulativeContracts = 0, 0, 0, 0
		require.Equal(t, *want, *got, "%s: %s", label, want.Date)
	}
}

// The block-time pair of a late block's successor belongs to the successor's day, which need not be the
// next day: after an empty day it was never recounted, and kept the pair it had missed.
func TestDailyStatsRecountsTheSuccessorsDayAcrossAnEmptyDay(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	block := func(n int) *BlockData {
		ts := uint64(lateBlocksMidnight - 86400 + int64(n)*600)
		if n > 20 {
			ts = uint64(lateBlocksMidnight + 86400 + int64(n-20)*600)
		}
		return &BlockData{Block: &types.Block{Number: uint64(n), Hash: fmt.Sprintf("0xgap%060x", n),
			ParentHash: fmt.Sprintf("0xgap%060x", n-1), Timestamp: ts, GasUsed: 21000, GasLimit: 30_000_000}, SkipAddressStats: true}
	}
	for n := 21; n <= 40; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, block(n)))
	}
	advanceUntilStable(t, d)
	for n := 1; n <= 20; n++ {
		require.NoError(t, d.InsertBlockDataBatch(ctx, block(n)))
	}
	advanceUntilStable(t, d)
	requireStoredDaysEqualRecompute(t, d, "a late day, an empty day, then the head's day")
}
