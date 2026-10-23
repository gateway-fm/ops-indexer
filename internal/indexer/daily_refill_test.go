package indexer

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

func refillTestDB(t *testing.T) (*db.DB, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	c, err := postgres.RunContainer(ctx, testcontainers.WithImage("postgres:15-alpine"), postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"), postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(30*time.Second)))
	if err != nil {
		t.Skipf("skipping: postgres testcontainer unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	conn, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	d, err := db.New(conn)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	require.NoError(t, d.Migrate())
	pool, err := pgxpool.New(ctx, conn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return d, pool
}

// refillChain is four UTC days of blocks with uneven gaps, as ingest would write them and as the node serves them.
type refillChain struct {
	day0   time.Time
	blocks []*db.BlockData
	node   map[uint64]*RefillBlock
	recpts map[uint64][]*rpclient.Receipt
}

func newRefillChain() *refillChain {
	c := &refillChain{day0: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), node: map[uint64]*RefillBlock{}, recpts: map[uint64][]*rpclient.Receipt{}}
	addr := func(n uint64) string { return fmt.Sprintf("0x%040x", 0xa0000+n) }
	token := common.HexToAddress("0x00000000000000000000000000000000000070c1")
	topic := func(a string) common.Hash { return common.BytesToHash(common.HexToAddress(a).Bytes()) }
	ts := uint64(c.day0.Unix()) - 20_000
	c.node[0] = &RefillBlock{Number: 0, Timestamp: ts - 1_000}
	for n := uint64(1); ts < uint64(c.day0.Add(96*time.Hour).Unix())+3_000; n++ {
		ts += 1_000 + (n*7_919)%4_000
		bd := &db.BlockData{Block: &types.Block{Number: n, Hash: fmt.Sprintf("0xb%d", n), ParentHash: fmt.Sprintf("0xb%d", n-1),
			Timestamp: ts, GasUsed: 21_000 * (n % 5), GasLimit: 30_000_000, Size: 500 + (n*37)%900},
			AddressStats: map[string]*db.AddressStatsDelta{}}
		touch := func(a string) {
			if bd.AddressStats[a] == nil {
				bd.AddressStats[a] = &db.AddressStatsDelta{Address: a, BlockNumber: n, IsContract: a == addr(10)}
			}
			bd.AddressStats[a].TxCountDelta++
		}
		rb := &RefillBlock{Number: n, Timestamp: ts, GasUsed: bd.Block.GasUsed, Size: bd.Block.Size}
		var receipts []*rpclient.Receipt
		for i := uint64(0); i < n%4; i++ {
			hash := fmt.Sprintf("0xt%d_%d", n, i)
			from := addr((n + i) % 7)
			var to *string
			if (n+i)%9 != 0 {
				a := addr(10 + (n*3+i)%11)
				to = &a
			}
			status := 1
			if (n+i)%10 == 0 {
				status = 0
			}
			price := 1_000_000_000 + (n*13+i)%1_000
			bd.Block.TransactionCount++
			bd.Transactions = append(bd.Transactions, &types.Transaction{Hash: hash, BlockNumber: n, TxIndex: int(i), From: from, To: to,
				Value: "0", GasPrice: price, Status: status})
			rb.TxCount++
			rb.Txs = append(rb.Txs, RefillTx{From: from, To: to, GasPrice: new(big.Int).SetUint64(price)})
			touch(from)
			if to != nil {
				touch(*to)
			}
			rc := &rpclient.Receipt{Status: uint64(status), TxHash: common.HexToHash(hash)}
			if (n+i)%3 == 0 {
				bd.Transfers = append(bd.Transfers, &types.TokenTransfer{TxHash: hash, LogIndex: int(i), TokenAddress: token.Hex(),
					From: from, To: addr(30 + i), Value: "1", BlockNumber: n, TokenType: types.TokenTypeERC20})
				touch(addr(30 + i))
				rc.Logs = append(rc.Logs, &rpclient.Log{Address: token, Topics: []common.Hash{transferTopic, topic(from), topic(addr(30 + i))}})
			}
			if (n+i)%5 == 0 {
				rc.Logs = append(rc.Logs, &rpclient.Log{Address: token, Topics: []common.Hash{transferTopic, topic(from)}})
			}
			receipts = append(receipts, rc)
		}
		c.blocks = append(c.blocks, bd)
		c.node[n] = rb
		c.recpts[n] = receipts
	}
	return c
}

func (c *refillChain) day(n uint64) time.Time {
	return time.Unix(int64(c.node[n].Timestamp), 0).UTC().Truncate(24 * time.Hour)
}

func (c *refillChain) BlockNumber(context.Context) (uint64, error) { return uint64(len(c.blocks)), nil }

func (c *refillChain) Blocks(_ context.Context, numbers []uint64, bodies bool) ([]*RefillBlock, error) {
	out := make([]*RefillBlock, len(numbers))
	for i, n := range numbers {
		b := *c.node[n]
		if !bodies {
			b.Txs = nil
		}
		out[i] = &b
	}
	return out, nil
}

func (c *refillChain) BlockReceipts(_ context.Context, numbers []uint64) ([][]*rpclient.Receipt, error) {
	out := make([][]*rpclient.Receipt, len(numbers))
	for i, n := range numbers {
		out[i] = c.recpts[n]
	}
	return out, nil
}

func (c *refillChain) store(t *testing.T, d *db.DB, keep func(n uint64) bool) {
	t.Helper()
	for _, bd := range c.blocks {
		if keep(bd.Block.Number) {
			require.NoError(t, d.InsertBlockDataBatch(context.Background(), bd))
		}
	}
}

func advanceAll(t *testing.T, d *db.DB) {
	t.Helper()
	for i := 0; i < 50; i++ {
		before, err := d.AdvanceDailyStats(context.Background())
		require.NoError(t, err)
		after, err := d.AdvanceDailyStats(context.Background())
		require.NoError(t, err)
		if before == after {
			return
		}
	}
}

func dailyRows(t *testing.T, d *db.DB, from, to time.Time) []types.DailyStats {
	t.Helper()
	rows, err := d.GetDailyStats(context.Background(), from, to)
	require.NoError(t, err)
	return rows
}

// PRST-5436: days whose blocks never reached daily_stats (catch-up stored them below the watermark) and are now
// pruned, recounted from the node, equal a full sync's daily_stats on all 16 columns: the days before, the first day
// the lossy database aggregated, and the day after. Block gaps are uneven, so a block time counted without the
// previous day's last block shows.
func TestRefilledDaysEqualAFullSync(t *testing.T) {
	ctx := context.Background()
	chain := newRefillChain()
	last := uint64(len(chain.blocks))
	day1, day3, day4 := chain.day0, chain.day0.Add(48*time.Hour), chain.day0.Add(72*time.Hour)
	var firstOfDay3 uint64
	for n := uint64(1); n <= last; n++ {
		if !chain.day(n).Before(day3) {
			firstOfDay3 = n
			break
		}
	}

	full, _ := refillTestDB(t)
	for n := uint64(1); n <= last; n++ {
		chain.store(t, full, func(m uint64) bool { return m == n })
		_, err := full.AdvanceDailyStats(ctx)
		require.NoError(t, err)
	}

	want := dailyRows(t, full, day1, day4)
	require.Len(t, want, 4)

	lossy, pool := refillTestDB(t)
	chain.store(t, lossy, func(n uint64) bool { return n >= firstOfDay3 })
	_, err := pool.Exec(ctx, `INSERT INTO daily_stats_watermark (id, last_block) VALUES (1, $1)`, int64(firstOfDay3-1))
	require.NoError(t, err)
	advanceAll(t, lossy)
	chain.store(t, lossy, func(n uint64) bool { return n < firstOfDay3 })
	advanceAll(t, lossy)
	for _, q := range []string{`DELETE FROM token_transfers WHERE block_number < $1`, `DELETE FROM transactions WHERE block_number < $1`,
		`DELETE FROM blocks WHERE number < $1`} {
		_, err := pool.Exec(ctx, q, int64(firstOfDay3))
		require.NoError(t, err)
	}
	lost := dailyRows(t, lossy, day1, day4)
	require.NotEqual(t, want, lost, "the lossy database is missing days 1 and 2")

	plan, err := RefillDailyStats(ctx, lossy, chain, []time.Time{day1, day1.Add(24 * time.Hour), day3},
		DailyRefillOptions{Bodies: true, Receipts: true, BatchesPerSecond: 1e6, Batch: 7, Plan: true})
	require.NoError(t, err)
	require.Len(t, plan.Days, 3)
	require.Equal(t, lost, dailyRows(t, lossy, day1, day4), "a plan writes nothing")

	headers, err := RefillDailyStats(ctx, lossy, chain, []time.Time{day1, day1.Add(24 * time.Hour), day3},
		DailyRefillOptions{BatchesPerSecond: 1e6, Batch: 7, Dry: true})
	require.NoError(t, err)
	for i, got := range headers.Rows[:4] {
		w := want[i]
		require.Equal(t, [5]float64{float64(w.TotalBlocks), float64(w.TotalTransactions), float64(w.TotalGasUsed), w.AvgBlockTime, float64(w.AvgBlockSize)},
			[5]float64{float64(got.TotalBlocks), float64(got.TotalTransactions), float64(got.TotalGasUsed), got.AvgBlockTime, float64(got.AvgBlockSize)},
			"headers make the block-level columns exact (%s)", w.Date)
		require.Equal(t, [5]int64{int64(w.NewAddresses), int64(w.NewContracts), w.CumulativeTransactions, w.CumulativeAddresses, w.CumulativeContracts},
			[5]int64{int64(got.NewAddresses), int64(got.NewContracts), got.CumulativeTransactions, got.CumulativeAddresses, got.CumulativeContracts},
			"and the history-wide columns (%s)", w.Date)
		if i < 2 {
			require.Zero(t, got.AvgGasPrice+int64(got.SuccessfulTxs+got.ActiveAddresses+got.TokenTransferCount),
				"what headers cannot count keeps the stored value: none for a day that had no row (%s)", w.Date)
		}
	}

	dry, err := RefillDailyStats(ctx, lossy, chain, []time.Time{day1, day1.Add(24 * time.Hour), day3},
		DailyRefillOptions{Bodies: true, Receipts: true, BatchesPerSecond: 1e6, Batch: 7, Dry: true})
	require.NoError(t, err)
	require.Equal(t, want, dry.Rows[:4], "the dry run prints the rows the refill writes")
	require.Equal(t, lost, dailyRows(t, lossy, day1, day4), "a dry run writes nothing")

	_, err = RefillDailyStats(ctx, lossy, chain, []time.Time{day1, day1.Add(24 * time.Hour), day3},
		DailyRefillOptions{Bodies: true, Receipts: true, BatchesPerSecond: 1e6, Batch: 7})
	require.NoError(t, err)
	require.Equal(t, want, dailyRows(t, lossy, day1, day4), "before, boundary and after days equal a full sync")

	_, err = RefillDailyStats(ctx, lossy, chain, []time.Time{day1, day1.Add(24 * time.Hour), day3},
		DailyRefillOptions{Bodies: true, Receipts: true, BatchesPerSecond: 1e6, Batch: 7})
	require.NoError(t, err)
	require.Equal(t, want, dailyRows(t, lossy, day1, day4), "a rerun resumes from the checkpointed days and changes nothing")
}
