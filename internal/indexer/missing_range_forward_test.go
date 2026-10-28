package indexer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
)

// timeoutOnceDB runs the first gap scan that covers block failAt with a 1 ms statement timeout, so the server
// cancels it, and records every range scanned.
type timeoutOnceDB struct {
	*db.DB
	failAt  uint64
	mu      sync.Mutex
	failed  bool
	scanned [][2]uint64
}

func (d *timeoutOnceDB) FindMissingBlocksInRange(ctx context.Context, from, to uint64) ([]db.BlockRange, error) {
	d.mu.Lock()
	d.scanned = append(d.scanned, [2]uint64{from, to})
	fail := !d.failed && from <= d.failAt && d.failAt <= to
	d.failed = d.failed || fail
	d.mu.Unlock()
	if fail {
		d.SetMissingScanTimeout(time.Millisecond)
		defer d.SetMissingScanTimeout(0)
	}
	return d.DB.FindMissingBlocksInRange(ctx, from, to)
}

// The forward scan up to realtime walks its range in batches and records its progress after each one. A batch that
// fails, here at its statement timeout, costs only that batch: the next tick resumes there and finds every gap.
func TestForwardScanAdvancesBatchByBatchAndResumesAfterATimeout(t *testing.T) {
	store, pool := refillTestDB(t)
	_, err := pool.Exec(context.Background(), `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT n, '0xhash' || n, '0xparent' || n, 1000, 21000, 30000000, 0
		FROM generate_series(1, 350000) AS n
		WHERE n <> ALL(ARRAY[50000, 210000, 210001, 210002, 340000]::bigint[])`)
	require.NoError(t, err)

	database := &timeoutOnceDB{DB: store, failAt: 210_000}
	node := new(MockRPCClient)
	node.On("BlockNumber", mock.Anything).Return(uint64(350_000), nil)
	c := NewMissingRangeCollector(database, node, &MissingRangeCollectorConfig{BatchSize: 100_000})
	c.minFetchedBlock = 1

	c.scanForward()
	_, maxFetched, _, _ := c.GetProgress()
	require.Equal(t, uint64(200_000), maxFetched, "the two batches before the failed one are kept")
	progress, err := store.GetIndexerProgress(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(200_000), progress.MaxFetchedBlock, "and recorded")

	c.scanForward()
	_, maxFetched, _, _ = c.GetProgress()
	require.Equal(t, uint64(350_000), maxFetched)
	require.Equal(t, [][2]uint64{{1, 100_000}, {100_001, 200_000}, {200_001, 300_000}, {200_001, 300_000}, {300_001, 350_000}},
		database.scanned, "batch by batch, the failed batch again on the next tick")

	total, err := store.GetTotalMissingBlocks(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(5), total, "blocks 50000, 210000-210002 and 340000")
}
