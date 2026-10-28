package indexer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// withholdingNode is a receiptsGapNode whose receipts are unavailable for every block in withheld.
type withholdingNode struct {
	*receiptsGapNode
	mu       sync.Mutex
	withheld map[uint64]bool
	asked    map[uint64]int
}

func newWithholdingNode(head uint64, blocks ...uint64) *withholdingNode {
	n := &withholdingNode{receiptsGapNode: newReceiptsGapNode(head, 0), withheld: map[uint64]bool{}, asked: map[uint64]int{}}
	n.receiptsGapNode.withheld.Store(false)
	for _, b := range blocks {
		n.withheld[b] = true
	}
	return n
}

func (n *withholdingNode) withhold(block uint64, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.withheld[block] = on
}

func (n *withholdingNode) FetchReceiptsBatch(ctx context.Context, hashes []common.Hash, w, r int, block ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	n.mu.Lock()
	held := len(block) > 0 && n.withheld[block[0]]
	if len(block) > 0 {
		n.asked[block[0]]++
	}
	n.mu.Unlock()
	if held {
		return nil, errors.New("receipts for the block unavailable: eth_getBlockReceipts returned null")
	}
	return n.receiptsGapNode.FetchReceiptsBatch(ctx, hashes, w, r, block...)
}

// failingRanges is a real database whose writes to missing_block_ranges after a retry fail while failRest is set.
type failingRanges struct {
	*db.DB
	failRest bool
}

func (f *failingRanges) SaveMissingRanges(ctx context.Context, ranges []db.BlockRange) error {
	if f.failRest {
		f.failRest = false
		return errors.New("connection reset")
	}
	return f.DB.SaveMissingRanges(ctx, ranges)
}

func (f *failingRanges) AdvanceMissingRange(ctx context.Context, r db.BlockRange, next uint64) error {
	if f.failRest {
		f.failRest = false
		return errors.New("connection reset")
	}
	return f.DB.AdvanceMissingRange(ctx, r, next)
}

// indexChainSkipping runs the standard loop over 1..head until it has stored head and recorded every block in
// skipped, then stops it.
func indexChainSkipping(t *testing.T, idx *Indexer, pool *pgxpool.Pool, head uint64, skipped int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = idx.startStandardMode(ctx, 0)
	}()
	for countWhere(t, pool, `SELECT COALESCE(max(number), 0) FROM blocks`) != int64(head) ||
		countWhere(t, pool, `SELECT count(*) FROM missing_block_ranges`) != int64(skipped) {
		require.NoError(t, ctx.Err(), "the loop stores the head and records the skipped blocks")
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()
}

func missingBlocks(t *testing.T, pool *pgxpool.Pool) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT g FROM generate_series(1, (SELECT max(number) FROM blocks)) g
		WHERE NOT EXISTS (SELECT 1 FROM blocks WHERE number = g)
		  AND NOT EXISTS (SELECT 1 FROM missing_block_ranges WHERE g BETWEEN from_number AND to_number)
		ORDER BY g`)
	require.NoError(t, err)
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var g int64
		require.NoError(t, rows.Scan(&g))
		out = append(out, g)
	}
	return out
}

// A retry fills the start of a recorded range and stops at a block the node still cannot serve. Recording the rest
// fails: the rest stays recorded, and is filled once the node serves it.
func TestRetryKeepsTheRestOfARangeRecordedWhenRecordingItFails(t *testing.T) {
	d, pool := refillTestDB(t)
	ctx := context.Background()
	node := newWithholdingNode(100, 83, 84, 85, 86)
	store := &failingRanges{DB: d}
	idx := NewWithConfig(store, node, 5*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
	indexChainSkipping(t, idx, pool, 100, 4)
	_, err := pool.Exec(ctx, `DELETE FROM missing_block_ranges; INSERT INTO missing_block_ranges (from_number, to_number) VALUES (83, 86)`)
	require.NoError(t, err)

	node.withhold(83, false)
	node.withhold(84, false)
	node.withhold(86, false)
	store.failRest = true
	idx.missingRetryAt = time.Time{}
	idx.retryMissing(ctx, 100, &receiptSkip{recorded: true})
	require.EqualValues(t, 2, countWhere(t, pool, `SELECT count(*) FROM blocks WHERE number IN (83, 84)`))
	require.Empty(t, missingBlocks(t, pool), "every block not stored is still recorded")

	node.withhold(85, false)
	idx.missingRetryAt = time.Time{}
	idx.retryMissing(ctx, 100, &receiptSkip{recorded: true})
	require.EqualValues(t, 4, countWhere(t, pool, `SELECT count(*) FROM blocks WHERE number BETWEEN 83 AND 86`))
	require.Zero(t, countWhere(t, pool, `SELECT count(*) FROM missing_block_ranges`))
}

func (n *withholdingNode) timesAsked(block uint64) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.asked[block]
}

// The lowest recorded block never becomes available: the retry still fills a higher one, and asks for the lowest
// again only after a growing pause.
func TestRetryFillsHigherRangesPastOneThatKeepsFailing(t *testing.T) {
	d, pool := refillTestDB(t)
	ctx := context.Background()
	node := newWithholdingNode(100, 83, 90)
	idx := NewWithConfig(d, node, 5*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
	indexChainSkipping(t, idx, pool, 100, 2)

	node.withhold(90, false)
	idx.missingRetryAt = time.Time{}
	idx.retryMissing(ctx, 100, &receiptSkip{recorded: true})
	require.EqualValues(t, 1, countWhere(t, pool, `SELECT count(*) FROM blocks WHERE number = 90`), "90 is filled past the failing 83")
	require.EqualValues(t, 1, countWhere(t, pool, `SELECT count(*) FROM missing_block_ranges WHERE from_number = 83`))
	require.Zero(t, countWhere(t, pool, `SELECT count(*) FROM missing_block_ranges WHERE from_number = 90`))

	before := node.timesAsked(83)
	for k := 0; k < 3; k++ {
		idx.missingRetryAt = time.Time{}
		idx.retryMissing(ctx, 100, &receiptSkip{recorded: true})
	}
	require.Equal(t, before, node.timesAsked(83), "a block that keeps failing waits out its pause instead of being asked every pass")
}
