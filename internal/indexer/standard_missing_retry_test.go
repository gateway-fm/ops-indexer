package indexer

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// receiptsGapNode is a chain of blocks with one transaction each, whose receipts for one block are unavailable
// while withheld is set.
type receiptsGapNode struct {
	*reorgNode
	block    uint64
	withheld atomic.Bool
}

func newReceiptsGapNode(head, block uint64) *receiptsGapNode {
	n := &receiptsGapNode{reorgNode: newReorgNode(1<<62, head), block: block}
	to := common.HexToAddress("0xb0b")
	for number, b := range n.chain {
		b.Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash(new(big.Int).SetUint64(0x7000_0000 + number).Bytes()),
			From: common.HexToAddress("0xa11ce"), To: &to}}
	}
	n.withheld.Store(true)
	return n
}

func (n *receiptsGapNode) setHead(head uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.head = head
}

func (n *receiptsGapNode) FetchReceiptsBatch(_ context.Context, hashes []common.Hash, _, _ int, block ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	if len(block) > 0 && block[0] == n.block && n.withheld.Load() {
		return nil, errors.New("receipts for the block unavailable: eth_getBlockReceipts returned null")
	}
	out := make(map[common.Hash]*rpclient.Receipt, len(hashes))
	for _, h := range hashes {
		out[h] = &rpclient.Receipt{Status: 1, GasUsed: 21_000, TxHash: h}
	}
	return out, nil
}

func countWhere(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	require.NoError(t, pool.QueryRow(context.Background(), query, args...).Scan(&n))
	return n
}

// In standard mode a block skipped for its receipts is recorded as missing. The loop retries it itself, while the
// head goes on: once the node answers its receipts, it is stored, the record goes, and the ledger closes its span,
// with no restart.
func TestStandardModeIndexesABlockSkippedForItsReceipts(t *testing.T) {
	for _, mode := range []struct {
		name  string
		depth int
	}{{"serial", 1}, {"pipelined", 3}} {
		t.Run(mode.name, func(t *testing.T) {
			d, pool := refillTestDB(t)
			ctx := context.Background()
			_, err := pool.Exec(ctx, `CREATE OR REPLACE FUNCTION prune_floor_gap() RETURNS BIGINT LANGUAGE sql IMMUTABLE AS $f$ SELECT 3::BIGINT $f$`)
			require.NoError(t, err, "a 3-block reorg floor, so spans of this short chain can close")
			node := newReceiptsGapNode(103, 83)
			node.setHead(90)
			idx := NewWithConfig(d, node, 5*time.Millisecond, 0,
				&Config{RPCWorkers: 1, RPCRateLimit: 100, CommitPipelineDepth: mode.depth})
			idx.missingRetryInterval = 50 * time.Millisecond

			runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = idx.startStandardMode(runCtx, 0)
			}()
			defer func() { cancel(); wg.Wait() }()
			waitFor := func(what string, ok func() bool) {
				t.Helper()
				for !ok() {
					if runCtx.Err() != nil {
						t.Fatalf("timed out waiting until %s", what)
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			waitFor("83 is recorded and the head reaches 90", func() bool {
				return countWhere(t, pool, `SELECT count(*) FROM missing_block_ranges WHERE from_number = 83`) == 1 &&
					countWhere(t, pool, `SELECT COALESCE(max(number), 0) FROM blocks`) == 90
			})
			node.setHead(103)
			waitFor("the head reaches 103 while 83's receipts are still unavailable", func() bool {
				return countWhere(t, pool, `SELECT COALESCE(max(number), 0) FROM blocks`) == 103
			})
			require.Zero(t, countWhere(t, pool, `SELECT count(*) FROM blocks WHERE number = 83`))
			closed, err := d.CloseLedgerSpans(ctx, db.LedgerConfig{SpanBlocks: 50}, 10)
			require.NoError(t, err)
			require.Equal(t, 1, closed, "the span holding 83 does not close while 83 is missing")

			node.withheld.Store(false)
			waitFor("83 is stored and no longer recorded", func() bool {
				return countWhere(t, pool, `SELECT count(*) FROM blocks WHERE number = 83`) == 1 &&
					countWhere(t, pool, `SELECT count(*) FROM missing_block_ranges`) == 0
			})
			require.Zero(t, countWhere(t, pool, `
				SELECT count(*) FROM generate_series(1, 103) g WHERE NOT EXISTS (SELECT 1 FROM blocks WHERE number = g)`),
				"1..103 is stored with no gap")
			require.Equal(t, node.chain[83].Hash.Hex(), func() string {
				b, err := d.GetBlock(ctx, 83)
				require.NoError(t, err)
				return b.Hash
			}())
			closed, err = d.CloseLedgerSpans(ctx, db.LedgerConfig{SpanBlocks: 50}, 10)
			require.NoError(t, err)
			require.Equal(t, 1, closed, "the span holding 83 closes")
			require.EqualValues(t, 100, countWhere(t, pool, `SELECT max(hi) FROM ledger_spans`))
		})
	}
}
