package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

func withShortStartWaits(t *testing.T, aboveHead time.Duration) {
	t.Helper()
	interval, above := startWaitInterval, staleAboveHeadWait
	startWaitInterval, staleAboveHeadWait = 10*time.Millisecond, aboveHead
	t.Cleanup(func() { startWaitInterval, staleAboveHeadWait = interval, above })
}

func startWaitingBlocks(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "indexer_start_waiting_blocks" {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("indexer_start_waiting_blocks is not registered")
	return 0
}

// runStart runs Start until stop is called; stop returns Start's error.
func runStart(idx *Indexer) (stop func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = idx.Start(ctx)
	}()
	return func() error {
		cancel()
		wg.Wait()
		return err
	}
}

func waitUntil(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// In collector mode, stored blocks 98..100 are on a fork the chain has left (a reorg while the indexer was down),
// and the node, behind on them, answers null. The start waits for the node, writes nothing meanwhile, and survives a
// restart; once the node serves the chain, 98..100 are reverted and replaced, and nothing above is built on them.
// Before, the start skipped them and catch-up wrote 101.. on top, keeping the stale blocks for good.
func TestCollectorStartWaitsForStoredBlocksTheNodeDoesNotServe(t *testing.T) {
	withShortStartWaits(t, time.Hour)
	ctx := context.Background()
	d, pool := refillTestDB(t)
	for n := uint64(1); n <= 100; n++ {
		require.NoError(t, d.InsertBlock(ctx, &types.Block{Number: n, Hash: oldHash(n).Hex(), ParentHash: oldHash(n - 1).Hex(),
			Difficulty: "0", TotalDifficulty: "0"}))
	}
	node := &laggingReorgNode{reorgNode: newReorgNode(98, 300), missing: map[uint64]bool{98: true, 99: true, 100: true}}
	cfg := &Config{RPCWorkers: 1, RPCRateLimit: 100, CatchupEnabled: true, CatchupWorkers: 2, CatchupBatchSize: 50, CatchupQueueSize: 100}
	stored := func() (count, highest int64) {
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*), COALESCE(max(number), 0) FROM blocks`).Scan(&count, &highest))
		return count, highest
	}

	stop := runStart(NewWithConfig(d, node, 10*time.Millisecond, 0, cfg))
	time.Sleep(time.Second)
	count, highest := stored()
	require.EqualValues(t, 100, highest, "nothing is built on the blocks the start could not compare")
	require.EqualValues(t, 100, count, "nothing is written or reverted while the start waits")
	require.EqualValues(t, 3, startWaitingBlocks(t), "the start waits for the three blocks")
	require.ErrorIs(t, stop(), context.Canceled, "a restart while waiting")
	require.Zero(t, startWaitingBlocks(t))

	node.catchUp()
	stop = runStart(NewWithConfig(d, node, 10*time.Millisecond, 0, cfg))
	defer stop()
	waitUntil(t, "1..300 is stored", 60*time.Second, func() bool { c, h := stored(); return c == 300 && h == 300 })
	require.ErrorIs(t, stop(), context.Canceled)

	var wrong []string
	for n := uint64(1); n <= 300; n++ {
		b, err := d.GetBlock(ctx, n)
		require.NoError(t, err)
		if b.Hash != node.chain[n].Hash.Hex() || b.ParentHash != node.chain[n].ParentHash.Hex() {
			wrong = append(wrong, fmt.Sprint(n))
		}
	}
	require.Empty(t, wrong, "blocks not the chain's")
}

// shrinkingNode serves a chain of one transaction per block, each to an address only that block sees, with the
// head settable: lowering it is a reorg onto a shorter chain.
type shrinkingNode struct {
	*reorgNode
}

func newShrinkingNode(head uint64) *shrinkingNode {
	n := &shrinkingNode{reorgNode: newReorgNode(1<<62, head)}
	from := common.HexToAddress("0xa11ce")
	for number, b := range n.chain {
		to := common.BytesToAddress(new(big.Int).SetUint64(0x1000 + number).Bytes())
		b.Timestamp = hexutil.Uint64(1_760_000_000 + 1_000*number)
		b.Transactions = []rpc.RawTransaction{{Hash: common.BytesToHash(new(big.Int).SetUint64(0x7000_0000 + number).Bytes()),
			From: from, To: &to}}
	}
	return n
}

func (n *shrinkingNode) setHead(head uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.head = head
}

func (n *shrinkingNode) FetchReceiptsBatch(_ context.Context, hashes []common.Hash, _, _ int, _ ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	out := make(map[common.Hash]*rpclient.Receipt, len(hashes))
	for _, h := range hashes {
		out[h] = &rpclient.Receipt{Status: 1, GasUsed: 21_000, TxHash: h}
	}
	return out, nil
}

func indexStandardTo(t *testing.T, d *db.DB, pool *pgxpool.Pool, node RPCClient, head int64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = NewWithConfig(d, node, 5*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100}).startStandardMode(ctx, 0)
	}()
	waitUntil(t, fmt.Sprintf("the head reaches %d", head), 30*time.Second, func() bool {
		return countWhere(t, pool, `SELECT COALESCE(max(number), 0) FROM blocks`) == head
	})
	cancel()
	wg.Wait()
}

// derivedState is everything a revert compensates, as text: the chain counters, address_stats and the days.
func derivedState(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `SELECT daily_stats_advance()`)
	require.NoError(t, err)
	var s string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT concat_ws(E'\n',
			(SELECT string_agg(name || '=' || count, ' ' ORDER BY name) FROM chain_counters
			 WHERE name <> 'address_stats_reverts'),
			(SELECT string_agg(concat_ws(' ', address, tx_count, internal_tx_count, token_transfer_count,
			                             first_seen, last_seen, is_contract), E'\n' ORDER BY address) FROM address_stats),
			(SELECT string_agg(concat_ws(' ', date, blocks, gas_used, txs, successful_txs, failed_txs, active_addresses,
			                             token_transfers), E'\n' ORDER BY date) FROM daily_stats_acc),
			(SELECT string_agg(date || ' ' || address, E'\n' ORDER BY date, address) FROM daily_active_addresses))`).Scan(&s))
	return s
}

// The chain reorgs onto a shorter one while the indexer is down: blocks 96..100 are stored, and the node's head stays
// at 95. After staleAboveHeadWait the start reverts 96..100 as stale, through the same revert as any reorg: the
// counters, address_stats (96..100's recipients lose their rows) and the days end as if 96..100 had never been
// indexed.
func TestStartRevertsStoredBlocksAboveANodeHeadThatStaysBelowThem(t *testing.T) {
	withShortStartWaits(t, 300*time.Millisecond)
	want, wantPool := refillTestDB(t)
	indexStandardTo(t, want, wantPool, newShrinkingNode(95), 95)

	d, pool := refillTestDB(t)
	node := newShrinkingNode(100)
	indexStandardTo(t, d, pool, node, 100)
	node.setHead(95)

	stop := runStart(NewWithConfig(d, node, 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100}))
	defer stop()
	waitUntil(t, "96..100 are reverted", 10*time.Second, func() bool {
		return countWhere(t, pool, `SELECT COALESCE(max(number), 0) FROM blocks`) == 95
	})
	if err := stop(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("start: %v", err)
	}
	require.Zero(t, countWhere(t, pool, `SELECT count(*) FROM address_stats WHERE address = $1`,
		strings.ToLower(common.BytesToAddress(big.NewInt(0x1000+98).Bytes()).Hex())), "seen only in a reverted block")
	require.Equal(t, derivedState(t, wantPool), derivedState(t, pool))
}

// nullHeadNode is a JSON-RPC node that serves blocks 1..90 with the stored hashes and answers eth_blockNumber with 90
// once, then with null.
func nullHeadNode(t *testing.T) *rpc.Client {
	t.Helper()
	var heads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		result := "null"
		switch req.Method {
		case "eth_blockNumber":
			if heads.Add(1) == 1 {
				result = `"0x5a"`
			}
		case "eth_getBlockByNumber":
			var hex string
			require.NoError(t, json.Unmarshal(req.Params[0], &hex))
			n, err := strconv.ParseUint(strings.TrimPrefix(hex, "0x"), 16, 64)
			require.NoError(t, err)
			if n >= 1 && n <= 90 {
				result = fmt.Sprintf(`{"number":%q,"hash":%q}`, hex, oldHash(n).Hex())
			}
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, result)
	}))
	t.Cleanup(srv.Close)
	client, err := rpc.New(srv.URL)
	require.NoError(t, err)
	return client
}

// A node whose head read answers null while the start waits (blocks 91..100 stored above its last head, 90) does not
// make the stored chain look far ahead of a head of 0: the read is retried, and nothing is reverted however long it
// lasts. Before, null decoded as head 0, and after staleAboveHeadWait the start deleted every stored block.
func TestStartWaitRevertsNothingOnANullHead(t *testing.T) {
	withShortStartWaits(t, 100*time.Millisecond)
	store := newReorgStore(100, func() int { return 0 })
	idx := NewWithConfig(store, nullHeadNode(t), 10*time.Millisecond, 0, &Config{RPCWorkers: 1, RPCRateLimit: 100})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _, err := idx.checkStoredHead(ctx, 100, 90)
	require.ErrorIs(t, err, context.DeadlineExceeded, "still waiting for a head")
	require.Empty(t, store.deletedBlocks())
}

// A chain reset found while the start waits (the node's head drops far below the stored head) refuses the start, or
// with FORCE_REINDEX wipes and starts, as the reset found at startup does.
func TestStartWaitHonoursForceReindexOnAChainReset(t *testing.T) {
	withShortStartWaits(t, time.Hour)
	start := func(force bool) (*MockDatabase, error) {
		mockDB, mockRPC := new(MockDatabase), new(MockRPCClient)
		for n := uint64(1); n <= 300; n++ {
			hash := fmt.Sprintf("0x%064x", 1000+n)
			mockDB.On("GetBlock", mock.Anything, n).Return(&types.Block{Number: n, Hash: hash}, nil).Maybe()
			mockRPC.On("RawBlockHash", mock.Anything, n).Return(hash, nil).Maybe()
		}
		mockDB.On("GetLatestBlockNumber", mock.Anything).Return(uint64(300), nil)
		mockDB.On("GetBlockCount", mock.Anything).Return(int64(300), nil)
		mockDB.On("GetBlockCountInRange", mock.Anything, mock.Anything, mock.Anything).Return(int64(300), nil).Maybe()
		mockDB.On("GetAllTokenAddresses", mock.Anything).Return([]string{}, nil).Maybe()
		mockDB.On("WipeAllData", mock.Anything).Return(nil).Maybe()
		mockDB.On("UpdateSyncStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		mockRPC.On("BlockNumber", mock.Anything).Return(uint64(290), nil).Once()
		mockRPC.On("BlockNumber", mock.Anything).Return(uint64(100), nil)
		mockRPC.On("CheckTracingSupport", mock.Anything).Return(false, nil).Maybe()
		idx := NewWithConfig(mockDB, mockRPC, time.Second, 0, &Config{RPCWorkers: 4, RPCRateLimit: 100, DBBatchSize: 100})
		if force {
			t.Setenv("FORCE_REINDEX", "true")
		} else {
			t.Setenv("FORCE_REINDEX", "")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		return mockDB, idx.Start(ctx)
	}

	mockDB, err := start(false)
	require.ErrorIs(t, err, errChainResetWhileWaiting)
	mockDB.AssertNotCalled(t, "WipeAllData", mock.Anything)

	mockDB, err = start(true)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the start goes on after the wipe")
	mockDB.AssertCalled(t, "WipeAllData", mock.Anything)
}
