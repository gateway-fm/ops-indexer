package nodelookup

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// fakeNode serves block n with one transaction, hash txHash(n); gate, when set, holds every block fetch.
type fakeNode struct {
	blockCalls, txCalls atomic.Int64
	gate                chan struct{}
	delay               time.Duration
}

func txHash(n uint64) common.Hash {
	return common.BytesToHash(new(big.Int).SetUint64(0x7000_0000 + n).Bytes())
}
func blockHash(n uint64) common.Hash {
	return common.BytesToHash(new(big.Int).SetUint64(0xb000_0000 + n).Bytes())
}

func (f *fakeNode) BlockByNumber(ctx context.Context, n uint64) (*rpc.RawBlock, error) {
	f.blockCalls.Add(1)
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	time.Sleep(f.delay)
	number := hexutil.Big(*new(big.Int).SetUint64(n))
	to := common.HexToAddress("0xb0b")
	return &rpc.RawBlock{Number: &number, Hash: blockHash(n), Timestamp: hexutil.Uint64(1_700_000_000 + n),
		Transactions: []rpc.RawTransaction{{Hash: txHash(n), From: common.HexToAddress("0xa11ce"), To: &to}}}, nil
}

func (f *fakeNode) BlockReceipts(_ context.Context, n uint64) ([]*rpclient.Receipt, error) {
	return []*rpclient.Receipt{{Status: 1, GasUsed: 21_000, TxHash: txHash(n)}}, nil
}

func (f *fakeNode) BlockNumberByHash(_ context.Context, hash string) (uint64, bool, error) {
	n := new(big.Int).SetBytes(common.HexToHash(hash).Bytes()).Uint64() - 0xb000_0000
	return n, true, nil
}

func (f *fakeNode) TxBlockNumber(_ context.Context, hash string) (uint64, bool, error) {
	f.txCalls.Add(1)
	return new(big.Int).SetBytes(common.HexToHash(hash).Bytes()).Uint64() - 0x7000_0000, true, nil
}

func TestCacheIsBoundedByEntriesAndBytes(t *testing.T) {
	ctx := context.Background()
	node := &fakeNode{}
	l := New(node, Config{MaxBlocks: 2})
	for _, n := range []uint64{1, 2, 3, 2} {
		_, err := l.BlockByNumber(ctx, n)
		require.NoError(t, err)
	}
	assert.EqualValues(t, 3, node.blockCalls.Load(), "2 was cached")
	_, err := l.BlockByNumber(ctx, 1)
	require.NoError(t, err)
	assert.EqualValues(t, 4, node.blockCalls.Load(), "1 was evicted, the oldest of three")

	node = &fakeNode{}
	l = New(node, Config{MaxBlocks: 100, MaxBytes: 1})
	for _, n := range []uint64{1, 2, 2} {
		_, err := l.BlockByNumber(ctx, n)
		require.NoError(t, err)
	}
	assert.EqualValues(t, 2, node.blockCalls.Load(), "over the byte bound only the newest block stays")
	assert.Equal(t, 1, l.order.Len())
}

func TestConcurrentRequestsForOneBlockShareAFetch(t *testing.T) {
	node := &fakeNode{delay: 100 * time.Millisecond}
	l := New(node, Config{})
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := l.BlockByNumber(context.Background(), 7)
			assert.NoError(t, err)
			assert.EqualValues(t, 7, b.Block.Number)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, node.blockCalls.Load())
}

// A crawler cannot hold more than Concurrency node calls; a request that finds them all taken for its timeout
// is refused, not queued without bound.
func TestNodeCallsAreCappedAndBounded(t *testing.T) {
	node := &fakeNode{gate: make(chan struct{})}
	l := New(node, Config{Concurrency: 1, Timeout: 2 * time.Second})
	held := make(chan error, 1)
	go func() {
		_, err := l.BlockByNumber(context.Background(), 1)
		held <- err
	}()
	require.Eventually(t, func() bool { return node.blockCalls.Load() == 1 }, time.Second, 10*time.Millisecond)

	l.cfg.Timeout = 150 * time.Millisecond
	start := time.Now()
	_, err := l.BlockByNumber(context.Background(), 2)
	assert.ErrorIs(t, err, ErrBusy)
	assert.Less(t, time.Since(start), time.Second)
	assert.EqualValues(t, 1, node.blockCalls.Load(), "the second block never reached the node")

	close(node.gate)
	require.NoError(t, <-held)
}

func TestTransactionIsServedOnlyBelowTheWatermark(t *testing.T) {
	ctx := context.Background()
	node := &fakeNode{}
	l := New(node, Config{})

	tx, err := l.Transaction(ctx, txHash(50).Hex(), 49)
	require.NoError(t, err)
	assert.Nil(t, tx, "block 50 is above the watermark: not pruned, not served")
	assert.EqualValues(t, 0, node.blockCalls.Load())

	tx, err = l.Transaction(ctx, txHash(40).Hex(), 49)
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.EqualValues(t, 40, tx.BlockNumber)
	assert.Equal(t, strings.ToLower(common.HexToAddress("0xa11ce").Hex()), tx.From, "lowercased, as the database stores it")

	calls := node.txCalls.Load()
	tx, err = l.Transaction(ctx, txHash(40).Hex(), 49)
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, calls, node.txCalls.Load(), "a cached block's transactions resolve without the node")

	b, err := l.BlockByHash(ctx, blockHash(40).Hex(), 49)
	require.NoError(t, err)
	require.NotNil(t, b)
	assert.EqualValues(t, 40, b.Block.Number)
}
