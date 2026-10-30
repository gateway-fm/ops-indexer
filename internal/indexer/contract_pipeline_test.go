package indexer

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// laterBeginsFirst holds block 1's write until block 2's has begun: an order the pipeline allows, since each
// block's write begins in its own goroutine.
type laterBeginsFirst struct {
	*db.DB
	secondBegun chan struct{}
}

func (d *laterBeginsFirst) BeginBlockData(ctx context.Context, b *db.BlockData) (*db.BlockWrite, error) {
	if b.Block.Number == 1 {
		select {
		case <-d.secondBegun:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return d.DB.BeginBlockData(ctx, b)
	}
	w, err := d.DB.BeginBlockData(ctx, b)
	close(d.secondBegun)
	return w, err
}

// Two pipelined blocks each create the same contract from inside a call (CREATE2, destroyed again in the same
// transaction, as EIP-6780 allows); the pipeline commits both whichever block begins its write first.
func TestPipelineCommitsBlocksThatCreateOneContractAddress(t *testing.T) {
	real, pool := refillTestDB(t)
	mockRPC := new(MockRPCClient)
	child := strings.ToLower(common.HexToAddress("0x0000000000000000000000000000000000c41d").Hex())
	factory := strings.ToLower(statsB.Hex())
	for n := uint64(1); n <= 2; n++ {
		number := hexutil.Big(*new(big.Int).SetUint64(n))
		tx := common.BytesToHash(new(big.Int).SetUint64(0x7000 + n).Bytes())
		raw := &rpc.RawBlock{Number: &number, Hash: common.BytesToHash(new(big.Int).SetUint64(1000 + n).Bytes()),
			ParentHash: common.BytesToHash(new(big.Int).SetUint64(1000 + n - 1).Bytes()),
			Timestamp:  hexutil.Uint64(1_700_000_000 + n), Transactions: []rpc.RawTransaction{statsTx(tx, 0, statsA, statsB, &number)}}
		mockRPC.On("RawBlockByNumber", mock.Anything, n).Return(raw, nil).Maybe()
		mockRPC.On("FetchReceiptsBatch", mock.Anything, mock.MatchedBy(func(h []common.Hash) bool { return len(h) == 1 && h[0] == tx }),
			mock.Anything, mock.Anything).Return(map[common.Hash]*rpclient.Receipt{tx: {Status: 1, TxHash: tx, BlockNumber: new(big.Int).SetUint64(n)}}, nil).Maybe()
		output, to := "0x6080", child
		frames := []*types.InternalTransaction{
			{TxHash: tx.Hex(), BlockNumber: n, TraceAddress: "", CallType: "call", From: strings.ToLower(statsA.Hex()), To: &factory, Value: "0"},
			{TxHash: tx.Hex(), BlockNumber: n, TraceAddress: "0", CallType: "create2", From: factory, To: &to, Value: "0", Output: &output},
		}
		mockRPC.On("FetchTracesBatch", mock.Anything, mock.Anything, n, mock.Anything, mock.Anything, mock.Anything).Return(frames, nil).Maybe()
	}
	mockRPC.On("RawBlockHash", mock.Anything, mock.Anything).Return("", nil).Maybe()

	idx := NewWithConfig(&laterBeginsFirst{DB: real, secondBegun: make(chan struct{})}, mockRPC, time.Second, 0,
		&Config{RPCWorkers: 4, RPCRateLimit: 500, DBBatchSize: 500, TraceWorkers: 1, TraceRateLimit: 50, EnableTracing: true})
	idx.tracingSupported = true

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	last := idx.indexPipelined(ctx, 0, 2, 2, &receiptSkip{})
	require.EqualValues(t, 2, last, "both blocks committed")

	var rows int
	var block int64
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*), min(block_number) FROM contracts WHERE address = $1`, child).Scan(&rows, &block))
	assert.Equal(t, 1, rows)
	assert.EqualValues(t, 1, block, "the earlier block's creation")
}
