package indexer

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	statsA     = common.HexToAddress("0x00000000000000000000000000000000000000a1")
	statsB     = common.HexToAddress("0x00000000000000000000000000000000000000b2")
	statsC     = common.HexToAddress("0x00000000000000000000000000000000000000c3")
	statsToken = common.HexToAddress("0x00000000000000000000000000000000000070c1")
)

func statsTx(hash common.Hash, index uint64, from, to common.Address, block *hexutil.Big) rpc.RawTransaction {
	zero := hexutil.Big(*big.NewInt(0))
	nonce := hexutil.Uint64(index)
	return rpc.RawTransaction{
		Hash: hash, BlockNumber: block, TransactionIndex: hexutil.Uint64(index),
		From: from, To: &to, Value: &zero, GasPrice: &zero, Gas: 21000, Nonce: &nonce,
	}
}

func transferLog(from, to common.Address, index uint) *rpclient.Log {
	return &rpclient.Log{
		Address: statsToken,
		Topics:  []common.Hash{transferTopic, common.BytesToHash(from.Bytes()), common.BytesToHash(to.Bytes())},
		Data:    hexutil.Bytes(common.LeftPadBytes(big.NewInt(1).Bytes(), 32)),
		Index:   index,
	}
}

func addressStatsOf(t *testing.T, data *db.BlockData, addr common.Address) db.AddressStatsDelta {
	t.Helper()
	d, ok := data.AddressStats[strings.ToLower(addr.Hex())]
	require.True(t, ok, "no address_stats delta for %s", addr.Hex())
	return *d
}

// Each counter matches what the by-address lists return: a transaction once per address it involves (as sender,
// recipient or transfer participant), a transfer or internal call once per address on either side.
func TestAddressStatsCountEachTransactionTransferAndCallOncePerAddress(t *testing.T) {
	ctx := context.Background()
	mockRPC := new(MockRPCClient)
	idx := New(new(MockDatabase), mockRPC, time.Second, 0)
	idx.tokenCache.Add(statsToken.Hex())
	idx.tracingSupported = true

	number := hexutil.Big(*big.NewInt(77))
	selfSend, other := common.HexToHash("0x5e1f"), common.HexToHash("0x07e4")
	raw := &rpc.RawBlock{
		Number: &number, Hash: common.HexToHash("0xb10c"), Timestamp: hexutil.Uint64(time.Now().Unix()),
		Transactions: []rpc.RawTransaction{
			statsTx(selfSend, 0, statsA, statsA, &number),
			statsTx(other, 1, statsA, statsB, &number),
		},
	}
	receipts := map[common.Hash]*rpclient.Receipt{
		selfSend: {Status: 1, TxHash: selfSend, Logs: []*rpclient.Log{transferLog(statsA, statsA, 0), transferLog(statsB, statsC, 1)}},
		other:    {Status: 1, TxHash: other, Logs: []*rpclient.Log{transferLog(statsC, statsA, 2), transferLog(statsC, statsA, 3)}},
	}
	lowerA, lowerB := strings.ToLower(statsA.Hex()), strings.ToLower(statsB.Hex())
	mockRPC.On("FetchTracesBatch", ctx, mock.Anything, uint64(77), mock.Anything, mock.Anything, mock.Anything).
		Return([]*types.InternalTransaction{
			{TxHash: selfSend.Hex(), From: statsA.Hex(), To: &lowerA},
			{TxHash: other.Hex(), From: statsA.Hex(), To: &lowerB},
		}, nil).Once()

	data := idx.buildBlock(ctx, raw, receipts).data

	a := addressStatsOf(t, data, statsA)
	assert.Equal(t, 2, a.TxCountDelta, "A: the self-send and the A->B transaction")
	assert.Equal(t, 3, a.TokenTransferDelta, "A: the self-transfer once, and two transfers in from C")
	assert.Equal(t, 2, a.InternalTxCountDelta, "A: the self-call once, and the call to B")

	b := addressStatsOf(t, data, statsB)
	assert.Equal(t, 2, b.TxCountDelta, "B: a transfer participant in the self-send and the recipient of the other")
	assert.Equal(t, 1, b.TokenTransferDelta)
	assert.Equal(t, 1, b.InternalTxCountDelta)

	c := addressStatsOf(t, data, statsC)
	assert.Equal(t, 2, c.TxCountDelta, "C: one per transaction, not per transfer")
	assert.Equal(t, 3, c.TokenTransferDelta)
	assert.Equal(t, 0, c.InternalTxCountDelta)

	_, hasZero := data.AddressStats[strings.ToLower(zeroAddress.Hex())]
	assert.False(t, hasZero, "the zero address keeps no counters")
	mockRPC.AssertExpectations(t)
}
