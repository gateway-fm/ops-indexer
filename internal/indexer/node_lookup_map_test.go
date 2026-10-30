package indexer

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/gateway-fm/chain-indexer/internal/nodelookup"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// A pruned block served from the node reads as ingest wrote it: the node lookup maps the same raw block and
// receipts to the same rows buildBlock produces. Ingest's addresses are lowercased by the insert (LOWER($n)), and
// block_timestamp and categories are filled on the way into the database; the lookup fills them itself.
func TestNodeLookupMapsABlockLikeIngest(t *testing.T) {
	ctx := context.Background()
	mockRPC := new(MockRPCClient)
	idx := New(new(MockDatabase), mockRPC, time.Second, 0)
	idx.tokenCache.Add(statsToken.Hex())

	number := hexutil.Big(*big.NewInt(4_242))
	plain, failed, call, create := common.HexToHash("0xa1"), common.HexToHash("0xb2"), common.HexToHash("0xc3"), common.HexToHash("0xd4")
	value := hexutil.Big(*big.NewInt(1_000))
	fee, tip := hexutil.Big(*big.NewInt(7)), hexutil.Big(*big.NewInt(2))
	baseFee := hexutil.Big(*big.NewInt(5))

	plainTx := statsTx(plain, 0, statsA, statsB, &number)
	plainTx.Value = &value
	failedTx := statsTx(failed, 1, statsB, statsC, &number)
	failedTx.Input = hexutil.Bytes{0xa9, 0x05, 0x9c, 0xbb, 0x01}
	callTx := statsTx(call, 2, statsC, statsToken, &number)
	callTx.Type, callTx.MaxFeePerGas, callTx.MaxPriorityFeePerGas = 2, &fee, &tip
	callTx.Input = hexutil.Bytes{0xa9, 0x05, 0x9c, 0xbb}
	createTx := statsTx(create, 3, statsA, statsA, &number)
	createTx.To, createTx.Input = nil, hexutil.Bytes{0x60, 0x80}
	created := common.HexToAddress("0x00000000000000000000000000000000000c0de1")

	raw := &rpc.RawBlock{
		Number: &number, Hash: common.HexToHash("0xb10c4242"), ParentHash: common.HexToHash("0xb10c4241"),
		Timestamp: 1_700_000_000, GasUsed: 90_000, GasLimit: 30_000_000, BaseFeePerGas: &baseFee,
		Miner: statsC, Size: 1_234, ExtraData: hexutil.Bytes{0x01, 0x02},
		Transactions: []rpc.RawTransaction{plainTx, failedTx, callTx, createTx},
	}
	receipts := []*rpclient.Receipt{
		{Status: 1, GasUsed: 21_000, TxHash: plain},
		{Status: 0, GasUsed: 30_000, TxHash: failed},
		{Status: 1, GasUsed: 40_000, TxHash: call, Logs: []*rpclient.Log{transferLog(statsC, statsA, 0)}},
		{Status: 1, GasUsed: 50_000, TxHash: create, ContractAddress: created},
	}
	byHash := map[common.Hash]*rpclient.Receipt{}
	for _, r := range receipts {
		byHash[r.TxHash] = r
	}
	mockRPC.On("GetCode", mock.Anything, created).Return([]byte{0x60, 0x80}, nil).Maybe()

	ingest := idx.buildBlock(ctx, raw, byHash).data
	node := nodelookup.MapBlock(raw, receipts)

	assert.Equal(t, *ingest.Block, *node.Block)
	lower := func(s *string) *string {
		if s == nil {
			return nil
		}
		l := strings.ToLower(*s)
		return &l
	}
	// Ingest stores input without "0x" and the category rule wants "0x" plus 8 hex digits, so a bare 4-byte selector
	// is not a contract_call in the database; the node path serves what the database would.
	wantCategories := [][]string{{"coin_transfer"}, {"contract_call"}, {"token_transfer"}, {"contract_creation"}}
	assert.Len(t, node.Txs, len(ingest.Transactions))
	for i, in := range ingest.Transactions {
		want := *in
		want.From, want.To, want.ContractAddress = strings.ToLower(want.From), lower(want.To), lower(want.ContractAddress)
		want.BlockTimestamp = uint64(raw.Timestamp)
		got := node.Txs[i]
		assert.ElementsMatch(t, wantCategories[i], got.TxCategories, "tx %d categories", i)
		got.TxCategories = nil
		assert.Equal(t, want, got, "tx %d", i)
	}
}
