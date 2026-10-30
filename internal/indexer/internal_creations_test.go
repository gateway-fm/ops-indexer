package indexer

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// PRST-5426: on a production deployment the indexer knew 226 contracts, Blockscout 1,265; the 1,022 deployed by other contracts were
// missing, because only a top-level creation's receipt made a contracts row. A factory's CREATE / CREATE2 frames
// now do too, unless the creation was undone.
func TestFactoryDeployedContractsAreRecorded(t *testing.T) {
	ctx := context.Background()
	mockRPC := new(MockRPCClient)
	idx := New(new(MockDatabase), mockRPC, time.Second, 0)
	idx.tracingSupported = true

	number := hexutil.Big(*big.NewInt(900))
	deploy, failedTx := common.HexToHash("0xf1"), common.HexToHash("0xf2")
	factory := common.HexToAddress("0xfac7")
	lower := func(a common.Address) string { return strings.ToLower(a.Hex()) }
	childA, childB, childC, childD, childE :=
		common.HexToAddress("0xc0a"), common.HexToAddress("0xc0b"), common.HexToAddress("0xc0c"), common.HexToAddress("0xc0d"), common.HexToAddress("0xc0e")
	frame := func(tx common.Hash, path, callType string, from, to common.Address, output, errMsg string) *types.InternalTransaction {
		f := &types.InternalTransaction{TxHash: tx.Hex(), BlockNumber: 900, TraceAddress: path, CallType: callType, From: lower(from)}
		t := lower(to)
		f.To = &t
		if output != "" {
			f.Output = &output
		}
		if errMsg != "" {
			f.Error = &errMsg
		}
		return f
	}
	frames := []*types.InternalTransaction{
		frame(deploy, "", "call", statsA, factory, "", ""),
		frame(deploy, "0", "create2", factory, childA, "0x6080aa", ""),
		frame(deploy, "1", "call", factory, statsB, "", "execution reverted"),
		frame(deploy, "1,0", "create", statsB, childB, "0x6080bb", ""),
		frame(deploy, "2", "create", factory, childC, "", "out of gas"),
		frame(deploy, "3", "create", factory, childD, "", ""),
		frame(failedTx, "", "call", statsA, factory, "", "execution reverted"),
		frame(failedTx, "0", "create2", factory, childE, "0x6080ee", ""),
	}
	mockRPC.On("FetchTracesBatch", ctx, mock.Anything, uint64(900), mock.Anything, mock.Anything, mock.Anything).Return(frames, nil).Once()
	mockRPC.On("GetCode", mock.Anything, childD).Return([]byte{0x60, 0x80, 0xdd}, nil).Once()

	raw := &rpc.RawBlock{Number: &number, Hash: common.HexToHash("0xb900"), Timestamp: hexutil.Uint64(time.Now().Unix()),
		Transactions: []rpc.RawTransaction{statsTx(deploy, 0, statsA, factory, &number), statsTx(failedTx, 1, statsA, factory, &number)}}
	receipts := map[common.Hash]*rpclient.Receipt{deploy: {Status: 1, TxHash: deploy}, failedTx: {Status: 0, TxHash: failedTx}}

	data := idx.buildBlock(ctx, raw, receipts).data

	got := map[string]*types.Contract{}
	for _, c := range data.Contracts {
		got[c.Address] = c
	}
	assert.ElementsMatch(t, []string{lower(childA), lower(childD)}, keys(got),
		"B's enclosing call reverted, C's create failed, E's transaction failed")
	a := got[lower(childA)]
	assert.Equal(t, "6080aa", a.Bytecode, "the deployed code from the CREATE frame's output")
	assert.Equal(t, lower(factory), a.Creator, "the factory deployed it")
	assert.Equal(t, deploy.Hex(), a.CreationTx)
	assert.EqualValues(t, 900, a.BlockNumber)
	assert.Equal(t, "6080dd", got[lower(childD)].Bytecode, "no output in the frame: read from the node")
	assert.True(t, data.AddressStats[lower(childA)].IsContract)
	mockRPC.AssertExpectations(t)
}

func keys(m map[string]*types.Contract) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
