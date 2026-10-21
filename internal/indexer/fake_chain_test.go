package indexer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

var (
	chainSender    = common.HexToAddress("0x00000000000000000000000000000000000000a1")
	chainRecipient = common.HexToAddress("0x00000000000000000000000000000000000000b2")
	chainToken     = common.HexToAddress("0x00000000000000000000000000000000000070c1")
)

// fakeChain is a node holding blocks [0, head]: blocks below busy carry one transaction each from the sender to the
// recipient, a mint of 7 to the recipient in even blocks, and every tenth block is a self-send carrying a self-transfer.
// Calls it does not script fall through to the embedded mock and fail the test.
type fakeChain struct {
	*MockRPCClient
	head, busy uint64
	calls      int
}

func blockTime(n uint64) hexutil.Uint64 { return hexutil.Uint64(1_700_000_000 + n) }

func (f *fakeChain) txHash(n uint64) common.Hash { return common.BytesToHash(new(big.Int).SetUint64(n + 1).Bytes()) }

func (f *fakeChain) BlockNumber(context.Context) (uint64, error) { f.calls++; return f.head, nil }

func (f *fakeChain) RawBlockByNumber(_ context.Context, n uint64) (*rpc.RawBlock, error) {
	f.calls++
	if n > f.head {
		return nil, fmt.Errorf("block %d above head %d", n, f.head)
	}
	num := hexutil.Big(*new(big.Int).SetUint64(n))
	b := &rpc.RawBlock{Number: &num, Hash: common.BytesToHash(new(big.Int).SetUint64(1_000_000 + n).Bytes()),
		ParentHash: common.BytesToHash(new(big.Int).SetUint64(999_999 + n).Bytes()), Timestamp: blockTime(n), GasUsed: 21000, GasLimit: 30_000_000}
	if n < f.busy {
		to := chainRecipient
		txType := hexutil.Uint64(0)
		if n%10 == 0 {
			to, txType = chainSender, 2
		}
		zero := hexutil.Big(*big.NewInt(0))
		nonce := hexutil.Uint64(n)
		b.Transactions = []rpc.RawTransaction{{Hash: f.txHash(n), BlockNumber: &num, From: chainSender, To: &to,
			Value: &zero, GasPrice: &zero, Gas: 21000, Nonce: &nonce, Type: txType}}
	}
	return b, nil
}

func (f *fakeChain) FetchReceiptsBatch(_ context.Context, hashes []common.Hash, _ int, _ int, blockNumber ...uint64) (map[common.Hash]*rpclient.Receipt, error) {
	f.calls++
	out := map[common.Hash]*rpclient.Receipt{}
	for _, h := range hashes {
		n := h.Big().Uint64() - 1
		r := &rpclient.Receipt{Status: 1, TxHash: h, GasUsed: 21000}
		if n%2 == 0 {
			r.Logs = append(r.Logs, transferEvent(common.Address{}, chainRecipient, 7, 0))
		}
		if n%10 == 0 {
			r.Logs = append(r.Logs, transferEvent(chainSender, chainSender, 1, 1))
		}
		out[h] = r
	}
	return out, nil
}

func transferEvent(from, to common.Address, value int64, index uint) *rpclient.Log {
	return &rpclient.Log{Address: chainToken, Index: index,
		Topics: []common.Hash{transferTopic, common.BytesToHash(from.Bytes()), common.BytesToHash(to.Bytes())},
		Data:   hexutil.Bytes(common.LeftPadBytes(big.NewInt(value).Bytes(), 32))}
}

func (f *fakeChain) FetchTokenMetadataBatch(_ context.Context, addrs []common.Address, _ int, _ int) (map[common.Address]*rpc.TokenMetadataResult, error) {
	f.calls++
	name := "Chain Token"
	out := map[common.Address]*rpc.TokenMetadataResult{}
	for _, a := range addrs {
		out[a] = &rpc.TokenMetadataResult{Address: a, Symbol: "CT", Name: &name, Decimals: 18}
	}
	return out, nil
}

func (f *fakeChain) FetchTracesBatch(context.Context, []common.Hash, uint64, uint64, int, int) ([]*types.InternalTransaction, error) {
	f.calls++
	return nil, nil
}
