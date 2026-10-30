package grpcserver

import (
	"context"
	"math/big"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/nodelookup"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// archiveNode answers every block number with one transaction; hashes encode the number.
type archiveNode struct{}

func nodeTxHash(n uint64) common.Hash {
	return common.BytesToHash(new(big.Int).SetUint64(0x7000_0000 + n).Bytes())
}
func nodeBlockHash(n uint64) common.Hash {
	return common.BytesToHash(new(big.Int).SetUint64(0xb000_0000 + n).Bytes())
}

func (archiveNode) BlockByNumber(_ context.Context, n uint64) (*rpc.RawBlock, error) {
	number := hexutil.Big(*new(big.Int).SetUint64(n))
	to := common.HexToAddress("0xb0b")
	return &rpc.RawBlock{Number: &number, Hash: nodeBlockHash(n), Timestamp: hexutil.Uint64(1_700_000_000 + n),
		Transactions: []rpc.RawTransaction{{Hash: nodeTxHash(n), From: common.HexToAddress("0xa11ce"), To: &to}}}, nil
}

func (archiveNode) BlockReceipts(_ context.Context, n uint64) ([]*rpclient.Receipt, error) {
	return []*rpclient.Receipt{{Status: 1, GasUsed: 21_000, TxHash: nodeTxHash(n)}}, nil
}

func (archiveNode) BlockNumberByHash(_ context.Context, hash string) (uint64, bool, error) {
	return new(big.Int).SetBytes(common.HexToHash(hash).Bytes()).Uint64() - 0xb000_0000, true, nil
}

func (archiveNode) TxBlockNumber(_ context.Context, hash string) (uint64, bool, error) {
	return new(big.Int).SetBytes(common.HexToHash(hash).Bytes()).Uint64() - 0x7000_0000, true, nil
}

func servedBy(md metadata.MD) string {
	if v := md.Get(SourceHeader); len(v) > 0 {
		return v[0]
	}
	return "database"
}

// Below the watermark, blocks, transactions and a block's transaction list come from the node, marked as such;
// above it nothing is served from the node, and a retained block still comes from the database.
func TestPrunedHistoryIsServedFromTheNode(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()
	ctx := context.Background()
	h.prune(2)
	h.srv.cfg.NodeLookup = nodelookup.New(archiveNode{}, nodelookup.Config{})

	var md metadata.MD
	b, err := h.client.GetBlock(ctx, &indexerv1.GetBlockRequest{Selector: &indexerv1.GetBlockRequest_Number{Number: 1}}, grpc.Header(&md))
	if err != nil || b.GetHash() != nodeBlockHash(1).Hex() || servedBy(md) != "node" {
		t.Fatalf("pruned block 1: %v %v, served by %s", b, err, servedBy(md))
	}

	md = nil
	b, err = h.client.GetBlock(ctx, &indexerv1.GetBlockRequest{Selector: &indexerv1.GetBlockRequest_Hash{Hash: nodeBlockHash(2).Hex()}}, grpc.Header(&md))
	if err != nil || b.GetNumber() != 2 || servedBy(md) != "node" {
		t.Fatalf("pruned block 2 by hash: %v %v, served by %s", b, err, servedBy(md))
	}

	md = nil
	tx, err := h.client.GetTransaction(ctx, &indexerv1.GetTransactionRequest{Hash: nodeTxHash(2).Hex()}, grpc.Header(&md))
	if err != nil || tx.GetBlockNumber() != 2 || tx.GetGasUsed() != 21_000 || servedBy(md) != "node" {
		t.Fatalf("pruned tx in block 2: %v %v, served by %s", tx, err, servedBy(md))
	}

	md = nil
	list, err := h.client.ListTransactions(ctx, &indexerv1.ListTransactionsRequest{Filter: &indexerv1.ListTransactionsRequest_ByBlock{
		ByBlock: &indexerv1.ListTransactionsRequest_BlockFilter{Selector: &indexerv1.ListTransactionsRequest_BlockFilter_Number{Number: 1}}}}, grpc.Header(&md))
	if err != nil || len(list.GetTransactions()) != 1 || list.GetTransactions()[0].GetHash() != nodeTxHash(1).Hex() || servedBy(md) != "node" {
		t.Fatalf("pruned block 1's transactions: %v %v, served by %s", list, err, servedBy(md))
	}

	_, err = h.client.GetTransaction(ctx, &indexerv1.GetTransactionRequest{Hash: nodeTxHash(50).Hex()})
	requireCode(t, err, codes.NotFound)

	md = nil
	b, err = h.client.GetBlock(ctx, &indexerv1.GetBlockRequest{Selector: &indexerv1.GetBlockRequest_Number{Number: 3}}, grpc.Header(&md))
	if err != nil || servedBy(md) != "database" {
		t.Fatalf("retained block 3: %v, served by %s", err, servedBy(md))
	}

	h.srv.cfg.NodeLookup = nil
	_, err = h.client.GetBlock(ctx, &indexerv1.GetBlockRequest{Selector: &indexerv1.GetBlockRequest_Number{Number: 1}})
	requireCode(t, err, codes.OutOfRange)
}
