package nodelookup

import (
	"context"
	"fmt"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

// NodeSource is Source over the indexer's RPC client. The node answers null for what it does not have.
type NodeSource struct{ c *rpc.Client }

func NewNodeSource(c *rpc.Client) NodeSource { return NodeSource{c: c} }

func (s NodeSource) BlockByNumber(ctx context.Context, number uint64) (*rpc.RawBlock, error) {
	var raw *rpc.RawBlock
	if err := s.c.Raw().CallContext(ctx, &raw, "eth_getBlockByNumber", fmt.Sprintf("0x%x", number), true); err != nil {
		return nil, err
	}
	return raw, nil
}

func (s NodeSource) BlockReceipts(ctx context.Context, number uint64) ([]*rpclient.Receipt, error) {
	return s.c.FetchBlockReceipts(ctx, number)
}

func (s NodeSource) BlockNumberByHash(ctx context.Context, hash string) (uint64, bool, error) {
	var head *struct {
		Number *hexutil.Big `json:"number"`
	}
	if err := s.c.Raw().CallContext(ctx, &head, "eth_getBlockByHash", hash, false); err != nil || head == nil || head.Number == nil {
		return 0, false, err
	}
	return head.Number.ToInt().Uint64(), true, nil
}

func (s NodeSource) TxBlockNumber(ctx context.Context, hash string) (uint64, bool, error) {
	var tx *struct {
		BlockNumber *hexutil.Big `json:"blockNumber"`
	}
	if err := s.c.Raw().CallContext(ctx, &tx, "eth_getTransactionByHash", hash); err != nil || tx == nil || tx.BlockNumber == nil {
		return 0, false, err
	}
	return tx.BlockNumber.ToInt().Uint64(), true, nil
}
