package rpc

import (
	"context"
	"fmt"
	"math/big"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/hexutil"
)

type RawTransaction struct {
	Hash             common.Hash    `json:"hash"`
	BlockHash        common.Hash    `json:"blockHash"`
	BlockNumber      *hexutil.Big   `json:"blockNumber"`
	TransactionIndex hexutil.Uint64 `json:"transactionIndex"`
	From             common.Address `json:"from"`
	To               *common.Address `json:"to"`
	Value            *hexutil.Big   `json:"value"`
	Gas              hexutil.Uint64 `json:"gas"`
	GasPrice         *hexutil.Big   `json:"gasPrice"`
	Input            hexutil.Bytes  `json:"input"`
	Nonce            *hexutil.Uint64 `json:"nonce"`
	Type             hexutil.Uint64 `json:"type"`

	MaxFeePerGas         *hexutil.Big `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas *hexutil.Big `json:"maxPriorityFeePerGas,omitempty"`

	V *hexutil.Big `json:"v,omitempty"`
	R *hexutil.Big `json:"r,omitempty"`
	S *hexutil.Big `json:"s,omitempty"`

	SourceHash  *common.Hash `json:"sourceHash,omitempty"`
	Mint        *hexutil.Big `json:"mint,omitempty"`
	IsSystemTx  *FlexBool    `json:"isSystemTx,omitempty"`

	DepositNonce          *hexutil.Uint64 `json:"depositNonce,omitempty"`
	DepositReceiptVersion *hexutil.Uint64 `json:"depositReceiptVersion,omitempty"`
}

type RawBlock struct {
	Number           *hexutil.Big    `json:"number"`
	Hash             common.Hash     `json:"hash"`
	ParentHash       common.Hash     `json:"parentHash"`
	Timestamp        hexutil.Uint64  `json:"timestamp"`
	GasUsed          hexutil.Uint64  `json:"gasUsed"`
	GasLimit         hexutil.Uint64  `json:"gasLimit"`
	BaseFeePerGas    *hexutil.Big    `json:"baseFeePerGas,omitempty"`
	Miner            common.Address  `json:"miner"`
	Difficulty       *hexutil.Big    `json:"difficulty"`
	TotalDifficulty  *hexutil.Big    `json:"totalDifficulty,omitempty"`
	Size             hexutil.Uint64  `json:"size"`
	Nonce            hexutil.Bytes   `json:"nonce"`
	ExtraData        hexutil.Bytes   `json:"extraData"`
	StateRoot        common.Hash     `json:"stateRoot"`
	TransactionsRoot common.Hash     `json:"transactionsRoot"`
	ReceiptsRoot     common.Hash     `json:"receiptsRoot"`

	Transactions []RawTransaction `json:"transactions"`
}

func (b *RawBlock) NumberU64() uint64 {
	if b.Number == nil {
		return 0
	}
	return b.Number.ToInt().Uint64()
}

func (b *RawBlock) DifficultyString() string {
	if b.Difficulty == nil {
		return "0"
	}
	return b.Difficulty.ToInt().String()
}

func (b *RawBlock) TotalDifficultyString() string {
	if b.TotalDifficulty == nil {
		return "0"
	}
	return b.TotalDifficulty.ToInt().String()
}

func (b *RawBlock) NonceHex() string {
	if len(b.Nonce) == 8 {
		return fmt.Sprintf("0x%016x", new(big.Int).SetBytes(b.Nonce).Uint64())
	}
	return "0x0000000000000000"
}

func (b *RawBlock) BaseFeeU64() *uint64 {
	if b.BaseFeePerGas == nil {
		return nil
	}
	v := b.BaseFeePerGas.ToInt().Uint64()
	return &v
}

func (c *Client) RawBlockByNumber(ctx context.Context, number uint64) (*RawBlock, error) {
	var raw RawBlock
	err := c.raw.CallContext(ctx, &raw, "eth_getBlockByNumber", toHex(number), true)
	if err != nil {
		return nil, fmt.Errorf("raw block fetch for %d: %w", number, err)
	}
	if raw.Number == nil {
		return nil, fmt.Errorf("block %d not found", number)
	}
	return &raw, nil
}

func (c *Client) RawBlockHash(ctx context.Context, number uint64) (string, error) {
	var raw struct {
		Hash common.Hash `json:"hash"`
	}
	err := c.raw.CallContext(ctx, &raw, "eth_getBlockByNumber", toHex(number), false)
	if err != nil {
		return "", fmt.Errorf("raw block hash fetch for %d: %w", number, err)
	}
	return raw.Hash.Hex(), nil
}
