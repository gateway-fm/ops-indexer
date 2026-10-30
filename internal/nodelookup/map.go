package nodelookup

import (
	"strings"

	"github.com/gateway-fm/chain-indexer/internal/db"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

var transferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

// MapBlock maps a node block to the rows ingest would have written for it (indexer.buildBlock), so a pruned
// block reads the same from the node as it did from the database.
func MapBlock(raw *rpc.RawBlock, receipts []*rpclient.Receipt) *Block {
	number := raw.NumberU64()
	timestamp := uint64(raw.Timestamp)
	byHash := make(map[common.Hash]*rpclient.Receipt, len(receipts))
	for _, r := range receipts {
		if r != nil {
			byHash[r.TxHash] = r
		}
	}
	b := &Block{
		Block: &types.Block{
			Number:           number,
			Hash:             raw.Hash.Hex(),
			ParentHash:       raw.ParentHash.Hex(),
			Timestamp:        timestamp,
			GasUsed:          uint64(raw.GasUsed),
			GasLimit:         uint64(raw.GasLimit),
			BaseFeePerGas:    raw.BaseFeeU64(),
			TransactionCount: len(raw.Transactions),
			Size:             uint64(raw.Size),
			Difficulty:       raw.DifficultyString(),
			TotalDifficulty:  raw.TotalDifficultyString(),
			Nonce:            raw.NonceHex(),
			Miner:            raw.Miner.Hex(),
			ExtraData:        common.Bytes2Hex(raw.ExtraData),
			StateRoot:        raw.StateRoot.Hex(),
			TransactionsRoot: raw.TransactionsRoot.Hex(),
			ReceiptsRoot:     raw.ReceiptsRoot.Hex(),
		},
		Txs:  make([]types.Transaction, 0, len(raw.Transactions)),
		size: 512 + len(raw.ExtraData),
	}
	for idx := range raw.Transactions {
		tx := mapTransaction(&raw.Transactions[idx], byHash[raw.Transactions[idx].Hash], number, timestamp, idx)
		b.Txs = append(b.Txs, tx)
		b.size += 400 + len(tx.InputData)
	}
	return b
}

func mapTransaction(rawTx *rpc.RawTransaction, receipt *rpclient.Receipt, number, timestamp uint64, idx int) types.Transaction {
	var to *string
	if rawTx.To != nil {
		s := strings.ToLower(rawTx.To.Hex())
		to = &s
	}
	var nonce uint64
	if rawTx.Nonce != nil {
		nonce = uint64(*rawTx.Nonce)
	}
	gasLimit := uint64(rawTx.Gas)
	var gasPrice uint64
	if rawTx.GasPrice != nil {
		gasPrice = rawTx.GasPrice.ToInt().Uint64()
	}
	value := "0"
	if rawTx.Value != nil {
		value = rawTx.Value.ToInt().String()
	}
	var maxFee, maxPriority *uint64
	if rawTx.MaxFeePerGas != nil {
		v := rawTx.MaxFeePerGas.ToInt().Uint64()
		maxFee = &v
	}
	if rawTx.MaxPriorityFeePerGas != nil {
		v := rawTx.MaxPriorityFeePerGas.ToInt().Uint64()
		maxPriority = &v
	}
	tx := types.Transaction{
		Hash:                 rawTx.Hash.Hex(),
		BlockNumber:          number,
		BlockTimestamp:       timestamp,
		TxIndex:              idx,
		From:                 strings.ToLower(rawTx.From.Hex()),
		To:                   to,
		Value:                types.JSONString(value),
		GasUsed:              gasLimit,
		GasPrice:             gasPrice,
		GasLimit:             &gasLimit,
		MaxFeePerGas:         maxFee,
		MaxPriorityFeePerGas: maxPriority,
		Nonce:                &nonce,
		TxType:               int(rawTx.Type),
		InputData:            common.Bytes2Hex(rawTx.Input),
		Status:               1,
	}
	hasTransfer := false
	if receipt != nil {
		tx.GasUsed = receipt.GasUsed
		tx.Status = int(receipt.Status)
		if receipt.Status == 0 {
			msg := "transaction reverted"
			tx.Error = &msg
		}
		if rawTx.To == nil && receipt.ContractAddress != (common.Address{}) {
			s := strings.ToLower(receipt.ContractAddress.Hex())
			tx.ContractAddress = &s
		}
		for _, lg := range receipt.Logs {
			if len(lg.Topics) > 0 && lg.Topics[0] == transferTopic {
				hasTransfer = true
				break
			}
		}
	}
	tx.TxCategories = db.TxCategoryNames(&tx, hasTransfer)
	return tx
}
