package indexerv2

import (
	"strconv"
	"time"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func nativeField(m protoreflect.Message, name string, v any) error {
	if t, ok := v.(time.Time); ok && t.IsZero() {
		return nil
	}
	if v == nil {
		return nil
	}
	return setFact(m, m.Descriptor().Fields().ByName(protoreflect.Name(name)), v)
}
func nativeBlock(b *types.Block) (*pb.Block, error) {
	if b == nil {
		return nil, dataError()
	}
	out := &pb.Block{Key: &pb.BlockKey{Number: b.Number}}
	m := out.ProtoReflect()
	values := map[string]any{"hash": b.Hash, "parent_hash": b.ParentHash, "timestamp": int64(b.Timestamp), "gas_used": int64(b.GasUsed), "gas_limit": int64(b.GasLimit), "transaction_count": int64(b.TransactionCount), "size": int64(b.Size), "difficulty": b.Difficulty, "total_difficulty": b.TotalDifficulty, "nonce": b.Nonce, "miner": b.Miner, "extra_data": b.ExtraData, "state_root": b.StateRoot, "transactions_root": b.TransactionsRoot, "receipts_root": b.ReceiptsRoot, "created_at": b.CreatedAt}
	if b.BaseFeePerGas != nil {
		values["base_fee_per_gas"] = strconv.FormatUint(*b.BaseFeePerGas, 10)
	}
	for k, v := range values {
		if e := nativeField(m, k, v); e != nil {
			return nil, e
		}
	}
	return out, nil
}
func nativeTransaction(t *types.Transaction) (*pb.Transaction, error) {
	h, e := decodeHex(t.Hash, 32)
	if e != nil {
		return nil, e
	}
	if t.TxIndex < 0 {
		return nil, dataError()
	}
	out := &pb.Transaction{Key: &pb.TransactionKey{BlockNumber: t.BlockNumber, TransactionIndex: uint32(t.TxIndex), Hash: h}}
	m := out.ProtoReflect()
	values := map[string]any{"block_timestamp": int64(t.BlockTimestamp), "from": t.From, "value": string(t.Value), "gas_used": int64(t.GasUsed), "gas_price": strconv.FormatUint(t.GasPrice, 10), "transaction_type": int64(t.TxType), "input_data": t.InputData, "status": int64(t.Status), "created_at": t.CreatedAt, "categories": t.TxCategories, "token_transfer_count": int64(t.TokenTransferCount)}
	for k, p := range map[string]*string{"to": t.To, "contract_address": t.ContractAddress, "error": t.Error, "revert_reason": t.RevertReason} {
		if p != nil {
			values[k] = *p
		}
	}
	for k, p := range map[string]*uint64{"gas_limit": t.GasLimit, "nonce": t.Nonce} {
		if p != nil {
			values[k] = int64(*p)
		}
	}
	for k, p := range map[string]*uint64{"max_fee_per_gas": t.MaxFeePerGas, "max_priority_fee_per_gas": t.MaxPriorityFeePerGas} {
		if p != nil {
			values[k] = strconv.FormatUint(*p, 10)
		}
	}
	for k, v := range values {
		if e := nativeField(m, k, v); e != nil {
			return nil, e
		}
	}
	return out, nil
}
