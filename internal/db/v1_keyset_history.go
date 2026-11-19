package db

import (
	"context"
	"fmt"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/jackc/pgx/v5"
)

// All optional filters are combined with AND, including the legacy address
// transfer-participant rule. Ranges retain V1's half-open upper bound.
type V1TransactionFilter struct {
	Address, Direction, Hash string
	Block                    *uint64
	From, To                 uint64
	TimeFrom, TimeTo         *int64
	CategoryMask             int16
	Status                   *int
	GlobalTotal              bool
	RetainedTotal            bool
}

func (d *DB) V1Transactions(ctx context.Context, f V1TransactionFilter, p V1PageInput) (V1PageResult[types.Transaction], error) {
	q := v1Query[types.Transaction]{columns: `t.hash,t.block_number,t.block_timestamp,t.tx_index,t.from_address,t.to_address,t.value::text,t.gas_used,t.gas_price,t.gas_limit,t.max_fee_per_gas,t.max_priority_fee_per_gas,t.nonce,t.tx_type,t.input_data,t.status,t.error,t.revert_reason,t.created_at,t.categories`, from: "transactions t", history: "t.block_number", order: []v1Order{v1Desc("t.block_number", "bigint"), v1Desc("t.tx_index", "int"), v1Desc("t.hash", "text")}, scan: scanTransactionsWithCategories, key: func(t types.Transaction) []string { return []string{v1U(t.BlockNumber), v1I(t.TxIndex), t.Hash} }, block: func(t types.Transaction) uint64 { return t.BlockNumber }}
	q.skipTotal = !f.GlobalTotal && !f.RetainedTotal
	if len(d.HiddenTxTypes) > 0 {
		q.add("NOT(t.tx_type=ANY($%d::int[]))", d.HiddenTxTypes)
	}
	q.blockRange(f.From, f.To)
	if f.Address != "" {
		switch f.Direction {
		case "from":
			q.add("t.from_address=$%d", f.Address)
		case "to":
			q.add("t.to_address=$%d", f.Address)
		default:
			q.args = append(q.args, f.Address)
			n := len(q.args)
			q.conditions = append(q.conditions, fmtV1AddressTx(n))
		}
	}
	if f.Block != nil {
		q.add("t.block_number=$%d", *f.Block)
		q.lower = *f.Block
		q.upper = *f.Block + 1
		q.order = []v1Order{v1Asc("t.tx_index", "int"), v1Asc("t.hash", "text")}
		q.key = func(t types.Transaction) []string { return []string{v1I(t.TxIndex), t.Hash} }
	}
	if f.Hash != "" {
		q.blockHash = f.Hash
		q.add("EXISTS(SELECT 1 FROM blocks b WHERE b.hash=$%d AND b.number=t.block_number)", f.Hash)
		q.order = []v1Order{v1Asc("t.tx_index", "int"), v1Asc("t.hash", "text")}
		q.key = func(t types.Transaction) []string { return []string{v1I(t.TxIndex), t.Hash} }
	}
	if f.TimeFrom != nil {
		q.add("t.block_timestamp>=$%d", *f.TimeFrom)
	}
	if f.TimeTo != nil {
		q.add("t.block_timestamp<$%d", *f.TimeTo)
	}
	if f.Status != nil {
		q.add("t.status=$%d", *f.Status)
	}
	if f.CategoryMask != 0 {
		q.add("(t.categories & $%d::smallint)<>0", f.CategoryMask)
	}
	if f.GlobalTotal && len(d.HiddenTxTypes) == 0 {
		q.total = `SELECT COALESCE((SELECT count FROM chain_counters WHERE name='transactions_total'),(SELECT count(*) FROM transactions WHERE block_number>=history_from()))`
	}
	return v1Read(ctx, d, q, p)
}
func fmtV1AddressTx(n int) string {
	return fmt.Sprintf(`(t.from_address=$%d OR t.to_address=$%d OR EXISTS(SELECT 1 FROM token_transfers tt WHERE tt.block_number=t.block_number AND tt.tx_hash=t.hash AND (tt.from_address=$%d OR tt.to_address=$%d)))`, n, n, n, n)
}

type V1LogFilter struct {
	TxHash, Address, Topic string
	From, To               uint64
}

func (d *DB) V1Logs(ctx context.Context, f V1LogFilter, p V1PageInput) (V1PageResult[types.Log], error) {
	q := v1Query[types.Log]{columns: `id,tx_hash,log_index,address,topic0,topic1,topic2,topic3,data,block_number,timestamp,removed`, from: "logs", history: "block_number", order: []v1Order{v1Desc("block_number", "bigint"), v1Desc("log_index", "int"), v1Desc("tx_hash", "text"), v1Desc("id", "bigint")}, scan: scanLogs, key: func(t types.Log) []string {
		return []string{v1U(t.BlockNumber), v1I(t.LogIndex), t.TxHash, v1I64(t.ID)}
	}, block: func(t types.Log) uint64 { return t.BlockNumber }}
	q.blockRange(f.From, f.To)
	if f.TxHash != "" {
		q.add("tx_hash=$%d", f.TxHash)
		q.order = []v1Order{v1Asc("log_index", "int"), v1Asc("id", "bigint")}
		q.key = func(t types.Log) []string { return []string{v1I(t.LogIndex), v1I64(t.ID)} }
	}
	if f.Address != "" {
		q.add("address=$%d", f.Address)
	}
	if f.Topic != "" {
		q.add("topic0=$%d", f.Topic)
	}
	return v1Read(ctx, d, q, p)
}

type V1TransferFilter struct {
	TxHash, Address, Token, TokenType string
	From, To                          uint64
	Cumulative                        bool
}

func (d *DB) V1Transfers(ctx context.Context, f V1TransferFilter, p V1PageInput) (V1PageResult[types.TokenTransfer], error) {
	q := v1Query[types.TokenTransfer]{columns: `id,tx_hash,log_index,token_address,from_address,to_address,value::text,block_number,timestamp,transfer_type,token_type,token_id,is_internal`, from: "token_transfers", history: "block_number", order: []v1Order{v1Desc("block_number", "bigint"), v1Desc("log_index", "int"), v1Desc("tx_hash", "text"), v1Desc("id", "bigint")}, scan: scanTokenTransfers, key: func(t types.TokenTransfer) []string {
		return []string{v1U(t.BlockNumber), v1I(t.LogIndex), t.TxHash, v1I64(t.ID)}
	}, block: func(t types.TokenTransfer) uint64 { return t.BlockNumber }}
	q.blockRange(f.From, f.To)
	if f.TxHash != "" {
		q.add("tx_hash=$%d", f.TxHash)
		q.order = []v1Order{v1Asc("log_index", "int"), v1Asc("id", "bigint")}
		q.key = func(t types.TokenTransfer) []string { return []string{v1I(t.LogIndex), v1I64(t.ID)} }
	}
	if f.Address != "" {
		q.args = append(q.args, f.Address)
		n := len(q.args)
		q.conditions = append(q.conditions, fmt.Sprintf("(from_address=$%d OR to_address=$%d)", n, n))
	}
	if f.Token != "" {
		q.add("token_address=$%d", f.Token)
	}
	if f.TokenType != "" {
		q.add("token_type=$%d", f.TokenType)
	}
	if f.Cumulative {
		switch {
		case f.Address != "" && f.Token == "" && f.TxHash == "" && f.From == 0 && f.To == 0:
			q.total = `SELECT COALESCE((SELECT token_transfer_count FROM address_stats WHERE address=$1),0)`
			q.totalArgs = []any{f.Address}
		case f.Token != "" && f.Address == "" && f.TxHash == "" && f.From == 0 && f.To == 0:
			q.total = `SELECT COALESCE((SELECT COALESCE(transfer_count,0) FROM tokens WHERE address=$1),(SELECT count(*) FROM token_transfers WHERE token_address=$1 AND block_number>=history_from()))`
			q.totalArgs = []any{f.Token}
		case f.Address == "" && f.Token == "" && f.TxHash == "" && f.TokenType == "":
			q.total = `SELECT COALESCE((SELECT count FROM chain_counters WHERE name='transfers_total'),(SELECT count(*) FROM token_transfers WHERE block_number>=history_from()))`
		}
	}
	return v1Read(ctx, d, q, p)
}

type V1InternalFilter struct {
	TxHash, Address, Hash string
	Block                 *uint64
}

func (d *DB) V1Internals(ctx context.Context, f V1InternalFilter, p V1PageInput) (V1PageResult[types.InternalTransaction], error) {
	q := v1Query[types.InternalTransaction]{columns: `id,tx_hash,block_number,trace_address,from_address,to_address,value::text,gas,gas_used,input,output,call_type,error,timestamp`, from: "internal_transactions", history: "block_number", order: []v1Order{v1Desc("block_number", "bigint"), v1Desc("id", "bigint"), v1Desc("tx_hash", "text"), v1Desc("trace_address", "text")}, scan: scanInternalTransactions, key: func(t types.InternalTransaction) []string {
		return []string{v1U(t.BlockNumber), v1I64(t.ID), t.TxHash, t.TraceAddress}
	}, block: func(t types.InternalTransaction) uint64 { return t.BlockNumber }}
	if f.TxHash != "" {
		q.add("tx_hash=$%d", f.TxHash)
		q.order = []v1Order{v1Asc("trace_address", "text"), v1Asc("tx_hash", "text"), v1Asc("id", "bigint")}
		q.key = func(t types.InternalTransaction) []string { return []string{t.TraceAddress, t.TxHash, v1I64(t.ID)} }
	}
	if f.Address != "" {
		q.args = append(q.args, f.Address)
		n := len(q.args)
		q.conditions = append(q.conditions, fmt.Sprintf("(from_address=$%d OR to_address=$%d)", n, n))
		if f.TxHash == "" && f.Block == nil && f.Hash == "" {
			q.total = `SELECT COALESCE((SELECT internal_tx_count FROM address_stats WHERE address=$1),0)`
			q.totalArgs = []any{f.Address}
		}
	}
	if f.Block != nil {
		q.add("block_number=$%d", *f.Block)
		q.lower = *f.Block
		q.upper = *f.Block + 1
		if f.TxHash == "" {
			q.order = []v1Order{v1Asc("id", "bigint"), v1Asc("tx_hash", "text"), v1Asc("trace_address", "text")}
			q.key = func(t types.InternalTransaction) []string { return []string{v1I64(t.ID), t.TxHash, t.TraceAddress} }
		}
	}
	if f.Hash != "" {
		q.blockHash = f.Hash
		q.add("EXISTS(SELECT 1 FROM blocks b WHERE b.hash=$%d AND b.number=internal_transactions.block_number)", f.Hash)
		if f.TxHash == "" {
			q.order = []v1Order{v1Asc("id", "bigint"), v1Asc("tx_hash", "text"), v1Asc("trace_address", "text")}
			q.key = func(t types.InternalTransaction) []string { return []string{v1I64(t.ID), t.TxHash, t.TraceAddress} }
		}
	}

	return v1Read(ctx, d, q, p)
}
func (d *DB) V1Blocks(ctx context.Context, before uint64, p V1PageInput) (V1PageResult[types.Block], error) {
	q := v1Query[types.Block]{columns: `number,hash,parent_hash,timestamp,gas_used,gas_limit,base_fee_per_gas,transaction_count,size,difficulty,total_difficulty,nonce,miner,extra_data,state_root,transactions_root,receipts_root,created_at`, from: "blocks", history: "number", order: []v1Order{v1Desc("number", "bigint")}, scan: func(rows pgx.Rows) ([]types.Block, error) {
		var out []types.Block
		for rows.Next() {
			var b types.Block
			if err := rows.Scan(&b.Number, &b.Hash, &b.ParentHash, &b.Timestamp, &b.GasUsed, &b.GasLimit, &b.BaseFeePerGas, &b.TransactionCount, &b.Size, &b.Difficulty, &b.TotalDifficulty, &b.Nonce, &b.Miner, &b.ExtraData, &b.StateRoot, &b.TransactionsRoot, &b.ReceiptsRoot, &b.CreatedAt); err != nil {
				return nil, err
			}
			out = append(out, b)
		}
		return out, rows.Err()
	}, key: func(b types.Block) []string { return []string{v1U(b.Number)} }, block: func(b types.Block) uint64 { return b.Number }}
	q.skipTotal = true
	q.blockRange(0, before)
	return v1Read(ctx, d, q, p)
}
