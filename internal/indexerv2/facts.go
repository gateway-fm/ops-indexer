package indexerv2

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	pb "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v2"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type definition struct {
	table, columns string
	order          []string
	fresh          func() proto.Message
}

func define(f string) definition {
	switch f {
	case "blocks":
		return definition{table: `blocks r`, order: []string{`r.number`}, columns: `
r.number AS "_number",
CASE WHEN octet_length(r.hash)>4194306 THEN NULL ELSE r.hash END AS "hash",
CASE WHEN octet_length(r.parent_hash)>4194306 THEN NULL ELSE r.parent_hash END AS "parent_hash",
r.timestamp AS "timestamp",
r.gas_used AS "gas_used",
r.gas_limit AS "gas_limit",
r.base_fee_per_gas::text AS "base_fee_per_gas",
r.transaction_count AS "transaction_count",
r.size AS "size",
CASE WHEN octet_length(r.difficulty)>2097152 THEN NULL ELSE r.difficulty END AS "difficulty",
CASE WHEN octet_length(r.total_difficulty)>2097152 THEN NULL ELSE r.total_difficulty END AS "total_difficulty",
CASE WHEN octet_length(r.nonce)>4194306 THEN NULL ELSE r.nonce END AS "nonce",
CASE WHEN octet_length(r.miner)>4194306 THEN NULL ELSE r.miner END AS "miner",
CASE WHEN octet_length(r.extra_data)>4194306 THEN NULL ELSE r.extra_data END AS "extra_data",
CASE WHEN octet_length(r.state_root)>4194306 THEN NULL ELSE r.state_root END AS "state_root",
CASE WHEN octet_length(r.transactions_root)>4194306 THEN NULL ELSE r.transactions_root END AS "transactions_root",
CASE WHEN octet_length(r.receipts_root)>4194306 THEN NULL ELSE r.receipts_root END AS "receipts_root",
r.created_at AS "created_at",
(COALESCE((octet_length(r.hash)>4194306),false) OR COALESCE((octet_length(r.parent_hash)>4194306),false) OR COALESCE((octet_length(r.difficulty)>2097152),false) OR COALESCE((octet_length(r.total_difficulty)>2097152),false) OR COALESCE((octet_length(r.nonce)>4194306),false) OR COALESCE((octet_length(r.miner)>4194306),false) OR COALESCE((octet_length(r.extra_data)>4194306),false) OR COALESCE((octet_length(r.state_root)>4194306),false) OR COALESCE((octet_length(r.transactions_root)>4194306),false) OR COALESCE((octet_length(r.receipts_root)>4194306),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.Block{} }}
	case "transactions":
		return definition{table: `transactions r JOIN blocks b ON b.number=r.block_number`, order: []string{`r.block_number`, `r.tx_index`, `r.hash COLLATE "C"`}, columns: `
r.block_number AS "_block",
r.tx_index AS "_index",
CASE WHEN octet_length(r.hash)>4194306 THEN NULL ELSE r.hash END AS "_hash",
b.timestamp AS block_timestamp,
CASE WHEN octet_length(r.from_address)>4194306 THEN NULL ELSE r.from_address END AS "from",
CASE WHEN octet_length(r.to_address)>4194306 THEN NULL ELSE r.to_address END AS "to",
(SELECT left(c.address,43) FROM contracts c WHERE r.to_address IS NULL AND c.creation_tx=r.hash AND c.block_number=r.block_number AND c.creator=r.from_address ORDER BY c.address LIMIT 1) AS contract_address,
r.value::text AS "value",
r.gas_used AS "gas_used",
r.gas_price::text AS "gas_price",
r.gas_limit AS "gas_limit",
r.max_fee_per_gas::text AS "max_fee_per_gas",
r.max_priority_fee_per_gas::text AS "max_priority_fee_per_gas",
r.nonce AS "nonce",
r.tx_type AS "transaction_type",
CASE WHEN octet_length(r.input_data)>4194306 THEN NULL ELSE r.input_data END AS "input_data",
r.status AS "status",
CASE WHEN octet_length(r.error)>2097152 THEN NULL ELSE r.error END AS "error",
CASE WHEN octet_length(r.revert_reason)>2097152 THEN NULL ELSE r.revert_reason END AS "revert_reason",
r.created_at AS "created_at",
ARRAY_REMOVE(ARRAY[CASE WHEN (r.categories&1)<>0 THEN 'coin_transfer' END,CASE WHEN (r.categories&2)<>0 THEN 'contract_creation' END,CASE WHEN (r.categories&4)<>0 THEN 'contract_call' END,CASE WHEN (r.categories&8)<>0 THEN 'token_transfer' END],NULL) AS categories,
(SELECT count(*) FROM token_transfers tt WHERE tt.tx_hash=r.hash AND tt.block_number=r.block_number) AS token_transfer_count,
(COALESCE((octet_length(r.hash)>4194306),false) OR COALESCE((octet_length(r.from_address)>4194306),false) OR COALESCE((octet_length(r.to_address)>4194306),false) OR COALESCE((octet_length(r.input_data)>4194306),false) OR COALESCE((octet_length(r.error)>2097152),false) OR COALESCE((octet_length(r.revert_reason)>2097152),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.Transaction{} }}
	case "logs":
		return definition{table: `logs r JOIN blocks b ON b.number=r.block_number`, order: []string{`r.block_number`, `r.log_index`}, columns: `
r.block_number AS "_block",
r.log_index AS "_index",
r.id AS "id",
CASE WHEN octet_length(r.tx_hash)<=66 THEN r.tx_hash END AS "transaction_hash",
CASE WHEN octet_length(r.address)>4194306 THEN NULL ELSE r.address END AS "address",
CASE WHEN octet_length(r.topic0)>4194306 THEN NULL ELSE r.topic0 END AS "topic0",
CASE WHEN octet_length(r.topic1)>4194306 THEN NULL ELSE r.topic1 END AS "topic1",
CASE WHEN octet_length(r.topic2)>4194306 THEN NULL ELSE r.topic2 END AS "topic2",
CASE WHEN octet_length(r.topic3)>4194306 THEN NULL ELSE r.topic3 END AS "topic3",
CASE WHEN octet_length(r.data)>4194306 THEN NULL ELSE r.data END AS "data",
r.timestamp AS "timestamp",
r.removed AS "removed",
(COALESCE((octet_length(r.address)>4194306),false) OR COALESCE((octet_length(r.topic0)>4194306),false) OR COALESCE((octet_length(r.topic1)>4194306),false) OR COALESCE((octet_length(r.topic2)>4194306),false) OR COALESCE((octet_length(r.topic3)>4194306),false) OR COALESCE((octet_length(r.data)>4194306),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.Log{} }}
	case "transfers":
		return definition{table: `token_transfers r JOIN blocks b ON b.number=r.block_number`, order: []string{`r.block_number`, `r.log_index`}, columns: `
r.block_number AS "_block",
r.log_index AS "_index",
r.id AS "id",
CASE WHEN octet_length(r.tx_hash)<=66 THEN r.tx_hash END AS "transaction_hash",
CASE WHEN octet_length(r.token_address)>4194306 THEN NULL ELSE r.token_address END AS "token_address",
CASE WHEN octet_length(r.from_address)>4194306 THEN NULL ELSE r.from_address END AS "from",
CASE WHEN octet_length(r.to_address)>4194306 THEN NULL ELSE r.to_address END AS "to",
r.value::text AS "value",
r.timestamp AS "timestamp",
CASE WHEN octet_length(r.transfer_type)>2097152 THEN NULL ELSE r.transfer_type END AS "transfer_type",
CASE WHEN octet_length(r.token_type)>2097152 THEN NULL ELSE r.token_type END AS "token_type",
r.token_id::text AS "token_id",
r.is_internal AS "is_internal",
(COALESCE((octet_length(r.token_address)>4194306),false) OR COALESCE((octet_length(r.from_address)>4194306),false) OR COALESCE((octet_length(r.to_address)>4194306),false) OR COALESCE((octet_length(r.transfer_type)>2097152),false) OR COALESCE((octet_length(r.token_type)>2097152),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.TokenTransfer{} }}
	case "internals":
		return definition{table: `internal_transactions r JOIN blocks b ON b.number=r.block_number`, order: []string{`r.block_number`, `r.tx_hash COLLATE "C"`, `r.trace_address COLLATE "C"`}, columns: `
r.block_number AS "_block",
r.tx_hash AS "_hash",
CASE WHEN octet_length(r.trace_address)>2097152 THEN NULL ELSE r.trace_address END AS "_trace",
r.id AS "id",
CASE WHEN octet_length(r.from_address)>4194306 THEN NULL ELSE r.from_address END AS "from",
CASE WHEN octet_length(r.to_address)>4194306 THEN NULL ELSE r.to_address END AS "to",
r.value::text AS "value",
r.gas AS "gas",
r.gas_used AS "gas_used",
CASE WHEN octet_length(r.input)>4194306 THEN NULL ELSE r.input END AS "input",
CASE WHEN octet_length(r.output)>4194306 THEN NULL ELSE r.output END AS "output",
CASE WHEN octet_length(r.call_type)>2097152 THEN NULL ELSE r.call_type END AS "call_type",
CASE WHEN octet_length(r.error)>2097152 THEN NULL ELSE r.error END AS "error",
r.timestamp AS "timestamp",
(COALESCE((octet_length(r.trace_address)>2097152),false) OR COALESCE((octet_length(r.from_address)>4194306),false) OR COALESCE((octet_length(r.to_address)>4194306),false) OR COALESCE((octet_length(r.input)>4194306),false) OR COALESCE((octet_length(r.output)>4194306),false) OR COALESCE((octet_length(r.call_type)>2097152),false) OR COALESCE((octet_length(r.error)>2097152),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.InternalTransaction{} }}
	case "contracts":
		return definition{table: `contracts r`, order: []string{`r.block_number`, `r.address COLLATE "C"`}, columns: `
r.block_number AS "_block",
CASE WHEN octet_length(r.address)>4194306 THEN NULL ELSE r.address END AS "_address",
CASE WHEN octet_length(r.bytecode)>4194306 THEN NULL ELSE r.bytecode END AS "bytecode",
CASE WHEN octet_length(r.bytecode_hash)>4194306 THEN NULL ELSE r.bytecode_hash END AS "bytecode_hash",
CASE WHEN octet_length(r.creator)>4194306 THEN NULL ELSE r.creator END AS "creator",
CASE WHEN octet_length(r.creation_tx)>4194306 THEN NULL ELSE r.creation_tx END AS "creation_transaction",
r.is_verified AS "verified",
CASE WHEN octet_length(r.contract_name)>2097152 THEN NULL ELSE r.contract_name END AS "name",
CASE WHEN octet_length(r.compiler_version)>2097152 THEN NULL ELSE r.compiler_version END AS "compiler_version",
r.optimization_used AS "optimization_used",
CASE WHEN octet_length(r.evm_version)>2097152 THEN NULL ELSE r.evm_version END AS "evm_version",
CASE WHEN octet_length(r.source_code)>2097152 THEN NULL ELSE r.source_code END AS "source_code",
CASE WHEN octet_length(r.abi::text)>2097152 THEN NULL ELSE r.abi::text END AS "abi_json",
r.created_at AS "created_at",
CASE WHEN octet_length(r.license_type)>2097152 THEN NULL ELSE r.license_type END AS "license_type",
CASE WHEN octet_length(r.constructor_args)>4194306 THEN NULL ELSE r.constructor_args END AS "constructor_arguments",
r.optimization_runs AS "optimization_runs",
(COALESCE((octet_length(r.address)>4194306),false) OR COALESCE((octet_length(r.bytecode)>4194306),false) OR COALESCE((octet_length(r.bytecode_hash)>4194306),false) OR COALESCE((octet_length(r.creator)>4194306),false) OR COALESCE((octet_length(r.creation_tx)>4194306),false) OR COALESCE((octet_length(r.contract_name)>2097152),false) OR COALESCE((octet_length(r.compiler_version)>2097152),false) OR COALESCE((octet_length(r.evm_version)>2097152),false) OR COALESCE((octet_length(r.source_code)>2097152),false) OR COALESCE((octet_length(r.abi::text)>2097152),false) OR COALESCE((octet_length(r.license_type)>2097152),false) OR COALESCE((octet_length(r.constructor_args)>4194306),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.Contract{} }}
	case "tokens":
		return definition{table: `tokens r`, order: []string{`r.block_number`, `r.address COLLATE "C"`}, columns: `
r.block_number AS "_block",
CASE WHEN octet_length(r.address)>4194306 THEN NULL ELSE r.address END AS "_address",
CASE WHEN octet_length(r.symbol)>2097152 THEN NULL ELSE r.symbol END AS "symbol",
CASE WHEN octet_length(r.name)>2097152 THEN NULL ELSE r.name END AS "name",
r.decimals AS "decimals",
CASE WHEN octet_length(r.token_type)>2097152 THEN NULL ELSE r.token_type END AS "token_type",
r.total_supply::text AS "total_supply",
r.holder_count AS "holder_count",
r.transfer_count AS "transfer_count",
r.usd_price::text AS "usd_price",
CASE WHEN octet_length(r.icon_url)>2097152 THEN NULL ELSE r.icon_url END AS "icon_url",
CASE WHEN octet_length(r.l1_address)>4194306 THEN NULL ELSE r.l1_address END AS "l1_address",
CASE WHEN octet_length(r.creation_tx)>4194306 THEN NULL ELSE r.creation_tx END AS "creation_transaction",
r.off_chain_updated_at AS "off_chain_updated_at",
r.created_at AS "created_at",
(COALESCE((octet_length(r.address)>4194306),false) OR COALESCE((octet_length(r.symbol)>2097152),false) OR COALESCE((octet_length(r.name)>2097152),false) OR COALESCE((octet_length(r.token_type)>2097152),false) OR COALESCE((octet_length(r.icon_url)>2097152),false) OR COALESCE((octet_length(r.l1_address)>4194306),false) OR COALESCE((octet_length(r.creation_tx)>4194306),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.Token{} }}
	case "addresses":
		return definition{table: `address_stats r`, order: []string{`r.address COLLATE "C"`}, columns: `
CASE WHEN octet_length(r.address)>4194306 THEN NULL ELSE r.address END AS "_address",
COALESCE((SELECT c.balance::text FROM token_balances_current c WHERE c.address=r.address AND c.token_address='0x0000000000000000000000000000000000000000'),'0') AS balance,
r.tx_count AS "transaction_count",
r.internal_tx_count AS "internal_transaction_count",
r.token_transfer_count AS "token_transfer_count",
r.sent_count AS "sent_count",
r.first_seen AS "first_seen",
r.last_seen AS "last_seen",
r.is_contract AS "is_contract",
r.updated_at AS "updated_at",
(COALESCE((octet_length(r.address)>4194306),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.Address{} }}
	case "balances":
		return definition{table: `token_balances_current r`, order: []string{`r.token_address COLLATE "C"`, `r.address COLLATE "C"`}, columns: `
CASE WHEN octet_length(r.address)>4194306 THEN NULL ELSE r.address END AS "_address",
CASE WHEN octet_length(r.token_address)>4194306 THEN NULL ELSE r.token_address END AS "_token",
r.block_number AS "block_number",
r.balance::text AS "balance",
(COALESCE((octet_length(r.address)>4194306),false) OR COALESCE((octet_length(r.token_address)>4194306),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.Balance{} }}
	case "holders":
		return definition{table: `token_balances_current r`, order: []string{`r.balance`, `r.address COLLATE "C"`}, columns: `
CASE WHEN octet_length(r.address)>4194306 THEN NULL ELSE r.address END AS "_address",
r.balance::text AS "_balance",
CASE WHEN octet_length(r.token_address)>4194306 THEN NULL ELSE r.token_address END AS "token_address",
COALESCE(100.0*r.balance/NULLIF((SELECT sum(c.balance) FROM token_balances_current c WHERE c.token_address=r.token_address AND c.balance>0),0),0)::float8 AS percentage,
EXISTS(SELECT 1 FROM contracts c WHERE c.address=r.address) AS is_contract,
(COALESCE((octet_length(r.address)>4194306),false) OR COALESCE((octet_length(r.token_address)>4194306),false)) AS _oversized
`, fresh: func() proto.Message { return &pb.Holder{} }}
	}
	panic("unknown fixed family")
}
func fmtUint(n uint64) string { return strconv.FormatUint(n, 10) }
func integer(v any) (uint64, error) {
	var n int64
	switch x := v.(type) {
	case int64:
		n = x
	case int32:
		n = int64(x)
	case int16:
		n = int64(x)
	case int:
		n = int64(x)
	default:
		return 0, dataError()
	}
	if n < 0 {
		return 0, dataError()
	}
	return uint64(n), nil
}
func rowFact(rows pgx.Rows, family string) (proto.Message, *pb.Key, error) {
	values, e := rows.Values()
	if e != nil {
		return nil, nil, e
	}
	cols := rows.FieldDescriptions()
	for i, c := range cols {
		if c.Name == "_oversized" {
			if b, ok := values[i].(bool); !ok || b {
				return nil, nil, budget()
			}
		}
	}
	d := define(family)
	m := d.fresh()
	r := m.ProtoReflect()
	keys := map[string]any{}
	for i, col := range cols {
		name := col.Name
		v := values[i]
		if len(name) > 0 && name[0] == '_' {
			keys[name] = v
			continue
		}
		fd := r.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			return nil, nil, dataError()
		}
		if v == nil {
			if fd.Kind() == protoreflect.BytesKind && !fd.HasPresence() {
				return nil, nil, dataError()
			}
			continue
		}
		if e = setFact(r, fd, v); e != nil {
			return nil, nil, e
		}
	}
	key, e := makeKey(family, keys)
	if e != nil {
		return nil, nil, e
	}
	var km proto.Message
	switch x := key.Value.(type) {
	case *pb.Key_Block:
		km = x.Block
	case *pb.Key_Transaction:
		km = x.Transaction
	case *pb.Key_Log:
		km = x.Log
	case *pb.Key_Transfer:
		km = x.Transfer
	case *pb.Key_Internal:
		km = x.Internal
	case *pb.Key_Creation:
		km = x.Creation
	case *pb.Key_Address:
		km = x.Address
	case *pb.Key_Balance:
		km = x.Balance
	case *pb.Key_Holder:
		km = x.Holder
	}
	r.Set(r.Descriptor().Fields().ByName("key"), protoreflect.ValueOfMessage(km.ProtoReflect()))
	return m, key, nil
}
func setFact(r protoreflect.Message, fd protoreflect.FieldDescriptor, v any) error {
	if fd.IsList() {
		ls := r.Mutable(fd).List()
		switch a := v.(type) {
		case []string:
			for _, s := range a {
				ls.Append(protoreflect.ValueOfString(s))
			}
		case []any:
			for _, x := range a {
				s, ok := x.(string)
				if !ok {
					return dataError()
				}
				ls.Append(protoreflect.ValueOfString(s))
			}
		default:
			return dataError()
		}
		return nil
	}
	var pv protoreflect.Value
	switch fd.Kind() {
	case protoreflect.BytesKind:
		str, ok := v.(string)
		if !ok {
			return dataError()
		}
		if fd.Name() == "abi_json" {
			if len(str) > maxField {
				return budget()
			}
			pv = protoreflect.ValueOfBytes([]byte(str))
			break
		}
		if fd.Name() == "nonce" && len(str)%2 == 1 && len(str) > 2 {
			str = "0x0" + str[2:]
		}
		want := 0
		switch fd.Name() {
		case "from", "to", "contract_address", "address", "miner", "token_address", "creator", "l1_address":
			want = 20
		case "hash", "transaction_hash", "parent_hash", "bytecode_hash", "creation_transaction", "topic0", "topic1", "topic2", "topic3":
			want = 32
		case "state_root", "transactions_root", "receipts_root":
			if str != "" {
				want = 32
			}
		}
		b, e := decodeHex(str, want)
		if e != nil {
			return e
		}
		pv = protoreflect.ValueOfBytes(b)
	case protoreflect.StringKind:
		s, ok := v.(string)
		if !ok {
			return dataError()
		}
		if len(s) > maxField {
			return budget()
		}
		switch fd.Name() {
		case "value", "balance", "gas_price", "base_fee_per_gas", "max_fee_per_gas", "max_priority_fee_per_gas", "total_supply", "token_id", "difficulty", "total_difficulty":
			if len(s) > 78 {
				return dataError()
			}
			n, ok := new(big.Int).SetString(s, 10)
			if !ok || n.Sign() < 0 || n.BitLen() > 256 || n.String() != s {
				return dataError()
			}
		case "usd_price":
			if len(s) > 32 {
				return dataError()
			}
			pieces := strings.Split(s, ".")
			if len(pieces) > 2 {
				return dataError()
			}
			for _, piece := range pieces {
				if piece == "" {
					return dataError()
				}
				for _, c := range piece {
					if c < '0' || c > '9' {
						return dataError()
					}
				}
			}
		}
		pv = protoreflect.ValueOfString(s)
	case protoreflect.Uint64Kind, protoreflect.Uint32Kind:
		n, e := integer(v)
		if e != nil || fd.Kind() == protoreflect.Uint32Kind && n > math.MaxUint32 {
			return dataError()
		}
		pv = protoreflect.ValueOfUint64(n)
	case protoreflect.Int64Kind:
		n, e := integer(v)
		if e != nil || n > math.MaxInt64 {
			return dataError()
		}
		pv = protoreflect.ValueOfInt64(int64(n))
	case protoreflect.BoolKind:
		b, ok := v.(bool)
		if !ok {
			return dataError()
		}
		pv = protoreflect.ValueOfBool(b)
	case protoreflect.DoubleKind:
		n, ok := v.(float64)
		if !ok || math.IsNaN(n) || math.IsInf(n, 0) {
			return dataError()
		}
		pv = protoreflect.ValueOfFloat64(n)
	case protoreflect.MessageKind:
		t, ok := v.(time.Time)
		if !ok {
			return dataError()
		}
		p := timestamppb.New(t)
		if p.CheckValid() != nil {
			return dataError()
		}
		pv = protoreflect.ValueOfMessage(p.ProtoReflect())
	default:
		return fmt.Errorf("unsupported fixed fact field")
	}
	if fd.Kind() == protoreflect.Uint32Kind {
		pv = protoreflect.ValueOfUint32(uint32(pv.Uint()))
	}
	r.Set(fd, pv)
	return nil
}
func makeKey(f string, v map[string]any) (*pb.Key, error) {
	number := func(k string) (uint64, error) { return integer(v[k]) }
	raw := func(k string, n int) ([]byte, error) {
		s, ok := v[k].(string)
		if !ok {
			return nil, dataError()
		}
		b, e := decodeHex(s, n)
		if e != nil {
			return nil, e
		}
		if hexString(b) != s {
			return nil, dataError()
		}
		return b, nil
	}
	var b, i uint64
	var e error
	if f == "blocks" {
		b, e = number("_number")
	} else if f == "transactions" || f == "logs" || f == "transfers" || f == "internals" || f == "contracts" || f == "tokens" {
		b, e = number("_block")
	}
	if e != nil {
		return nil, e
	}
	if f == "transactions" || f == "logs" || f == "transfers" {
		i, e = number("_index")
		if e != nil || i > math.MaxUint32 {
			return nil, dataError()
		}
	}
	switch f {
	case "blocks":
		return &pb.Key{Value: &pb.Key_Block{Block: &pb.BlockKey{Number: b}}}, nil
	case "transactions":
		h, e := raw("_hash", 32)
		return &pb.Key{Value: &pb.Key_Transaction{Transaction: &pb.TransactionKey{BlockNumber: b, TransactionIndex: uint32(i), Hash: h}}}, e
	case "logs":
		return &pb.Key{Value: &pb.Key_Log{Log: &pb.LogKey{BlockNumber: b, LogIndex: uint32(i)}}}, nil
	case "transfers":
		return &pb.Key{Value: &pb.Key_Transfer{Transfer: &pb.TransferKey{BlockNumber: b, LogIndex: uint32(i)}}}, nil
	case "internals":
		h, e := raw("_hash", 32)
		s, ok := v["_trace"].(string)
		if !ok || len(s) > maxField {
			return nil, dataError()
		}
		return &pb.Key{Value: &pb.Key_Internal{Internal: &pb.InternalKey{BlockNumber: b, TransactionHash: h, TraceAddress: s}}}, e
	case "contracts", "tokens":
		a, e := raw("_address", 20)
		return &pb.Key{Value: &pb.Key_Creation{Creation: &pb.CreationKey{BlockNumber: b, Address: a}}}, e
	case "addresses":
		a, e := raw("_address", 20)
		return &pb.Key{Value: &pb.Key_Address{Address: &pb.AddressKey{Address: a}}}, e
	case "balances":
		a, e := raw("_address", 20)
		if e != nil {
			return nil, e
		}
		t, e := raw("_token", 20)
		return &pb.Key{Value: &pb.Key_Balance{Balance: &pb.BalanceKey{Address: a, TokenAddress: t}}}, e
	case "holders":
		a, e := raw("_address", 20)
		n, ok := v["_balance"].(string)
		if !ok || !canonicalUint256(n) {
			return nil, dataError()
		}
		return &pb.Key{Value: &pb.Key_Holder{Holder: &pb.HolderKey{Address: a, Balance: n}}}, e
	}
	return nil, dataError()
}

func canonicalUint256(s string) bool {
	if len(s) == 0 || len(s) > 78 {
		return false
	}
	n, ok := new(big.Int).SetString(s, 10)
	return ok && n.Sign() >= 0 && n.BitLen() <= 256 && n.String() == s
}
