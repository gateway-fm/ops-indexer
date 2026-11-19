package indexedread

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gateway-fm/chain-indexer/internal/indexedquery"
	"strconv"
	"strings"
)

type cohortColumn struct {
	name     string
	oid      uint32
	quantity bool
}
type cohortTable struct {
	table   string
	columns []cohortColumn
}

var cohortTables = map[indexedquery.Family]cohortTable{
	indexedquery.Addresses:    {"address_stats", []cohortColumn{{"address", 25, false}, {"first_seen", 20, true}, {"internal_tx_count", 23, true}, {"is_contract", 16, false}, {"last_seen", 20, true}, {"token_transfer_count", 23, true}, {"tx_count", 23, true}, {"updated_at", 1114, false}}},
	indexedquery.Balances:     {"balances", []cohortColumn{{"address", 25, false}, {"balance", 1700, true}, {"block_number", 20, true}, {"token_address", 25, false}}},
	indexedquery.Blocks:       {"blocks", []cohortColumn{{"base_fee_per_gas", 20, true}, {"created_at", 1114, false}, {"difficulty", 25, false}, {"extra_data", 25, false}, {"gas_limit", 20, true}, {"gas_used", 20, true}, {"hash", 25, false}, {"miner", 25, false}, {"nonce", 25, false}, {"number", 20, true}, {"parent_hash", 25, false}, {"receipts_root", 25, false}, {"size", 20, true}, {"state_root", 25, false}, {"timestamp", 20, true}, {"total_difficulty", 25, false}, {"transaction_count", 23, true}, {"transactions_root", 25, false}}},
	indexedquery.Contracts:    {"contracts", []cohortColumn{{"address", 25, false}, {"block_number", 20, true}, {"compiler_version", 25, false}, {"constructor_args", 25, false}, {"contract_name", 25, false}, {"created_at", 1114, false}, {"creation_tx", 25, false}, {"creator", 25, false}, {"evm_version", 25, false}, {"is_verified", 16, false}, {"license_type", 25, false}, {"optimization_runs", 23, false}, {"optimization_used", 16, false}, {"source_code", 25, false}}},
	indexedquery.Progress:     {"indexer_progress", []cohortColumn{{"backfill_complete", 16, false}, {"id", 23, false}, {"max_fetched_block", 20, true}, {"min_fetched_block", 20, true}, {"updated_at", 1114, false}}},
	indexedquery.Internals:    {"internal_transactions", []cohortColumn{{"block_number", 20, true}, {"call_type", 25, false}, {"error", 25, false}, {"from_address", 25, false}, {"gas", 20, true}, {"gas_used", 20, true}, {"id", 23, false}, {"input", 25, false}, {"output", 25, false}, {"timestamp", 20, true}, {"to_address", 25, false}, {"trace_address", 25, false}, {"tx_hash", 25, false}, {"value", 1700, true}}},
	indexedquery.Logs:         {"logs", []cohortColumn{{"address", 25, false}, {"block_number", 20, true}, {"data", 25, false}, {"id", 23, false}, {"log_index", 23, false}, {"removed", 16, false}, {"timestamp", 20, true}, {"topic0", 25, false}, {"topic1", 25, false}, {"topic2", 25, false}, {"topic3", 25, false}, {"tx_hash", 25, false}}},
	indexedquery.Sync:         {"sync_status", []cohortColumn{{"id", 23, false}, {"is_syncing", 16, false}, {"last_finalized_block", 20, true}, {"last_indexed_block", 20, true}, {"last_verified_block", 20, true}, {"updated_at", 1114, false}}},
	indexedquery.Transfers:    {"token_transfers", []cohortColumn{{"block_number", 20, true}, {"from_address", 25, false}, {"id", 23, false}, {"is_internal", 16, false}, {"log_index", 23, false}, {"timestamp", 20, true}, {"to_address", 25, false}, {"token_address", 25, false}, {"token_id", 1700, true}, {"token_type", 25, false}, {"transfer_type", 25, false}, {"tx_hash", 25, false}, {"value", 1700, true}}},
	indexedquery.Tokens:       {"tokens", []cohortColumn{{"address", 25, false}, {"block_number", 20, true}, {"created_at", 1114, false}, {"creation_tx", 25, false}, {"decimals", 23, false}, {"holder_count", 23, true}, {"icon_url", 25, false}, {"l1_address", 25, false}, {"name", 25, false}, {"off_chain_updated_at", 1114, false}, {"symbol", 25, false}, {"token_type", 25, false}, {"total_supply", 1700, true}, {"transfer_count", 23, true}, {"usd_price", 1700, true}}},
	indexedquery.Transactions: {"transactions", []cohortColumn{{"block_number", 20, true}, {"block_timestamp", 20, true}, {"categories", 21, false}, {"created_at", 1114, false}, {"error", 25, false}, {"from_address", 25, false}, {"gas_limit", 20, true}, {"gas_price", 20, true}, {"gas_used", 20, true}, {"hash", 25, false}, {"input_data", 25, false}, {"max_fee_per_gas", 20, true}, {"max_priority_fee_per_gas", 20, true}, {"nonce", 20, true}, {"revert_reason", 25, false}, {"status", 21, false}, {"to_address", 25, false}, {"tx_index", 23, false}, {"tx_type", 21, false}, {"value", 1700, true}}},
}

type cohortState struct {
	pass   *indexedquery.ReadPass
	query  indexedquery.Query
	budget *budget
}

func cohortError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrTooBroad):
		return indexedquery.ErrTooBroad
	case errors.Is(err, ErrInvalid):
		return indexedquery.ErrInvalid
	case errors.Is(err, ErrConflict):
		return indexedquery.ErrConflict
	default:
		return indexedquery.ErrUnavailable
	}
}
func (r *Reader) ReadCohort(ctx context.Context, q indexedquery.Query) (*indexedquery.ReadPass, error) {
	if r == nil {
		return nil, indexedquery.ErrInvalid
	}
	normalized, err := indexedquery.Normalize(q)
	if err != nil {
		return nil, err
	}
	bound, cancel, err := normalized.Bind(ctx)
	if err != nil {
		return nil, indexedquery.ErrUnavailable
	}
	defer cancel()
	b := newBudgetForContext(ctx, r.limits)
	if normalized.Deadline().Before(b.deadline) {
		b.deadline = normalized.Deadline()
	}
	var data indexedquery.Data
	err = r.withSnapshot(bound, b, func(s *snapshot) error { var e error; data, normalized, e = r.readCohort(s, normalized); return e })
	if err != nil {
		return nil, cohortError(err)
	}
	state := &cohortState{query: normalized, budget: b}
	pass, err := indexedquery.NewReadPass(r.metadata, normalized, data, b.charged, state)
	if err != nil {
		return nil, err
	}
	state.pass = pass
	return pass, nil
}
func (r *Reader) Recheck(ctx context.Context, pass *indexedquery.ReadPass) error {
	if r == nil || pass.ValidateFor(r.metadata) != nil {
		return indexedquery.ErrUnavailable
	}
	raw, err := pass.StateFor(r.metadata.Identity)
	if err != nil {
		return err
	}
	state, ok := raw.(*cohortState)
	if !ok || state.pass != pass {
		return indexedquery.ErrUnavailable
	}
	bound, cancel, err := state.query.Bind(ctx)
	if err != nil {
		return indexedquery.ErrUnavailable
	}
	defer cancel()
	var data indexedquery.Data
	err = r.withSnapshot(bound, state.budget, func(s *snapshot) error { var e error; data, _, e = r.readCohort(s, state.query); return e })
	if err != nil {
		return cohortError(err)
	}
	// Reconstruct only within the same borrowed reader/state and original budget.
	next, err := indexedquery.NewReadPass(r.metadata, state.query, data, state.budget.charged, state)
	if err != nil {
		return err
	}
	if next.Digest() != pass.Digest() {
		return indexedquery.ErrConflict
	}
	return nil
}
func (r *Reader) readCohort(s *snapshot, q indexedquery.Query) (indexedquery.Data, indexedquery.Query, error) {
	data := indexedquery.Data{}
	sql := `SELECT number,CASE WHEN octet_length(hash)=66 THEN hash END FROM public.blocks`
	args := []any{}
	if q.Upper != nil {
		sql += ` WHERE number=$1`
		args = append(args, int64(q.Upper.Number))
	}
	sql += ` ORDER BY number DESC LIMIT 1`
	rows, err := s.query(sql, args...)
	if err != nil {
		return data, q, err
	}
	if rows.Next() {
		var n int64
		var hash *string
		if err = rows.Scan(&n, &hash); err != nil || n < 0 || hash == nil || !indexedquery.ValidHash(*hash) {
			rows.Close()
			return data, q, ErrUnavailable
		}
		data.Upper = &indexedquery.Anchor{Number: uint64(n), Hash: *hash}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return data, q, err
	}
	if q.Upper != nil && (data.Upper == nil || *data.Upper != *q.Upper) {
		return data, q, ErrConflict
	}
	if data.Upper != nil {
		copy := *data.Upper
		q.Upper = &copy
	}
	if q.Family == indexedquery.Search {
		data.Rows, err = r.searchRows(s, q)
	} else {
		data.Rows, err = r.cohortRows(s, q, nil)
	}
	if err != nil {
		return data, q, err
	}
	if data.Upper == nil && len(data.Rows) > 0 && q.Family != indexedquery.Sync && q.Family != indexedquery.Progress {
		return data, q, ErrUnavailable
	}
	data.Exhausted = len(data.Rows) <= q.RawLimit
	hashes := []string{}
	seen := map[string]bool{}
	for _, row := range data.Rows {
		var fields map[string]json.RawMessage
		if json.Unmarshal(row.JSON, &fields) != nil {
			return data, q, ErrUnavailable
		}
		key := "tx_hash"
		if row.Family == indexedquery.Transactions {
			key = "hash"
		}
		var hash string
		if json.Unmarshal(fields[key], &hash) == nil && indexedquery.ValidHash(hash) && !seen[hash] {
			hashes = append(hashes, hash)
			seen[hash] = true
		}
	}
	if len(hashes) > 0 {
		for _, family := range []indexedquery.Family{indexedquery.Transactions, indexedquery.Transfers, indexedquery.Internals, indexedquery.Logs} {
			contextQuery := q
			contextQuery.Family = family
			contextQuery.Address = ""
			contextQuery.Token = ""
			contextQuery.Hash = ""
			contextQuery.Topic0 = ""
			contextQuery.Block = nil
			contextQuery.From = nil
			contextQuery.Through = nil
			contextQuery.After = nil
			contextQuery.Search = ""
			found, e := r.cohortRows(s, contextQuery, hashes)
			if e != nil {
				return data, q, e
			}
			data.Context = append(data.Context, found...)
		}
	}
	if q.Family == indexedquery.Sync {
		contextQuery := q
		contextQuery.Family = indexedquery.Progress
		found, e := r.cohortRows(s, contextQuery, nil)
		if e != nil {
			return data, q, e
		}
		data.Context = append(data.Context, found...)
	}
	return data, q, nil
}

func (r *Reader) cohortRows(s *snapshot, q indexedquery.Query, hashes []string) ([]indexedquery.Record, error) {
	family := q.Family
	if family == indexedquery.Holders {
		family = indexedquery.Balances
	}
	spec, ok := cohortTables[family]
	if !ok {
		return nil, ErrInvalid
	}
	// Every variable field is bounded before transport. An oversized field is
	// signaled separately, never silently replaced by a legitimate NULL.
	fields := []string{}
	bad := []string{}
	for _, c := range spec.columns {
		expression := `r."` + c.name + `"`
		text := expression + `::text`
		if c.name == "block_timestamp" && family == indexedquery.Transactions {
			expression = `COALESCE(r.block_timestamp,b.timestamp)`
			text = expression + `::text`
		}
		bad = append(bad, `COALESCE(octet_length(`+text+`)>`+strconv.Itoa(r.limits.MaxFieldBytes)+`,false)`)
		guarded := `CASE WHEN octet_length(` + text + `)>` + strconv.Itoa(r.limits.MaxFieldBytes) + ` THEN NULL ELSE ` + expression + ` END`
		if c.quantity {
			guarded = `CASE WHEN octet_length(` + text + `)>` + strconv.Itoa(r.limits.MaxFieldBytes) + ` THEN NULL ELSE ` + text + ` END`
		}
		fields = append(fields, `'`+c.name+`',`+guarded)
	}
	if family == indexedquery.Transactions {
		fields = append(fields, `'block_hash',CASE WHEN octet_length(b.hash)=66 THEN b.hash END`)
		bad = append(bad, `COALESCE(octet_length(b.hash)>`+strconv.Itoa(r.limits.MaxFieldBytes)+`,false)`)
	}
	fields = append(fields, `'__too_broad',(`+strings.Join(bad, ` OR `)+`)`)
	object := `jsonb_build_object(` + strings.Join(fields, `, `) + `)`
	from := `public.` + spec.table + ` r`
	args := []any{}
	where := []string{`true`}
	bind := func(value any) string { args = append(args, value); return "$" + strconv.Itoa(len(args)) }
	block := "r.block_number"
	if family == indexedquery.Blocks {
		block = "r.number"
	}
	if family == indexedquery.Transactions {
		from += ` JOIN public.blocks b ON b.number=r.block_number`
	}
	if family == indexedquery.Balances {
		upper := int64(1<<63 - 1)
		if q.Upper != nil {
			upper = int64(q.Upper.Number)
		}
		from = `(SELECT *,row_number()OVER(PARTITION BY address,token_address ORDER BY block_number DESC)AS latest FROM public.balances WHERE block_number<=` + bind(upper) + `)r`
		where = append(where, `r.latest=1`, `r.balance>0`)
	}
	hasBlock := family != indexedquery.Addresses && family != indexedquery.Sync && family != indexedquery.Progress
	if q.Upper != nil && hasBlock {
		where = append(where, block+`<=`+bind(int64(q.Upper.Number)))
	}
	if q.Block != nil && hasBlock {
		where = append(where, block+`=`+bind(int64(*q.Block)))
	}
	if q.From != nil && hasBlock {
		where = append(where, block+`>=`+bind(int64(*q.From)))
	}
	if q.Through != nil && hasBlock {
		where = append(where, block+`<=`+bind(int64(*q.Through)))
	}
	hashColumn := "r.tx_hash"
	if family == indexedquery.Transactions || family == indexedquery.Blocks {
		hashColumn = "r.hash"
	}
	if len(hashes) > 0 {
		where = append(where, hashColumn+`=ANY(`+bind(hashes)+`::text[])`)
	} else {
		if q.Hash != "" {
			where = append(where, hashColumn+`=`+bind(q.Hash))
		}
		if q.Address != "" {
			p := bind(q.Address)
			switch family {
			case indexedquery.Transactions, indexedquery.Transfers, indexedquery.Internals:
				where = append(where, `(r.from_address=`+p+` OR r.to_address=`+p+`)`)
			default:
				where = append(where, `r.address=`+p)
			}
		}
		if q.Token != "" {
			col := "r.token_address"
			if family == indexedquery.Tokens {
				col = "r.address"
			}
			where = append(where, col+`=`+bind(q.Token))
		}
		if q.Topic0 != "" {
			where = append(where, `r.topic0=`+bind(q.Topic0))
		}
		if q.Search != "" {
			p := bind(strings.ToLower(q.Search))
			col := "r.address"
			if family == indexedquery.Transactions || family == indexedquery.Blocks {
				col = "r.hash"
			}
			where = append(where, `strpos(lower(`+col+`),`+p+`)>0`)
		}
	}
	order := ""
	tuple := ""
	values := []any{}
	switch family {
	case indexedquery.Transactions:
		order = `r.block_number DESC,r.tx_index DESC,r.hash COLLATE "C" DESC`
		tuple = `(r.block_number,r.tx_index,r.hash COLLATE "C")`
		if q.After != nil {
			values = []any{int64(q.After.Block), int64(q.After.Index), q.After.Hash}
		}
	case indexedquery.Blocks:
		order = `r.number DESC,r.hash COLLATE "C" DESC`
		tuple = `(r.number,r.hash COLLATE "C")`
		if q.After != nil {
			values = []any{int64(q.After.Block), q.After.Hash}
		}
	case indexedquery.Transfers, indexedquery.Logs:
		order = `r.block_number DESC,r.tx_hash COLLATE "C" DESC,r.log_index DESC`
		tuple = `(r.block_number,r.tx_hash COLLATE "C",r.log_index)`
		if q.After != nil {
			values = []any{int64(q.After.Block), q.After.Hash, int64(q.After.Index)}
		}
	case indexedquery.Internals:
		order = `r.block_number DESC,r.tx_hash COLLATE "C" DESC,r.trace_address COLLATE "C" DESC`
		tuple = `(r.block_number,r.tx_hash COLLATE "C",r.trace_address COLLATE "C")`
		if q.After != nil {
			values = []any{int64(q.After.Block), q.After.Hash, q.After.Subkey}
		}
	case indexedquery.Tokens, indexedquery.Contracts:
		order = `r.block_number DESC,r.address COLLATE "C" DESC`
		tuple = `(r.block_number,r.address COLLATE "C")`
		if q.After != nil {
			values = []any{int64(q.After.Block), q.After.Hash}
		}
	case indexedquery.Balances:
		if q.Family == indexedquery.Holders {
			order = `r.balance DESC,r.address COLLATE "C" DESC`
			tuple = `(r.balance,r.address COLLATE "C")`
			if q.After != nil {
				values = []any{q.After.Amount, q.After.Hash}
			}
		} else {
			order = `r.token_address COLLATE "C" DESC,r.address COLLATE "C" DESC`
			tuple = `(r.token_address COLLATE "C",r.address COLLATE "C")`
			if q.After != nil {
				values = []any{q.After.Hash, q.After.Subkey}
			}
		}
	case indexedquery.Addresses:
		order = `r.address COLLATE "C" DESC`
		tuple = `(r.address COLLATE "C")`
		if q.After != nil {
			values = []any{q.After.Hash}
		}
	case indexedquery.Sync, indexedquery.Progress:
		order = `r.id DESC`
	}
	if len(hashes) == 0 && len(values) > 0 {
		params := []string{}
		for _, v := range values {
			param := bind(v)
			if q.Family == indexedquery.Holders && len(params) == 0 {
				param += `::numeric`
			}
			params = append(params, param)
		}
		where = append(where, tuple+`<(`+strings.Join(params, `, `)+`)`)
	}
	limit := q.RawLimit + 1
	if len(hashes) > 0 {
		limit = r.limits.MaxResultRows + 1
	}
	sql := `SELECT CASE WHEN octet_length(payload::text)<=` + strconv.Itoa(r.limits.MaxFieldBytes) + ` THEN payload::text END FROM (SELECT ` + object + ` AS payload FROM ` + from + ` WHERE ` + strings.Join(where, ` AND `) + ` ORDER BY ` + order + ` LIMIT ` + bind(limit) + `) bounded`
	rows, err := s.query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []indexedquery.Record{}
	for rows.Next() {
		var raw *string
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, ErrTooBroad
		}
		if err = s.b.retain(2 * len(*raw)); err != nil {
			return nil, err
		}
		row, e := cohortRecord(q.Family, []byte(*raw))
		if e != nil {
			return nil, e
		}
		out = append(out, row)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}
func cohortRecord(f indexedquery.Family, raw []byte) (indexedquery.Record, error) {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return indexedquery.Record{}, ErrUnavailable
	}
	var bad bool
	if json.Unmarshal(m["__too_broad"], &bad) != nil || bad {
		return indexedquery.Record{}, ErrTooBroad
	}
	delete(m, "__too_broad")
	str := func(key string) string { var value string; _ = json.Unmarshal(m[key], &value); return value }
	number := func(key string) uint64 {
		value := str(key)
		if value == "" {
			value = string(m[key])
		}
		n, _ := strconv.ParseUint(value, 10, 64)
		return n
	}
	pos := indexedquery.Position{Block: number("block_number")}
	key := ""
	switch f {
	case indexedquery.Blocks:
		pos.Block = number("number")
		pos.Hash = str("hash")
		key = pos.Hash
	case indexedquery.Transactions:
		pos.Hash = str("hash")
		pos.Index = uint32(number("tx_index"))
		key = pos.Hash
	case indexedquery.Transfers, indexedquery.Logs:
		pos.Hash = str("tx_hash")
		pos.Index = uint32(number("log_index"))
		key = pos.Hash + ":" + strconv.FormatUint(uint64(pos.Index), 10)
	case indexedquery.Internals:
		pos.Hash = str("tx_hash")
		pos.Subkey = str("trace_address")
		key = pos.Hash + ":" + pos.Subkey
	case indexedquery.Holders:
		pos.Hash = str("address")
		pos.Amount = str("balance")
		key = pos.Hash
	case indexedquery.Balances:
		pos.Hash = str("token_address")
		pos.Subkey = str("address")
		key = pos.Hash + ":" + pos.Subkey
	default:
		pos.Hash = str("address")
		key = pos.Hash
		if f == indexedquery.Sync || f == indexedquery.Progress {
			key = string(m["id"])
		}
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return indexedquery.Record{}, ErrUnavailable
	}
	return indexedquery.Record{Family: f, Key: key, Position: pos, JSON: encoded}, nil
}

func (r *Reader) searchRows(s *snapshot, q indexedquery.Query) ([]indexedquery.Record, error) {
	upper := int64(1<<63 - 1)
	if q.Upper != nil {
		upper = int64(q.Upper.Number)
	}
	afterKind, afterValue := "", ""
	if q.After != nil {
		afterKind = q.After.Subkey
		afterValue = q.After.Hash
	}
	sql := `WITH candidates AS (
 SELECT 'transaction'::text kind,hash::text value,hash::text tx_hash FROM public.transactions WHERE block_number<=$2 AND strpos(lower(hash),$1)>0
 UNION ALL SELECT 'address',address,NULL FROM public.address_stats WHERE strpos(lower(address),$1)>0
 UNION ALL SELECT 'token',address,NULL FROM public.tokens WHERE block_number<=$2 AND (strpos(lower(address),$1)>0 OR strpos(lower(symbol),$1)>0 OR strpos(lower(name),$1)>0)
 UNION ALL SELECT 'contract',address,NULL FROM public.contracts WHERE block_number<=$2 AND strpos(lower(address),$1)>0
 )SELECT kind,CASE WHEN octet_length(value)<=66 THEN value END,CASE WHEN octet_length(tx_hash)<=66 THEN tx_hash END FROM candidates WHERE ($3='' OR (kind COLLATE "C",value COLLATE "C")<($3,$4)) ORDER BY kind COLLATE "C" DESC,value COLLATE "C" DESC LIMIT $5`
	rows, e := s.query(sql, strings.ToLower(q.Search), upper, afterKind, afterValue, q.RawLimit+1)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []indexedquery.Record{}
	for rows.Next() {
		var kind string
		var value, hash *string
		if e = rows.Scan(&kind, &value, &hash); e != nil || value == nil {
			return nil, ErrUnavailable
		}
		if kind == "transaction" && !indexedquery.ValidHash(*value) || kind != "transaction" && !indexedquery.ValidAddress(*value) {
			return nil, ErrUnavailable
		}
		obj := map[string]any{"kind": kind, "value": *value}
		if hash != nil {
			obj["tx_hash"] = *hash
		}
		raw, e := json.Marshal(obj)
		if e != nil {
			return nil, e
		}
		if e = s.b.retain(4 * len(raw)); e != nil {
			return nil, e
		}
		out = append(out, indexedquery.Record{Family: indexedquery.Search, Key: kind + ":" + *value, Position: indexedquery.Position{Hash: *value, Subkey: kind}, JSON: raw})
	}
	return out, rows.Err()
}
