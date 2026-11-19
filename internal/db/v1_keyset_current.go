package db

import (
	"context"
	"fmt"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/jackc/pgx/v5"
	"time"
)

func (d *DB) V1Tokens(ctx context.Context, tokenType, search string, p V1PageInput) (V1PageResult[types.Token], error) {
	q := v1Query[types.Token]{columns: `address,symbol,name,decimals,token_type,total_supply,holder_count,transfer_count,usd_price,icon_url,l1_address,block_number,creation_tx,off_chain_updated_at,created_at`, from: "tokens", order: []v1Order{v1Desc("holder_count", "bigint"), v1Asc("address", "text")}, key: func(t types.Token) []string { return []string{v1I(t.HolderCount), t.Address} }, scan: func(rows pgx.Rows) ([]types.Token, error) {
		var out []types.Token
		for rows.Next() {
			var t types.Token
			if err := rows.Scan(&t.Address, &t.Symbol, &t.Name, &t.Decimals, &t.TokenType, &t.TotalSupply, &t.HolderCount, &t.TransferCount, &t.USDPrice, &t.IconURL, &t.L1Address, &t.BlockNumber, &t.CreationTx, &t.OffChainUpdatedAt, &t.CreatedAt); err != nil {
				return nil, err
			}
			out = append(out, t)
		}
		return out, rows.Err()
	}}
	if tokenType != "" {
		q.add("token_type=$%d", tokenType)
	}
	if search != "" {
		q.args = append(q.args, "%"+search+"%")
		n := len(q.args)
		q.conditions = append(q.conditions, fmt.Sprintf("(LOWER(name) LIKE $%d OR LOWER(symbol) LIKE $%d OR address LIKE $%d)", n, n, n))
	}
	return v1Read(ctx, d, q, p)
}
func (d *DB) V1Addresses(ctx context.Context, p V1PageInput) (V1PageResult[types.AddressStats], error) {
	q := v1Query[types.AddressStats]{columns: `address,tx_count,internal_tx_count,token_transfer_count,sent_count,first_seen,last_seen,is_contract,updated_at`, from: "address_stats", order: []v1Order{v1Desc("tx_count", "bigint"), v1Asc("address", "text")}, key: func(t types.AddressStats) []string { return []string{v1I(t.TxCount), t.Address} }, scan: func(rows pgx.Rows) ([]types.AddressStats, error) {
		var out []types.AddressStats
		for rows.Next() {
			var t types.AddressStats
			if err := rows.Scan(&t.Address, &t.TxCount, &t.InternalTxCount, &t.TokenTransferCount, &t.SentCount, &t.FirstSeen, &t.LastSeen, &t.IsContract, &t.UpdatedAt); err != nil {
				return nil, err
			}
			out = append(out, t)
		}
		return out, rows.Err()
	}}
	return v1Read(ctx, d, q, p)
}
func (d *DB) V1Holders(ctx context.Context, token string, p V1PageInput) (V1PageResult[types.TokenHolder], error) {
	q := v1Query[types.TokenHolder]{columns: `c.address,c.balance::text,(c.balance/NULLIF((SELECT SUM(balance) FROM token_balances_current WHERE token_address=c.token_address AND balance>0),0)*100)::numeric(10,4),EXISTS(SELECT 1 FROM contracts WHERE address=c.address)`, from: "token_balances_current c", conditions: []string{"c.balance>0"}, order: []v1Order{v1Desc("c.balance", "numeric"), v1Asc("c.address", "text")}, key: func(t types.TokenHolder) []string { return []string{string(t.Balance), t.Address} }, scan: func(rows pgx.Rows) ([]types.TokenHolder, error) {
		var out []types.TokenHolder
		for rows.Next() {
			var t types.TokenHolder
			if err := rows.Scan(&t.Address, &t.Balance, &t.Percentage, &t.IsContract); err != nil {
				return nil, err
			}
			out = append(out, t)
		}
		return out, rows.Err()
	}}
	q.add("c.token_address=$%d", token)
	q.total = `SELECT COALESCE((SELECT holder_count FROM tokens WHERE address=$1),(SELECT COUNT(*) FROM token_balances_current WHERE token_address=$1 AND balance>0))`
	q.totalArgs = []any{token}
	return v1Read(ctx, d, q, p)
}
func (d *DB) V1Inventory(ctx context.Context, token, id string, p V1PageInput) (V1PageResult[types.TokenInventoryItem], error) {
	q := v1Query[types.TokenInventoryItem]{columns: `n.token_id::text,n.owner,n.token_uri`, from: "nft_tokens n", conditions: []string{"owner<>'0x0000000000000000000000000000000000000000'"}, order: []v1Order{v1Asc("n.token_id", "numeric")}, key: func(t types.TokenInventoryItem) []string { return []string{t.TokenID} }, scan: func(rows pgx.Rows) ([]types.TokenInventoryItem, error) {
		var out []types.TokenInventoryItem
		for rows.Next() {
			var t types.TokenInventoryItem
			if err := rows.Scan(&t.TokenID, &t.Owner, &t.TokenURI); err != nil {
				return nil, err
			}
			out = append(out, t)
		}
		return out, rows.Err()
	}}
	q.add("LOWER(n.token_address)=$%d", token)
	if id != "" {
		q.add("token_id=$%d::numeric", id)
	}
	return v1Read(ctx, d, q, p)
}
func (d *DB) V1Balances(ctx context.Context, address, token string, p V1PageInput) (V1PageResult[types.Balance], error) {
	q := v1Query[types.Balance]{columns: `address,token_address,block_number,balance::text`, from: "token_balances_current", order: []v1Order{v1Asc("token_address", "text")}, key: func(t types.Balance) []string { return []string{t.TokenAddress} }, scan: func(rows pgx.Rows) ([]types.Balance, error) {
		var out []types.Balance
		for rows.Next() {
			var t types.Balance
			if err := rows.Scan(&t.Address, &t.TokenAddress, &t.BlockNumber, &t.Balance); err != nil {
				return nil, err
			}
			out = append(out, t)
		}
		return out, rows.Err()
	}}
	q.skipTotal = true
	q.add("address=$%d", address)
	if token != "" {
		q.add("token_address=$%d", token)
	}
	return v1Read(ctx, d, q, p)
}
func (d *DB) V1Deposits(ctx context.Context, from string, lower, upper uint64, p V1PageInput) (V1PageResult[types.OPDeposit], error) {
	q := v1Query[types.OPDeposit]{columns: `l2_tx_hash,l1_block_number,l1_block_timestamp,l1_tx_hash,l1_tx_origin,created_at`, from: "op_deposits", order: []v1Order{v1Desc("l1_block_number", "bigint"), v1Desc("l2_tx_hash", "text")}, key: func(t types.OPDeposit) []string { return []string{v1U(t.L1BlockNumber), t.L2TxHash} }, scan: func(rows pgx.Rows) ([]types.OPDeposit, error) {
		var out []types.OPDeposit
		for rows.Next() {
			var t types.OPDeposit
			if err := rows.Scan(&t.L2TxHash, &t.L1BlockNumber, &t.L1BlockTimestamp, &t.L1TxHash, &t.L1TxOrigin, &t.CreatedAt); err != nil {
				return nil, err
			}
			out = append(out, t)
		}
		return out, rows.Err()
	}}
	q.skipTotal = true
	if from != "" {
		q.add("l1_tx_origin=$%d", from)
	}
	if lower > 0 {
		q.add("l1_block_number>=$%d", lower)
	}
	if upper > 0 {
		q.add("l1_block_number<$%d", upper)
	}
	return v1Read(ctx, d, q, p)
}
func (d *DB) V1VerifiedContracts(ctx context.Context, p V1PageInput) (V1PageResult[types.Contract], error) {
	q := v1Query[types.Contract]{columns: `address,bytecode,bytecode_hash,creator,creation_tx,block_number,is_verified,contract_name,compiler_version,optimization_used,evm_version,source_code,abi,created_at`, from: "contracts", conditions: []string{"is_verified=true"}, order: []v1Order{v1Desc("created_at", "timestamptz"), v1Asc("address", "text")}, key: func(t types.Contract) []string { return []string{t.CreatedAt.Format(time.RFC3339Nano), t.Address} }, scan: func(rows pgx.Rows) ([]types.Contract, error) {
		var out []types.Contract
		for rows.Next() {
			var t types.Contract
			if err := rows.Scan(&t.Address, &t.Bytecode, &t.BytecodeHash, &t.Creator, &t.CreationTx, &t.BlockNumber, &t.IsVerified, &t.ContractName, &t.CompilerVersion, &t.OptimizationUsed, &t.EVMVersion, &t.SourceCode, &t.ABI, &t.CreatedAt); err != nil {
				return nil, err
			}
			out = append(out, t)
		}
		return out, rows.Err()
	}}
	return v1Read(ctx, d, q, p)
}
