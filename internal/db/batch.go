package db

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/jackc/pgx/v5"
)

// workMemPattern matches a Postgres memory size (digits + optional unit) so the
// value can be safely interpolated into SET LOCAL, which cannot be parameterized.
var workMemPattern = regexp.MustCompile(`(?i)^[0-9]+(kb|mb|gb|tb)?$`)

func sanitizeWorkMem(v string) string {
	v = strings.TrimSpace(v)
	if workMemPattern.MatchString(v) {
		return v
	}
	return "1GB"
}

type BlockData struct {
	Block                *types.Block
	Transactions         []*types.Transaction
	Logs                 []*types.Log
	Transfers            []*types.TokenTransfer
	Contracts            []*types.Contract
	Tokens               []*types.Token
	NFTTokens            []*types.NFTToken
	InternalTransactions []*types.InternalTransaction
	AddressStats         map[string]*AddressStatsDelta
	SkipAddressStats     bool // Skip address_stats updates (for catchup mode to avoid deadlocks)
}

type AddressStatsDelta struct {
	Address              string
	TxCountDelta         int
	InternalTxCountDelta int
	TokenTransferDelta   int
	IsContract           bool
	BlockNumber          uint64
}

func (d *DB) InsertBlockDataBatch(ctx context.Context, data *BlockData) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var deltaBlocks, deltaTxs, deltaTokens, deltaAddresses, deltaTransfers int64

	if data.Block != nil {
		ct, err := tx.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, transaction_count,
				size, difficulty, total_difficulty, nonce, miner, extra_data, state_root, transactions_root, receipts_root)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			ON CONFLICT (number) DO NOTHING`,
			data.Block.Number, data.Block.Hash, data.Block.ParentHash, data.Block.Timestamp,
			data.Block.GasUsed, data.Block.GasLimit, data.Block.BaseFeePerGas, data.Block.TransactionCount,
			data.Block.Size, data.Block.Difficulty, data.Block.TotalDifficulty, data.Block.Nonce,
			data.Block.Miner, data.Block.ExtraData, data.Block.StateRoot, data.Block.TransactionsRoot, data.Block.ReceiptsRoot)
		if err != nil {
			return err
		}
		deltaBlocks += ct.RowsAffected()
	}

	if len(data.Transactions) > 0 {
		// Precompute the set of tx hashes that have token transfers in this
		// batch so the category bitfield can be set atomically on the insert.
		txsWithTransfers := buildTokenTransferTxSet(data.Transfers)
		var blockTimestamp *uint64
		if data.Block != nil {
			ts := data.Block.Timestamp
			blockTimestamp = &ts
		}
		batch := &pgx.Batch{}
		for _, t := range data.Transactions {
			_, hasTransfer := txsWithTransfers[t.Hash]
			categories := computeTxCategories(t, hasTransfer)
			batch.Queue(`
				INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price,
					gas_limit, max_fee_per_gas, max_priority_fee_per_gas, nonce, tx_type, input_data, status, error, revert_reason, categories)
				VALUES ($1, $2, $3, $4, LOWER($5), LOWER($6), $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
				ON CONFLICT (hash) DO NOTHING`,
				t.Hash, t.BlockNumber, blockTimestamp, t.TxIndex, t.From, t.To, t.Value, t.GasUsed, t.GasPrice,
				t.GasLimit, t.MaxFeePerGas, t.MaxPriorityFeePerGas, t.Nonce, t.TxType, t.InputData, t.Status, t.Error, t.RevertReason, categories)
		}
		br := tx.SendBatch(ctx, batch)
		for range data.Transactions {
			ct, err := br.Exec()
			if err != nil {
				_ = br.Close()
				return err
			}
			deltaTxs += ct.RowsAffected()
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.Contracts) > 0 {
		batch := &pgx.Batch{}
		for _, c := range data.Contracts {
			batch.Queue(`
				INSERT INTO contracts (address, bytecode, bytecode_hash, creator, creation_tx, block_number, is_verified,
					contract_name, compiler_version, optimization_used, source_code, abi)
				VALUES (LOWER($1), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (address) DO NOTHING`,
				c.Address, c.Bytecode, c.BytecodeHash, c.Creator, c.CreationTx, c.BlockNumber, c.IsVerified,
				c.ContractName, c.CompilerVersion, c.OptimizationUsed, c.SourceCode, c.ABI)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.Tokens) > 0 {
		batch := &pgx.Batch{}
		for _, t := range data.Tokens {
			batch.Queue(`
				INSERT INTO tokens (address, symbol, name, decimals, token_type, total_supply, block_number, creation_tx, l1_address)
				VALUES (LOWER($1), $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (address) DO UPDATE SET
					symbol = COALESCE(EXCLUDED.symbol, tokens.symbol),
					name = COALESCE(EXCLUDED.name, tokens.name),
					decimals = COALESCE(EXCLUDED.decimals, tokens.decimals),
					-- Promote a previously misclassified row (e.g. an ERC721 first
					-- seen before this fix, stored as the ERC20 default) when a
					-- more specific type is observed, but never downgrade.
					token_type = CASE WHEN EXCLUDED.token_type <> 'ERC20' THEN EXCLUDED.token_type ELSE tokens.token_type END,
					total_supply = COALESCE(EXCLUDED.total_supply, tokens.total_supply)
				RETURNING (xmax = 0)`,
				t.Address, t.Symbol, t.Name, t.Decimals, t.TokenType, t.TotalSupply, t.BlockNumber, t.CreationTx, t.L1Address)
		}
		br := tx.SendBatch(ctx, batch)
		for range data.Tokens {
			var wasNew bool
			if err := br.QueryRow().Scan(&wasNew); err != nil {
				_ = br.Close()
				return err
			}
			if wasNew {
				deltaTokens++
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.NFTTokens) > 0 {
		batch := &pgx.Batch{}
		for _, n := range data.NFTTokens {
			batch.Queue(`
				INSERT INTO nft_tokens (token_address, token_id, owner, token_uri, block_number)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (token_address, token_id) DO UPDATE SET
					owner = EXCLUDED.owner,
					-- tokenURI is captured once at mint; a later transfer carries
					-- no URI, so keep the existing value rather than nulling it.
					token_uri = COALESCE(nft_tokens.token_uri, EXCLUDED.token_uri),
					block_number = EXCLUDED.block_number,
					updated_at = NOW()`,
				n.TokenAddress, n.TokenID, n.Owner, n.TokenURI, n.BlockNumber)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.Logs) > 0 {
		batch := &pgx.Batch{}
		for _, l := range data.Logs {
			batch.Queue(`
				INSERT INTO logs (tx_hash, log_index, address, topic0, topic1, topic2, topic3, data, block_number, timestamp, removed)
				VALUES ($1, $2, LOWER($3), $4, $5, $6, $7, $8, $9, $10, $11)
				ON CONFLICT (tx_hash, log_index) DO NOTHING`,
				l.TxHash, l.LogIndex, l.Address, l.Topic0, l.Topic1, l.Topic2, l.Topic3, l.Data, l.BlockNumber, l.Timestamp, l.Removed)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.Transfers) > 0 {
		batch := &pgx.Batch{}
		for _, t := range data.Transfers {
			batch.Queue(`
				INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value,
					block_number, timestamp, transfer_type, token_type, token_id, is_internal)
				VALUES ($1, $2, LOWER($3), LOWER($4), LOWER($5), $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (tx_hash, log_index) DO NOTHING`,
				t.TxHash, t.LogIndex, t.TokenAddress, t.From, t.To, t.Value,
				t.BlockNumber, t.Timestamp, t.TransferType, t.TokenType, t.TokenID, t.IsInternal)
		}
		br := tx.SendBatch(ctx, batch)
		for range data.Transfers {
			ct, err := br.Exec()
			if err != nil {
				_ = br.Close()
				return err
			}
			// ON CONFLICT DO NOTHING ⇒ only genuinely new rows count toward the total.
			deltaTransfers += ct.RowsAffected()
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.InternalTransactions) > 0 {
		batch := &pgx.Batch{}
		for _, it := range data.InternalTransactions {
			batch.Queue(`
				INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value,
					gas, gas_used, input, output, call_type, error, timestamp)
				VALUES ($1, $2, $3, LOWER($4), LOWER($5), $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (tx_hash, trace_address) DO NOTHING`,
				it.TxHash, it.BlockNumber, it.TraceAddress, it.From, it.To, it.Value,
				it.Gas, it.GasUsed, it.Input, it.Output, it.CallType, it.Error, it.Timestamp)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.AddressStats) > 0 && !data.SkipAddressStats {
		batch := &pgx.Batch{}
		for _, s := range data.AddressStats {
			batch.Queue(`
				INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, updated_at)
				VALUES (LOWER($1), $2, $3, $4, $5, $5, $6, NOW())
				ON CONFLICT (address) DO UPDATE SET
					tx_count = address_stats.tx_count + $2,
					internal_tx_count = address_stats.internal_tx_count + $3,
					token_transfer_count = address_stats.token_transfer_count + $4,
					last_seen = $5,
					is_contract = COALESCE(EXCLUDED.is_contract, address_stats.is_contract),
					updated_at = NOW()
				RETURNING (xmax = 0)`,
				s.Address, s.TxCountDelta, s.InternalTxCountDelta, s.TokenTransferDelta, s.BlockNumber, s.IsContract)
		}
		br := tx.SendBatch(ctx, batch)
		for range data.AddressStats {
			var wasNew bool
			if err := br.QueryRow().Scan(&wasNew); err != nil {
				_ = br.Close()
				return err
			}
			if wasNew {
				deltaAddresses++
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	if deltaBlocks|deltaTxs|deltaTokens|deltaAddresses|deltaTransfers != 0 {
		_, err := tx.Exec(ctx, `
			INSERT INTO chain_counters (name, count, updated_at) VALUES
				('blocks_total',       $1, NOW()),
				('transactions_total', $2, NOW()),
				('tokens_total',       $3, NOW()),
				('addresses_total',    $4, NOW()),
				('transfers_total',    $5, NOW())
			ON CONFLICT (name) DO UPDATE
				SET count = chain_counters.count + EXCLUDED.count,
					updated_at = NOW()`,
			deltaBlocks, deltaTxs, deltaTokens, deltaAddresses, deltaTransfers)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// RebuildAddressStats rebuilds address_stats after catchup completes.
func (d *DB) RebuildAddressStats(ctx context.Context) error {
	// All address keys are lowercased so duplicate-case rows from past
	// writes (some checksum-cased, some lowercase) collapse to one row per
	// address. Ongoing delta writes already lowercase the key, so this
	// stays consistent going forward.
	//
	// tx_count is the count of distinct tx hashes involving the address
	// either as the EVM from/to OR as a Transfer-event participant in that
	// tx's logs. This matches the inclusive list returned by
	// GetTransactionsByAddress — a recipient who never sent a tx of their
	// own still gets credit for the mint/transfer txs that gave them tokens.
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL work_mem = '%s'", sanitizeWorkMem(d.RebuildWorkMem))); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		TRUNCATE address_stats;

		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, updated_at)
		SELECT
			addr,
			COALESCE(tx_count, 0),
			COALESCE(internal_tx_count, 0),
			COALESCE(token_transfer_count, 0),
			first_seen,
			last_seen,
			COALESCE(is_contract, false),
			NOW()
		FROM (
			SELECT
				a.address as addr,
				tx.tx_count,
				it.internal_tx_count,
				tt.token_transfer_count,
				LEAST(tx.first_seen, it.first_seen, tt.first_seen) as first_seen,
				GREATEST(tx.last_seen, it.last_seen, tt.last_seen) as last_seen,
				c.is_contract
			FROM (
				SELECT LOWER(from_address) as address FROM transactions
				UNION
				SELECT LOWER(to_address) as address FROM transactions WHERE to_address IS NOT NULL
				UNION
				SELECT LOWER(from_address) as address FROM internal_transactions
				UNION
				SELECT LOWER(to_address) as address FROM internal_transactions WHERE to_address IS NOT NULL
				UNION
				SELECT LOWER(from_address) as address FROM token_transfers
				UNION
				SELECT LOWER(to_address) as address FROM token_transfers
			) a
			LEFT JOIN (
				SELECT address, COUNT(DISTINCT tx_hash) as tx_count, MIN(block_number) as first_seen, MAX(block_number) as last_seen
				FROM (
					SELECT LOWER(from_address) as address, hash as tx_hash, block_number FROM transactions
					UNION ALL
					SELECT LOWER(to_address) as address, hash as tx_hash, block_number FROM transactions WHERE to_address IS NOT NULL
					UNION ALL
					SELECT LOWER(tt.from_address) as address, tt.tx_hash, tt.block_number FROM token_transfers tt
					UNION ALL
					SELECT LOWER(tt.to_address) as address, tt.tx_hash, tt.block_number FROM token_transfers tt
				) t
				GROUP BY address
			) tx ON a.address = tx.address
			LEFT JOIN (
				SELECT address, COUNT(*) as internal_tx_count, MIN(block_number) as first_seen, MAX(block_number) as last_seen
				FROM (
					SELECT LOWER(from_address) as address, block_number FROM internal_transactions
					UNION ALL
					SELECT LOWER(to_address) as address, block_number FROM internal_transactions WHERE to_address IS NOT NULL
				) t
				GROUP BY address
			) it ON a.address = it.address
			LEFT JOIN (
				SELECT address, COUNT(*) as token_transfer_count, MIN(block_number) as first_seen, MAX(block_number) as last_seen
				FROM (
					SELECT LOWER(from_address) as address, block_number FROM token_transfers
					UNION ALL
					SELECT LOWER(to_address) as address, block_number FROM token_transfers
				) t
				GROUP BY address
			) tt ON a.address = tt.address
			LEFT JOIN (
				SELECT LOWER(address) as address, true as is_contract FROM contracts
			) c ON a.address = c.address
		) stats
		WHERE addr IS NOT NULL
	`); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (d *DB) InsertBalancesBatch(ctx context.Context, balances []*types.Balance) error {
	if len(balances) == 0 {
		return nil
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, b := range balances {
		batch.Queue(`
			INSERT INTO balances (address, token_address, block_number, balance)
			VALUES (LOWER($1), LOWER($2), $3, $4)
			ON CONFLICT (address, token_address, block_number) DO UPDATE SET balance = EXCLUDED.balance`,
			b.Address, b.TokenAddress, b.BlockNumber, b.Balance)

		// token_balances_current caches the row that a DISTINCT ON (address)
		// ... ORDER BY block_number DESC over balances would have returned, so
		// the guard has to preserve exactly that: highest block_number wins.
		// >= rather than > because the upsert above is last-write-wins for an
		// identical block_number, and the two must not disagree.
		//
		// The guard is also what makes this safe under concurrency: a second
		// transaction touching the same row blocks on the row lock, then
		// re-evaluates this WHERE against the updated row, so the highest
		// block_number survives whatever order the writes arrive in. Plain
		// last-write-wins would have been arrival-order dependent.
		//
		// One statement per row, matching the insert above, so repeated
		// (token, address) pairs within one flush batch stay separate
		// commands; a single INSERT whose VALUES list conflicts with itself
		// fails with "ON CONFLICT DO UPDATE command cannot affect row a
		// second time".
		//
		// block_number is the block that TRIGGERED the read, not the block the
		// balance was read at -- the balance workers read at latest. balances
		// carries the same defect, and this mirrors it rather than diverging
		// from the query it replaces. See PRST-4508.
		batch.Queue(`
			INSERT INTO token_balances_current (token_address, address, balance, block_number)
			VALUES (LOWER($1), LOWER($2), $3, $4)
			ON CONFLICT (token_address, address) DO UPDATE
				SET balance = EXCLUDED.balance,
					block_number = EXCLUDED.block_number
				WHERE EXCLUDED.block_number >= token_balances_current.block_number`,
			b.TokenAddress, b.Address, b.Balance, b.BlockNumber)
	}

	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (d *DB) GetAllTokenAddresses(ctx context.Context) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT address FROM tokens`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addresses []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		addresses = append(addresses, addr)
	}
	return addresses, rows.Err()
}
