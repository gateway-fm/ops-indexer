package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/jackc/pgx/v5"
)

// Block operations

func (d *DB) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	var num *uint64
	err := d.pool.QueryRow(ctx, "SELECT MAX(number) FROM blocks").Scan(&num)
	if err != nil || num == nil {
		return 0, err
	}
	return *num, nil
}

func (d *DB) InsertBlock(ctx context.Context, b *types.Block) error {
	// Tx wrap keeps the chain_counters bump atomic with the row insert.
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	ct, err := tx.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, transaction_count,
			size, difficulty, total_difficulty, nonce, miner, extra_data, state_root, transactions_root, receipts_root)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
		ON CONFLICT (number) DO NOTHING`,
		b.Number, b.Hash, b.ParentHash, b.Timestamp, b.GasUsed, b.GasLimit, b.BaseFeePerGas, b.TransactionCount,
		b.Size, b.Difficulty, b.TotalDifficulty, b.Nonce, b.Miner, b.ExtraData, b.StateRoot, b.TransactionsRoot, b.ReceiptsRoot)
	if err != nil {
		return err
	}
	if ct.RowsAffected() > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO chain_counters (name, count, updated_at) VALUES ('blocks_total', $1, NOW())
			ON CONFLICT (name) DO UPDATE SET count = chain_counters.count + EXCLUDED.count, updated_at = NOW()`,
			ct.RowsAffected()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (d *DB) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	var b types.Block
	err := d.pool.QueryRow(ctx, `
		SELECT number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, transaction_count,
			size, difficulty, total_difficulty, nonce, miner, extra_data, state_root, transactions_root, receipts_root, created_at
		FROM blocks WHERE number = $1`, number).Scan(
		&b.Number, &b.Hash, &b.ParentHash, &b.Timestamp, &b.GasUsed, &b.GasLimit, &b.BaseFeePerGas, &b.TransactionCount,
		&b.Size, &b.Difficulty, &b.TotalDifficulty, &b.Nonce, &b.Miner, &b.ExtraData, &b.StateRoot, &b.TransactionsRoot, &b.ReceiptsRoot, &b.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &b, err
}

func (d *DB) GetBlockByHash(ctx context.Context, hash string) (*types.Block, error) {
	var b types.Block
	err := d.pool.QueryRow(ctx, `
		SELECT number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, transaction_count,
			size, difficulty, total_difficulty, nonce, miner, extra_data, state_root, transactions_root, receipts_root, created_at
		FROM blocks WHERE hash = $1`, hash).Scan(
		&b.Number, &b.Hash, &b.ParentHash, &b.Timestamp, &b.GasUsed, &b.GasLimit, &b.BaseFeePerGas, &b.TransactionCount,
		&b.Size, &b.Difficulty, &b.TotalDifficulty, &b.Nonce, &b.Miner, &b.ExtraData, &b.StateRoot, &b.TransactionsRoot, &b.ReceiptsRoot, &b.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &b, err
}

func (d *DB) GetBlocks(ctx context.Context, limit int, beforeBlock *uint64) ([]types.Block, error) {
	var rows pgx.Rows
	var err error

	if beforeBlock != nil {
		rows, err = d.pool.Query(ctx, `
			SELECT number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, transaction_count,
				size, difficulty, total_difficulty, nonce, miner, extra_data, state_root, transactions_root, receipts_root, created_at
			FROM blocks WHERE number < $1 ORDER BY number DESC LIMIT $2`, *beforeBlock, limit)
	} else {
		rows, err = d.pool.Query(ctx, `
			SELECT number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, transaction_count,
				size, difficulty, total_difficulty, nonce, miner, extra_data, state_root, transactions_root, receipts_root, created_at
			FROM blocks ORDER BY number DESC LIMIT $1`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blocks []types.Block
	for rows.Next() {
		var b types.Block
		if err := rows.Scan(&b.Number, &b.Hash, &b.ParentHash, &b.Timestamp, &b.GasUsed, &b.GasLimit, &b.BaseFeePerGas, &b.TransactionCount,
			&b.Size, &b.Difficulty, &b.TotalDifficulty, &b.Nonce, &b.Miner, &b.ExtraData, &b.StateRoot, &b.TransactionsRoot, &b.ReceiptsRoot, &b.CreatedAt); err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, rows.Err()
}

func (d *DB) DeleteBlock(ctx context.Context, number uint64) error {
	_, err := d.pool.Exec(ctx, "DELETE FROM blocks WHERE number = $1", number)
	return err
}

// Transaction operations

func (d *DB) InsertTransaction(ctx context.Context, tx *types.Transaction) error {
	// The one-off insert path doesn't know whether there are token transfers
	// for this tx — bit 3 stays unset and is populated when the token_transfers
	// rows are inserted alongside. The batch path (InsertBlockDataBatch) sets
	// it atomically in the same transaction.
	categories := computeTxCategories(tx, false)
	_, err := d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price,
			gas_limit, max_fee_per_gas, max_priority_fee_per_gas, nonce, tx_type, input_data, status, error, revert_reason, categories)
		VALUES ($1, $2, $3, $4, LOWER($5), LOWER($6), $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
		ON CONFLICT (hash) DO NOTHING`,
		tx.Hash, tx.BlockNumber, tx.BlockTimestamp, tx.TxIndex, tx.From, tx.To, tx.Value, tx.GasUsed, tx.GasPrice,
		tx.GasLimit, tx.MaxFeePerGas, tx.MaxPriorityFeePerGas, tx.Nonce, tx.TxType, tx.InputData, tx.Status, tx.Error, tx.RevertReason, categories)
	return err
}

func (d *DB) GetTransaction(ctx context.Context, hash string) (*types.Transaction, error) {
	var tx types.Transaction
	var valueStr string
	err := d.pool.QueryRow(ctx, `
		SELECT t.hash, t.block_number, t.tx_index, t.from_address, t.to_address, c.address, t.value::text,
			t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
			t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at
		FROM transactions t
		LEFT JOIN contracts c ON c.creation_tx = t.hash
		WHERE t.hash = $1`, hash).Scan(
		&tx.Hash, &tx.BlockNumber, &tx.TxIndex, &tx.From, &tx.To, &tx.ContractAddress, &valueStr,
		&tx.GasUsed, &tx.GasPrice, &tx.GasLimit, &tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.Nonce,
		&tx.TxType, &tx.InputData, &tx.Status, &tx.Error, &tx.RevertReason, &tx.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tx.Value = types.JSONString(valueStr)
	return &tx, nil
}

func (d *DB) GetTransactions(ctx context.Context, limit int, beforeBlock *uint64) ([]types.Transaction, error) {
	var rows pgx.Rows
	var err error

	if beforeBlock != nil {
		rows, err = d.pool.Query(ctx, `
			SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, t.value::text,
				t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
				t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at
			FROM transactions t
			WHERE NOT (t.tx_type = ANY($1::int[])) AND t.block_number < $2 ORDER BY t.block_number DESC, t.tx_index DESC LIMIT $3`, d.HiddenTxTypes, *beforeBlock, limit)
	} else {
		rows, err = d.pool.Query(ctx, `
			SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, t.value::text,
				t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
				t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at
			FROM transactions t
			WHERE NOT (t.tx_type = ANY($1::int[])) ORDER BY t.block_number DESC, t.tx_index DESC LIMIT $2`, d.HiddenTxTypes, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanTransactionsWithTimestamp(rows)
}

func (d *DB) GetTransactionsPaginated(ctx context.Context, page, pageSize int) ([]types.Transaction, int64, error) {
	var total int64
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions WHERE NOT (tx_type = ANY($1::int[]))`, d.HiddenTxTypes).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	rows, err := d.pool.Query(ctx, `
		SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, t.value::text,
			t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
			t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at
		FROM transactions t
		WHERE NOT (t.tx_type = ANY($1::int[]))
		ORDER BY t.block_number DESC, t.tx_index DESC
		LIMIT $2 OFFSET $3`, d.HiddenTxTypes, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	txs, err := scanTransactionsWithTimestamp(rows)
	return txs, total, err
}

func (d *DB) GetTransactionsByBlock(ctx context.Context, blockNumber uint64) ([]types.Transaction, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, t.value::text,
			t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
			t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at
		FROM transactions t
		WHERE t.block_number = $1 ORDER BY t.tx_index`, blockNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTransactionsWithTimestamp(rows)
}

func (d *DB) GetTransactionsByAddress(ctx context.Context, address string, limit int, beforeBlock *uint64) ([]types.Transaction, error) {
	var rows pgx.Rows
	var err error

	// Addresses in this table are stored in mixed-case (checksum) form
	// because they come straight from RPC. Callers pass a normalised
	// lowercase address, so the WHERE clause has to be case-insensitive
	// for any rows to match.
	//
	// We also surface transactions where the address participates only via
	// a Transfer event (e.g. an ERC-20 recipient who never sent a tx of
	// their own and so isn't the tx-level from/to). Otherwise the page
	// reads "0 transactions" for any pure-recipient address, which surprises
	// users who clicked through from the token transfers list.
	addr := strings.ToLower(address)
	if beforeBlock != nil {
		rows, err = d.pool.Query(ctx, `
			SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, t.value::text,
				t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
				t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at
			FROM transactions t
			WHERE (
				t.from_address = $1
				OR t.to_address = $1
				OR t.hash IN (
					SELECT tx_hash FROM token_transfers
					WHERE from_address = $1 OR to_address = $1
				)
			) AND t.block_number < $2
			ORDER BY t.block_number DESC, t.tx_index DESC LIMIT $3`, addr, *beforeBlock, limit)
	} else {
		rows, err = d.pool.Query(ctx, `
			SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, t.value::text,
				t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
				t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at
			FROM transactions t
			WHERE t.from_address = $1
			   OR t.to_address = $1
			   OR t.hash IN (
				SELECT tx_hash FROM token_transfers
				WHERE from_address = $1 OR to_address = $1
			   )
			ORDER BY t.block_number DESC, t.tx_index DESC LIMIT $2`, addr, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTransactionsWithTimestamp(rows)
}

func scanTransactionsWithTimestamp(rows pgx.Rows) ([]types.Transaction, error) {
	var txs []types.Transaction
	for rows.Next() {
		var tx types.Transaction
		var valueStr string
		if err := rows.Scan(&tx.Hash, &tx.BlockNumber, &tx.BlockTimestamp, &tx.TxIndex, &tx.From, &tx.To, &valueStr,
			&tx.GasUsed, &tx.GasPrice, &tx.GasLimit, &tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.Nonce,
			&tx.TxType, &tx.InputData, &tx.Status, &tx.Error, &tx.RevertReason, &tx.CreatedAt); err != nil {
			return nil, err
		}
		tx.Value = types.JSONString(valueStr)
		txs = append(txs, tx)
	}
	return txs, rows.Err()
}

func (d *DB) GetTransactionsWithCategories(ctx context.Context, limit int, beforeBlock *uint64) ([]types.Transaction, error) {
	var rows pgx.Rows
	var err error

	// Categories are read from the materialized `categories` bitfield populated
	// at insert time (migration 003). Prior implementation ran 4 correlated
	// subqueries per row.
	query := `
		SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, t.value::text,
			t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
			t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at,
			t.categories
		FROM transactions t`

	if beforeBlock != nil {
		query += ` WHERE NOT (t.tx_type = ANY($1::int[])) AND t.block_number < $2 ORDER BY t.block_number DESC, t.tx_index DESC LIMIT $3`
		rows, err = d.pool.Query(ctx, query, d.HiddenTxTypes, *beforeBlock, limit)
	} else {
		query += ` WHERE NOT (t.tx_type = ANY($1::int[])) ORDER BY t.block_number DESC, t.tx_index DESC LIMIT $2`
		rows, err = d.pool.Query(ctx, query, d.HiddenTxTypes, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanTransactionsWithCategories(rows)
}

func (d *DB) GetTransactionsPaginatedWithCategories(ctx context.Context, page, pageSize int) ([]types.Transaction, int64, error) {
	var total int64
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM transactions WHERE NOT (tx_type = ANY($1::int[]))`, d.HiddenTxTypes).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	rows, err := d.pool.Query(ctx, `
		SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, t.value::text,
			t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
			t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at,
			t.categories
		FROM transactions t
		WHERE NOT (t.tx_type = ANY($1::int[]))
		ORDER BY t.block_number DESC, t.tx_index DESC
		LIMIT $2 OFFSET $3`, d.HiddenTxTypes, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	txs, err := scanTransactionsWithCategories(rows)
	return txs, total, err
}

func (d *DB) GetTransactionWithCategories(ctx context.Context, hash string) (*types.Transaction, error) {
	var tx types.Transaction
	var valueStr string
	var bits int16

	err := d.pool.QueryRow(ctx, `
		SELECT t.hash, t.block_number, t.block_timestamp, t.tx_index, t.from_address, t.to_address, c.address, t.value::text,
			t.gas_used, t.gas_price, t.gas_limit, t.max_fee_per_gas, t.max_priority_fee_per_gas, t.nonce,
			t.tx_type, t.input_data, t.status, t.error, t.revert_reason, t.created_at,
			t.categories
		FROM transactions t
		LEFT JOIN contracts c ON c.creation_tx = t.hash
		WHERE t.hash = $1`, hash).Scan(
		&tx.Hash, &tx.BlockNumber, &tx.BlockTimestamp, &tx.TxIndex, &tx.From, &tx.To, &tx.ContractAddress, &valueStr,
		&tx.GasUsed, &tx.GasPrice, &tx.GasLimit, &tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.Nonce,
		&tx.TxType, &tx.InputData, &tx.Status, &tx.Error, &tx.RevertReason, &tx.CreatedAt,
		&bits)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tx.Value = types.JSONString(valueStr)
	tx.TxCategories = buildCategoriesFromBits(tx.TxType, bits)
	if bits&CategoryTokenTransfer != 0 {
		tx.TokenTransferCount = 1 // materialized column stores presence, not count
	}

	return &tx, nil
}

func scanTransactionsWithCategories(rows pgx.Rows) ([]types.Transaction, error) {
	var txs []types.Transaction
	for rows.Next() {
		var tx types.Transaction
		var valueStr string
		var bits int16

		if err := rows.Scan(&tx.Hash, &tx.BlockNumber, &tx.BlockTimestamp, &tx.TxIndex, &tx.From, &tx.To, &valueStr,
			&tx.GasUsed, &tx.GasPrice, &tx.GasLimit, &tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.Nonce,
			&tx.TxType, &tx.InputData, &tx.Status, &tx.Error, &tx.RevertReason, &tx.CreatedAt,
			&bits); err != nil {
			return nil, err
		}
		tx.Value = types.JSONString(valueStr)
		tx.TxCategories = buildCategoriesFromBits(tx.TxType, bits)
		if bits&CategoryTokenTransfer != 0 {
			tx.TokenTransferCount = 1
		}
		txs = append(txs, tx)
	}
	return txs, rows.Err()
}

// buildCategoriesFromBits converts the materialized bitfield (migration 003)
// to the free-form category labels used in the existing types.Transaction.
// Preserves the legacy "system_transaction" special-case for OP deposit txs.
func buildCategoriesFromBits(txType int, bits int16) []string {
	if txType == types.TxTypeDeposit {
		return []string{types.TxCategorySystemTransaction}
	}
	var categories []string
	if bits&CategoryContractCreation != 0 {
		categories = append(categories, types.TxCategoryContractCreation)
	}
	if bits&CategoryContractCall != 0 {
		categories = append(categories, types.TxCategoryContractCall)
	}
	if bits&CategoryCoinTransfer != 0 {
		categories = append(categories, types.TxCategoryCoinTransfer)
	}
	if bits&CategoryTokenTransfer != 0 {
		categories = append(categories, types.TxCategoryTokenTransfer)
	}
	return categories
}

// Token operations

func (d *DB) InsertToken(ctx context.Context, t *types.Token) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO tokens (address, symbol, name, decimals, token_type, total_supply, block_number, creation_tx, l1_address)
		VALUES (LOWER($1), $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (address) DO UPDATE SET
			symbol = COALESCE(EXCLUDED.symbol, tokens.symbol),
			name = COALESCE(EXCLUDED.name, tokens.name),
			decimals = COALESCE(EXCLUDED.decimals, tokens.decimals),
			total_supply = COALESCE(EXCLUDED.total_supply, tokens.total_supply)`,
		t.Address, t.Symbol, t.Name, t.Decimals, t.TokenType, t.TotalSupply, t.BlockNumber, t.CreationTx, t.L1Address)
	return err
}

func (d *DB) GetToken(ctx context.Context, address string) (*types.Token, error) {
	var t types.Token
	err := d.pool.QueryRow(ctx, `
		SELECT address, symbol, name, decimals, token_type, total_supply, holder_count, transfer_count,
			usd_price, icon_url, l1_address, block_number, creation_tx, off_chain_updated_at, created_at
		FROM tokens WHERE address = LOWER($1)`, address).Scan(
		&t.Address, &t.Symbol, &t.Name, &t.Decimals, &t.TokenType, &t.TotalSupply, &t.HolderCount, &t.TransferCount,
		&t.USDPrice, &t.IconURL, &t.L1Address, &t.BlockNumber, &t.CreationTx, &t.OffChainUpdatedAt, &t.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &t, err
}

// GetTokens returns a page of tokens, optionally filtered by token type and a
// case-insensitive substring match over name / symbol / address. The same
// WHERE clause is reused for the COUNT and the page query so totals match.
func (d *DB) GetTokens(ctx context.Context, limit int, offset int, tokenType, search string) ([]types.Token, int64, error) {
	var (
		conds []string
		args  []any
	)
	if tokenType != "" {
		args = append(args, tokenType)
		conds = append(conds, fmt.Sprintf("token_type = $%d", len(args)))
	}
	if search = strings.TrimSpace(search); search != "" {
		args = append(args, "%"+strings.ToLower(search)+"%")
		conds = append(conds, fmt.Sprintf(
			"(LOWER(name) LIKE $%d OR LOWER(symbol) LIKE $%d OR LOWER(address) LIKE $%d)",
			len(args), len(args), len(args),
		))
	}
	where := ""
	if len(conds) > 0 {
		where = " WHERE " + strings.Join(conds, " AND ")
	}

	var total int64
	if err := d.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tokens"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, limit, offset)
	query := `
		SELECT address, symbol, name, decimals, token_type, total_supply, holder_count, transfer_count,
			usd_price, icon_url, l1_address, block_number, creation_tx, off_chain_updated_at, created_at
		FROM tokens` + where + fmt.Sprintf(
		" ORDER BY holder_count DESC, address LIMIT $%d OFFSET $%d", len(args)-1, len(args),
	)

	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var tokens []types.Token
	for rows.Next() {
		var t types.Token
		if err := rows.Scan(&t.Address, &t.Symbol, &t.Name, &t.Decimals, &t.TokenType, &t.TotalSupply, &t.HolderCount, &t.TransferCount,
			&t.USDPrice, &t.IconURL, &t.L1Address, &t.BlockNumber, &t.CreationTx, &t.OffChainUpdatedAt, &t.CreatedAt); err != nil {
			return nil, 0, err
		}
		tokens = append(tokens, t)
	}
	return tokens, total, rows.Err()
}

func (d *DB) UpdateTokenStats(ctx context.Context, address string, holderCount int, transferCount int) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE tokens SET holder_count = $2, transfer_count = $3 WHERE address = LOWER($1)`,
		address, holderCount, transferCount)
	return err
}

// RefreshTokenStats recomputes holder_count, transfer_count, and total_supply
// for a single token from the underlying token_transfers / balances / nft_tokens
// tables, then writes the result onto the tokens row. Cheap and idempotent. The
// token row must already exist; if not, the call is a no-op.
//
// holder_count counts addresses whose latest balance for this token is > 0.
// transfer_count is COUNT(*) over token_transfers.
// total_supply is mints−burns for ERC20, and the live (non-burned) token-id
// count for ERC721; left untouched for other standards.
func (d *DB) RefreshTokenStats(ctx context.Context, tokenAddress string) error {
	addr := strings.ToLower(tokenAddress)

	var tokenType string
	if err := d.pool.QueryRow(ctx,
		`SELECT token_type FROM tokens WHERE address = $1`, addr,
	).Scan(&tokenType); err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return fmt.Errorf("RefreshTokenStats: read token_type: %w", err)
	}

	var transferCount int64
	if err := d.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM token_transfers WHERE token_address = $1`, addr,
	).Scan(&transferCount); err != nil {
		return fmt.Errorf("RefreshTokenStats: count transfers: %w", err)
	}

	var holderCount int64
	if err := d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT DISTINCT ON (address) balance
			FROM balances
			WHERE token_address = $1
			ORDER BY address, block_number DESC
		) latest
		WHERE balance > 0`, addr,
	).Scan(&holderCount); err != nil {
		return fmt.Errorf("RefreshTokenStats: count holders: %w", err)
	}

	if tokenType == "ERC20" {
		_, err := d.pool.Exec(ctx, `
			UPDATE tokens
			SET holder_count = $2,
			    transfer_count = $3,
			    total_supply = (
			        SELECT COALESCE(
			            SUM(CASE WHEN from_address = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END)
			          - SUM(CASE WHEN to_address   = '0x0000000000000000000000000000000000000000' THEN value ELSE 0 END),
			            0
			        )
			        FROM token_transfers
			        WHERE token_address = $1 AND token_type = 'ERC20'
			    )
			WHERE address = $1`,
			addr, holderCount, transferCount)
		if err != nil {
			return fmt.Errorf("RefreshTokenStats: update ERC20 stats: %w", err)
		}
		return nil
	}

	if tokenType == "ERC721" {
		// Supply for an NFT collection is the number of live (non-burned) token
		// ids, tracked per-instance in nft_tokens.
		_, err := d.pool.Exec(ctx, `
			UPDATE tokens
			SET holder_count = $2,
			    transfer_count = $3,
			    total_supply = (
			        SELECT COUNT(*) FROM nft_tokens
			        WHERE LOWER(token_address) = $1
			          AND owner <> '0x0000000000000000000000000000000000000000'
			    )
			WHERE LOWER(address) = $1`,
			addr, holderCount, transferCount)
		if err != nil {
			return fmt.Errorf("RefreshTokenStats: update ERC721 stats: %w", err)
		}
		return nil
	}

	_, err := d.pool.Exec(ctx, `
		UPDATE tokens SET holder_count = $2, transfer_count = $3 WHERE address = $1`,
		addr, holderCount, transferCount)
	if err != nil {
		return fmt.Errorf("RefreshTokenStats: update stats: %w", err)
	}
	return nil
}

func (d *DB) UpdateTokenPrice(ctx context.Context, address string, price float64, iconURL *string) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE tokens SET usd_price = $2, icon_url = $3, off_chain_updated_at = NOW()
		WHERE address = LOWER($1)`,
		address, price, iconURL)
	return err
}

// Token transfer operations

func (d *DB) InsertTokenTransfer(ctx context.Context, t *types.TokenTransfer) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value,
			block_number, timestamp, transfer_type, token_type, token_id, is_internal)
		VALUES ($1, $2, LOWER($3), LOWER($4), LOWER($5), $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (tx_hash, log_index) DO NOTHING`,
		t.TxHash, t.LogIndex, t.TokenAddress, t.From, t.To, t.Value,
		t.BlockNumber, t.Timestamp, t.TransferType, t.TokenType, t.TokenID, t.IsInternal)
	return err
}

func (d *DB) GetTransfersByAddress(ctx context.Context, address string, limit int, beforeBlock *uint64) ([]types.TokenTransfer, error) {
	var rows pgx.Rows
	var err error

	// Addresses are canonical lowercase post migration 005; the caller's
	// strings.ToLower is enough to match the stored column without a
	// LOWER() on the column side (which would otherwise defeat
	// idx_transfer_from / idx_transfer_to).
	addr := strings.ToLower(address)
	if beforeBlock != nil {
		rows, err = d.pool.Query(ctx, `
			SELECT id, tx_hash, log_index, token_address, from_address, to_address, value::text, block_number,
				timestamp, transfer_type, token_type, token_id, is_internal
			FROM token_transfers WHERE (from_address = $1 OR to_address = $1) AND block_number < $2
			ORDER BY block_number DESC, log_index DESC LIMIT $3`, addr, *beforeBlock, limit)
	} else {
		rows, err = d.pool.Query(ctx, `
			SELECT id, tx_hash, log_index, token_address, from_address, to_address, value::text, block_number,
				timestamp, transfer_type, token_type, token_id, is_internal
			FROM token_transfers WHERE from_address = $1 OR to_address = $1
			ORDER BY block_number DESC, log_index DESC LIMIT $2`, addr, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTokenTransfers(rows)
}

func (d *DB) GetTransfersByTransaction(ctx context.Context, txHash string) ([]types.TokenTransfer, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, log_index, token_address, from_address, to_address, value::text, block_number,
			timestamp, transfer_type, token_type, token_id, is_internal
		FROM token_transfers WHERE tx_hash = $1
		ORDER BY log_index`, txHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTokenTransfers(rows)
}

func (d *DB) GetTransfersByToken(ctx context.Context, tokenAddress string, limit int, offset int) ([]types.TokenTransfer, int64, error) {
	var total int64
	d.pool.QueryRow(ctx, "SELECT COUNT(*) FROM token_transfers WHERE token_address = LOWER($1)", tokenAddress).Scan(&total)

	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, log_index, token_address, from_address, to_address, value::text, block_number,
			timestamp, transfer_type, token_type, token_id, is_internal
		FROM token_transfers WHERE token_address = LOWER($1)
		ORDER BY block_number DESC, log_index DESC LIMIT $2 OFFSET $3`, tokenAddress, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	transfers, err := scanTokenTransfers(rows)
	return transfers, total, err
}

func (d *DB) GetAllTransfers(ctx context.Context, limit int, offset int) ([]types.TokenTransfer, int64, error) {
	var total int64
	d.pool.QueryRow(ctx, "SELECT COUNT(*) FROM token_transfers").Scan(&total)

	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, log_index, token_address, from_address, to_address, value::text, block_number,
			timestamp, transfer_type, token_type, token_id, is_internal
		FROM token_transfers
		ORDER BY block_number DESC, log_index DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	transfers, err := scanTokenTransfers(rows)
	return transfers, total, err
}

func scanTokenTransfers(rows pgx.Rows) ([]types.TokenTransfer, error) {
	var transfers []types.TokenTransfer
	for rows.Next() {
		var t types.TokenTransfer
		var valueStr string
		if err := rows.Scan(&t.ID, &t.TxHash, &t.LogIndex, &t.TokenAddress, &t.From, &t.To, &valueStr, &t.BlockNumber,
			&t.Timestamp, &t.TransferType, &t.TokenType, &t.TokenID, &t.IsInternal); err != nil {
			return nil, err
		}
		t.Value = types.JSONString(valueStr)
		transfers = append(transfers, t)
	}
	return transfers, rows.Err()
}

// Token holder operations

func (d *DB) GetTokenHolders(ctx context.Context, tokenAddress string, limit int, offset int) ([]types.TokenHolder, int64, error) {
	var total int64
	d.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT address) FROM balances
		WHERE token_address = LOWER($1) AND balance > 0`, tokenAddress).Scan(&total)

	rows, err := d.pool.Query(ctx, `
		WITH latest_balances AS (
			SELECT address, balance,
				ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_number DESC) as rn
			FROM balances
			WHERE token_address = LOWER($1) AND balance > 0
		),
		total_supply AS (
			SELECT COALESCE(SUM(balance), 1) as supply FROM latest_balances WHERE rn = 1
		)
		SELECT lb.address, lb.balance::text,
			(lb.balance::numeric / NULLIF(ts.supply::numeric, 0) * 100)::numeric(10,4) as percentage,
			COALESCE((SELECT true FROM contracts WHERE address = LOWER(lb.address)), false) as is_contract
		FROM latest_balances lb, total_supply ts
		WHERE lb.rn = 1
		ORDER BY lb.balance DESC
		LIMIT $2 OFFSET $3`, tokenAddress, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var holders []types.TokenHolder
	for rows.Next() {
		var h types.TokenHolder
		var balanceStr string
		if err := rows.Scan(&h.Address, &balanceStr, &h.Percentage, &h.IsContract); err != nil {
			return nil, 0, err
		}
		h.Balance = types.JSONString(balanceStr)
		holders = append(holders, h)
	}
	return holders, total, rows.Err()
}

// GetTokenInventory lists live (non-burned) NFT instances of a collection,
// ordered by token id. Owner and tokenURI come straight from nft_tokens, which
// the indexer maintains per (token_address, token_id). A non-empty tokenID
// filters to a single instance (used by the per-NFT detail lookup).
func (d *DB) GetTokenInventory(ctx context.Context, tokenAddress string, tokenID string, limit int, offset int) ([]types.TokenInventoryItem, int64, error) {
	var total int64
	d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM nft_tokens
		WHERE LOWER(token_address) = LOWER($1)
		  AND owner <> '0x0000000000000000000000000000000000000000'
		  AND ($2 = '' OR token_id = $2::numeric)`,
		tokenAddress, tokenID).Scan(&total)

	rows, err := d.pool.Query(ctx, `
		SELECT token_id::text, owner, token_uri
		FROM nft_tokens
		WHERE LOWER(token_address) = LOWER($1)
		  AND owner <> '0x0000000000000000000000000000000000000000'
		  AND ($2 = '' OR token_id = $2::numeric)
		ORDER BY token_id
		LIMIT $3 OFFSET $4`, tokenAddress, tokenID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var items []types.TokenInventoryItem
	for rows.Next() {
		var it types.TokenInventoryItem
		if err := rows.Scan(&it.TokenID, &it.Owner, &it.TokenURI); err != nil {
			return nil, 0, err
		}
		items = append(items, it)
	}
	return items, total, rows.Err()
}

// Balance operations

func (d *DB) InsertBalance(ctx context.Context, b *types.Balance) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO balances (address, token_address, block_number, balance)
		VALUES (LOWER($1), LOWER($2), $3, $4)
		ON CONFLICT (address, token_address, block_number) DO UPDATE SET balance = EXCLUDED.balance`,
		b.Address, b.TokenAddress, b.BlockNumber, b.Balance)
	return err
}

func (d *DB) GetLatestBalance(ctx context.Context, address string, tokenAddress string) (*types.Balance, error) {
	var b types.Balance
	var balanceStr string
	err := d.pool.QueryRow(ctx, `
		SELECT address, token_address, block_number, balance::text
		FROM balances
		WHERE address = LOWER($1) AND token_address = LOWER($2)
		ORDER BY block_number DESC LIMIT 1`, address, tokenAddress).Scan(
		&b.Address, &b.TokenAddress, &b.BlockNumber, &balanceStr)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	b.Balance = types.JSONString(balanceStr)
	return &b, nil
}

func (d *DB) GetBalanceHistory(ctx context.Context, address string, tokenAddress string, limit int) ([]types.Balance, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT address, token_address, block_number, balance::text
		FROM balances
		WHERE address = LOWER($1) AND token_address = LOWER($2)
		ORDER BY block_number DESC LIMIT $3`, address, tokenAddress, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var balances []types.Balance
	for rows.Next() {
		var b types.Balance
		var balanceStr string
		if err := rows.Scan(&b.Address, &b.TokenAddress, &b.BlockNumber, &balanceStr); err != nil {
			return nil, err
		}
		b.Balance = types.JSONString(balanceStr)
		balances = append(balances, b)
	}
	return balances, rows.Err()
}

func (d *DB) GetTokenBalances(ctx context.Context, address string) ([]types.Balance, error) {
	rows, err := d.pool.Query(ctx, `
		WITH latest_balances AS (
			SELECT address, token_address, balance, block_number,
				ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY block_number DESC) as rn
			FROM balances
			WHERE address = LOWER($1)
		)
		SELECT address, token_address, block_number, balance::text
		FROM latest_balances WHERE rn = 1 AND balance > 0
		ORDER BY balance DESC`, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var balances []types.Balance
	for rows.Next() {
		var b types.Balance
		var balanceStr string
		if err := rows.Scan(&b.Address, &b.TokenAddress, &b.BlockNumber, &balanceStr); err != nil {
			return nil, err
		}
		b.Balance = types.JSONString(balanceStr)
		balances = append(balances, b)
	}
	return balances, rows.Err()
}

// Counter operations

func (d *DB) IncrementCounter(ctx context.Context, address string, counterType string, delta int64) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO counters (address, counter_type, count, updated_at)
		VALUES (LOWER($1), $2, $3, NOW())
		ON CONFLICT (address, counter_type) DO UPDATE SET
			count = counters.count + $3,
			updated_at = NOW()`,
		address, counterType, delta)
	return err
}

func (d *DB) GetCounter(ctx context.Context, address string, counterType string) (int64, error) {
	var count int64
	err := d.pool.QueryRow(ctx, `
		SELECT count FROM counters WHERE address = LOWER($1) AND counter_type = $2`,
		address, counterType).Scan(&count)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return count, err
}

func (d *DB) GetCounters(ctx context.Context, address string) ([]types.Counter, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT address, counter_type, count, updated_at
		FROM counters WHERE address = LOWER($1)`, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var counters []types.Counter
	for rows.Next() {
		var c types.Counter
		if err := rows.Scan(&c.Address, &c.CounterType, &c.Count, &c.UpdatedAt); err != nil {
			return nil, err
		}
		counters = append(counters, c)
	}
	return counters, rows.Err()
}

// Address stats operations

func (d *DB) UpsertAddressStats(ctx context.Context, address string, blockNumber uint64, isContract bool) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count, first_seen, last_seen, is_contract, updated_at)
		VALUES (LOWER($1), 1, $2, $2, $3, NOW())
		ON CONFLICT (address) DO UPDATE SET
			tx_count = address_stats.tx_count + 1,
			last_seen = $2,
			is_contract = COALESCE(EXCLUDED.is_contract, address_stats.is_contract),
			updated_at = NOW()`,
		address, blockNumber, isContract)
	return err
}

func (d *DB) UpdateAddressStatsCounters(ctx context.Context, address string, internalTxCount int, tokenTransferCount int) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE address_stats SET
			internal_tx_count = internal_tx_count + $2,
			token_transfer_count = token_transfer_count + $3,
			updated_at = NOW()
		WHERE address = LOWER($1)`,
		address, internalTxCount, tokenTransferCount)
	return err
}

func (d *DB) GetAddressStats(ctx context.Context, address string) (*types.AddressStats, error) {
	var s types.AddressStats
	err := d.pool.QueryRow(ctx, `
		SELECT address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, updated_at
		FROM address_stats WHERE address = LOWER($1)`, address).Scan(
		&s.Address, &s.TxCount, &s.InternalTxCount, &s.TokenTransferCount, &s.FirstSeen, &s.LastSeen, &s.IsContract, &s.UpdatedAt)
	if err == pgx.ErrNoRows {
		return &types.AddressStats{Address: address}, nil
	}
	return &s, err
}

func (d *DB) GetAccountsPaginated(ctx context.Context, page, pageSize int) ([]types.AddressStats, int64, error) {
	var total int64
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM address_stats`).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	rows, err := d.pool.Query(ctx, `
		SELECT address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, updated_at
		FROM address_stats
		ORDER BY tx_count DESC, address ASC
		LIMIT $1 OFFSET $2`, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var accounts []types.AddressStats
	for rows.Next() {
		var s types.AddressStats
		if err := rows.Scan(&s.Address, &s.TxCount, &s.InternalTxCount, &s.TokenTransferCount, &s.FirstSeen, &s.LastSeen, &s.IsContract, &s.UpdatedAt); err != nil {
			return nil, 0, err
		}
		accounts = append(accounts, s)
	}

	return accounts, total, rows.Err()
}

// Contract operations

func (d *DB) InsertContract(ctx context.Context, c *types.Contract) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO contracts (address, bytecode, bytecode_hash, creator, creation_tx, block_number, is_verified,
			contract_name, compiler_version, optimization_used, source_code, abi)
		VALUES (LOWER($1), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (address) DO NOTHING`,
		c.Address, c.Bytecode, c.BytecodeHash, c.Creator, c.CreationTx, c.BlockNumber, c.IsVerified,
		c.ContractName, c.CompilerVersion, c.OptimizationUsed, c.SourceCode, c.ABI)
	return err
}

func (d *DB) GetContract(ctx context.Context, address string) (*types.Contract, error) {
	var c types.Contract
	err := d.pool.QueryRow(ctx, `
		SELECT address, bytecode, bytecode_hash, creator, creation_tx, block_number, is_verified,
			contract_name, compiler_version, optimization_used, evm_version, source_code, abi, created_at,
			license_type, constructor_args, optimization_runs
		FROM contracts WHERE address = LOWER($1)`, address).Scan(
		&c.Address, &c.Bytecode, &c.BytecodeHash, &c.Creator, &c.CreationTx, &c.BlockNumber, &c.IsVerified,
		&c.ContractName, &c.CompilerVersion, &c.OptimizationUsed, &c.EVMVersion, &c.SourceCode, &c.ABI, &c.CreatedAt,
		&c.LicenseType, &c.ConstructorArgs, &c.OptimizationRuns)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &c, err
}

func (d *DB) IsContract(ctx context.Context, address string) (bool, error) {
	var exists bool
	err := d.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM contracts WHERE address = LOWER($1))`, address).Scan(&exists)
	return exists, err
}

func (d *DB) VerifyContract(ctx context.Context, address string, name string, compilerVersion string, optimizationUsed bool, sourceCode string, abi json.RawMessage, evmVersion string, licenseType string, constructorArgs string, optimizationRuns int) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE contracts SET
			is_verified = true,
			contract_name = $2,
			compiler_version = $3,
			optimization_used = $4,
			source_code = $5,
			abi = $6,
			evm_version = $7,
			license_type = NULLIF($8, ''),
			constructor_args = NULLIF($9, ''),
			optimization_runs = NULLIF($10, 0)
		WHERE address = LOWER($1)`,
		address, name, compilerVersion, optimizationUsed, sourceCode, abi, evmVersion, licenseType, constructorArgs, optimizationRuns)
	return err
}

func (d *DB) UpdateContractABI(ctx context.Context, address string, abi json.RawMessage) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE contracts SET abi = $2
		WHERE address = LOWER($1)`,
		address, abi)
	return err
}

func (d *DB) SetContractABI(ctx context.Context, address string, abi json.RawMessage) error {
	result, err := d.pool.Exec(ctx, `
		UPDATE contracts SET abi = $2
		WHERE address = LOWER($1)`,
		address, abi)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		_, err = d.pool.Exec(ctx, `
			INSERT INTO contracts (address, bytecode, creator, creation_tx, block_number, is_verified, abi)
			VALUES (LOWER($1), '', '', '', 0, false, $2)
			ON CONFLICT (address) DO UPDATE SET abi = $2`,
			address, abi)
	}
	return err
}

func (d *DB) GetVerifiedContracts(ctx context.Context, limit int, offset int) ([]types.Contract, int64, error) {
	var total int64
	d.pool.QueryRow(ctx, "SELECT COUNT(*) FROM contracts WHERE is_verified = true").Scan(&total)

	rows, err := d.pool.Query(ctx, `
		SELECT address, bytecode, bytecode_hash, creator, creation_tx, block_number, is_verified,
			contract_name, compiler_version, optimization_used, evm_version, source_code, abi, created_at
		FROM contracts WHERE is_verified = true
		ORDER BY created_at DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var contracts []types.Contract
	for rows.Next() {
		var c types.Contract
		if err := rows.Scan(&c.Address, &c.Bytecode, &c.BytecodeHash, &c.Creator, &c.CreationTx, &c.BlockNumber, &c.IsVerified,
			&c.ContractName, &c.CompilerVersion, &c.OptimizationUsed, &c.EVMVersion, &c.SourceCode, &c.ABI, &c.CreatedAt); err != nil {
			return nil, 0, err
		}
		contracts = append(contracts, c)
	}
	return contracts, total, rows.Err()
}

// Log operations

func (d *DB) InsertLog(ctx context.Context, l *types.Log) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO logs (tx_hash, log_index, address, topic0, topic1, topic2, topic3, data, block_number, timestamp, removed)
		VALUES ($1, $2, LOWER($3), $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (tx_hash, log_index) DO NOTHING`,
		l.TxHash, l.LogIndex, l.Address, l.Topic0, l.Topic1, l.Topic2, l.Topic3, l.Data, l.BlockNumber, l.Timestamp, l.Removed)
	return err
}

func (d *DB) GetLogsByTransaction(ctx context.Context, txHash string) ([]types.Log, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data, block_number, timestamp, removed
		FROM logs WHERE tx_hash = $1
		ORDER BY log_index`, txHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLogs(rows)
}

func (d *DB) GetLogsByAddress(ctx context.Context, address string, limit int, offset int) ([]types.Log, int64, error) {
	var total int64
	d.pool.QueryRow(ctx, "SELECT COUNT(*) FROM logs WHERE address = LOWER($1)", address).Scan(&total)

	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data, block_number, timestamp, removed
		FROM logs WHERE address = LOWER($1)
		ORDER BY block_number DESC, log_index DESC LIMIT $2 OFFSET $3`, address, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	logs, err := scanLogs(rows)
	return logs, total, err
}

func (d *DB) GetLogsByTopic(ctx context.Context, topic0 string, limit int, offset int) ([]types.Log, int64, error) {
	var total int64
	d.pool.QueryRow(ctx, "SELECT COUNT(*) FROM logs WHERE topic0 = $1", topic0).Scan(&total)

	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data, block_number, timestamp, removed
		FROM logs WHERE topic0 = $1
		ORDER BY block_number DESC, log_index DESC LIMIT $2 OFFSET $3`, topic0, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	logs, err := scanLogs(rows)
	return logs, total, err
}

func (d *DB) GetLogs(ctx context.Context, address *string, topic0 *string, fromBlock *uint64, toBlock *uint64, limit int) ([]types.Log, error) {
	query := `SELECT id, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data, block_number, timestamp, removed FROM logs WHERE 1=1`
	args := []interface{}{}
	argIdx := 1

	if address != nil {
		query += fmt.Sprintf(" AND address = LOWER($%d)", argIdx)
		args = append(args, *address)
		argIdx++
	}
	if topic0 != nil {
		query += fmt.Sprintf(" AND topic0 = $%d", argIdx)
		args = append(args, *topic0)
		argIdx++
	}
	if fromBlock != nil {
		query += fmt.Sprintf(" AND block_number >= $%d", argIdx)
		args = append(args, *fromBlock)
		argIdx++
	}
	if toBlock != nil {
		query += fmt.Sprintf(" AND block_number <= $%d", argIdx)
		args = append(args, *toBlock)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY block_number DESC, log_index DESC LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLogs(rows)
}

func scanLogs(rows pgx.Rows) ([]types.Log, error) {
	var logs []types.Log
	for rows.Next() {
		var l types.Log
		if err := rows.Scan(&l.ID, &l.TxHash, &l.LogIndex, &l.Address, &l.Topic0, &l.Topic1, &l.Topic2, &l.Topic3, &l.Data, &l.BlockNumber, &l.Timestamp, &l.Removed); err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}
	return logs, rows.Err()
}

// Internal transaction operations

func (d *DB) InsertInternalTransaction(ctx context.Context, it *types.InternalTransaction) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value,
			gas, gas_used, input, output, call_type, error, timestamp)
		VALUES ($1, $2, $3, LOWER($4), LOWER($5), $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (tx_hash, trace_address) DO NOTHING`,
		it.TxHash, it.BlockNumber, it.TraceAddress, it.From, it.To, it.Value,
		it.Gas, it.GasUsed, it.Input, it.Output, it.CallType, it.Error, it.Timestamp)
	return err
}

func (d *DB) GetInternalTransactionsByTx(ctx context.Context, txHash string) ([]types.InternalTransaction, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, block_number, trace_address, from_address, to_address, value::text,
			gas, gas_used, input, output, call_type, error, timestamp
		FROM internal_transactions WHERE tx_hash = $1
		ORDER BY trace_address`, txHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanInternalTransactions(rows)
}

func (d *DB) GetInternalTransactionsByBlock(ctx context.Context, blockNumber uint64) ([]types.InternalTransaction, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, block_number, trace_address, from_address, to_address, value::text,
			gas, gas_used, input, output, call_type, error, timestamp
		FROM internal_transactions WHERE block_number = $1
		ORDER BY id`, blockNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanInternalTransactions(rows)
}

func (d *DB) GetInternalTransactionsByAddress(ctx context.Context, address string, limit int, offset int) ([]types.InternalTransaction, int64, error) {
	var total int64
	d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM internal_transactions
		WHERE from_address = LOWER($1) OR to_address = LOWER($1)`, address).Scan(&total)

	rows, err := d.pool.Query(ctx, `
		SELECT id, tx_hash, block_number, trace_address, from_address, to_address, value::text,
			gas, gas_used, input, output, call_type, error, timestamp
		FROM internal_transactions
		WHERE from_address = LOWER($1) OR to_address = LOWER($1)
		ORDER BY block_number DESC, id DESC LIMIT $2 OFFSET $3`, address, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	its, err := scanInternalTransactions(rows)
	return its, total, err
}

func scanInternalTransactions(rows pgx.Rows) ([]types.InternalTransaction, error) {
	var its []types.InternalTransaction
	for rows.Next() {
		var it types.InternalTransaction
		var valueStr string
		if err := rows.Scan(&it.ID, &it.TxHash, &it.BlockNumber, &it.TraceAddress, &it.From, &it.To, &valueStr,
			&it.Gas, &it.GasUsed, &it.Input, &it.Output, &it.CallType, &it.Error, &it.Timestamp); err != nil {
			return nil, err
		}
		it.Value = types.JSONString(valueStr)
		its = append(its, it)
	}
	return its, rows.Err()
}

// Sync status operations

func (d *DB) GetSyncStatus(ctx context.Context) (*types.SyncStatus, error) {
	var s types.SyncStatus
	err := d.pool.QueryRow(ctx, `
		SELECT id, last_indexed_block, last_verified_block, last_finalized_block, is_syncing, updated_at
		FROM sync_status ORDER BY id LIMIT 1`).Scan(
		&s.ID, &s.LastIndexedBlock, &s.LastVerifiedBlock, &s.LastFinalizedBlock, &s.IsSyncing, &s.UpdatedAt)
	if err == pgx.ErrNoRows {
		return &types.SyncStatus{}, nil
	}
	return &s, err
}

func (d *DB) UpdateSyncStatus(ctx context.Context, lastIndexedBlock uint64, isSyncing bool) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE sync_status SET last_indexed_block = $1, is_syncing = $2, updated_at = NOW()
		WHERE id = (SELECT id FROM sync_status ORDER BY id LIMIT 1)`,
		lastIndexedBlock, isSyncing)
	return err
}

func (d *DB) UpdateSyncStatusBlocks(ctx context.Context, verifiedBlock *uint64, finalizedBlock *uint64) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE sync_status SET
			last_verified_block = COALESCE($1, last_verified_block),
			last_finalized_block = COALESCE($2, last_finalized_block),
			updated_at = NOW()
		WHERE id = (SELECT id FROM sync_status ORDER BY id LIMIT 1)`,
		verifiedBlock, finalizedBlock)
	return err
}

// Stats

func (d *DB) GetChainStats(ctx context.Context) (*types.ChainStats, error) {
	var stats types.ChainStats
	var avgBlockTime *float64
	err := d.pool.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT count FROM chain_counters WHERE name = 'blocks_total'), 0),
			COALESCE((SELECT count FROM chain_counters WHERE name = 'transactions_total'), 0),
			COALESCE((SELECT count FROM chain_counters WHERE name = 'addresses_total'), 0),
			COALESCE((SELECT count FROM chain_counters WHERE name = 'tokens_total'), 0),
			(SELECT
				CASE WHEN COUNT(*) > 1
				THEN (MAX(timestamp) - MIN(timestamp))::float / NULLIF(COUNT(*) - 1, 0)
				ELSE NULL END
			FROM (SELECT timestamp FROM blocks ORDER BY number DESC LIMIT 100) recent_blocks)
		`).Scan(&stats.TotalBlocks, &stats.TotalTransactions, &stats.TotalAddresses, &stats.TotalTokens, &avgBlockTime)
	if avgBlockTime != nil {
		stats.AvgBlockTime = *avgBlockTime
	}
	return &stats, err
}

func (d *DB) GetTransactionHistory(ctx context.Context, intervalSeconds int, limit int) ([]types.TxHistoryPoint, error) {
	// Time-window predicate keeps the scan within idx_blocks_timestamp.
	rows, err := d.pool.Query(ctx, `
		SELECT
			(b.timestamp / $1) * $1 as interval_start,
			COUNT(t.hash) as tx_count
		FROM blocks b
		LEFT JOIN transactions t ON t.block_number = b.number
		WHERE b.timestamp >= EXTRACT(EPOCH FROM NOW())::bigint - ($1::bigint * $2::bigint)
		GROUP BY interval_start
		ORDER BY interval_start DESC
		LIMIT $2`, intervalSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var points []types.TxHistoryPoint
	for rows.Next() {
		var p types.TxHistoryPoint
		if err := rows.Scan(&p.Timestamp, &p.Count); err != nil {
			return nil, err
		}
		points = append(points, p)
	}

	// Reverse to get chronological order (oldest first)
	for i, j := 0, len(points)-1; i < j; i, j = i+1, j-1 {
		points[i], points[j] = points[j], points[i]
	}

	return points, rows.Err()
}

// Search suggestions

func (d *DB) SearchSuggestions(ctx context.Context, query string, limit int) ([]types.SearchSuggestion, error) {
	var suggestions []types.SearchSuggestion

	if isNumeric(query) {
		rows, err := d.pool.Query(ctx, `
			SELECT number FROM blocks
			WHERE number::text LIKE $1 || '%'
			ORDER BY number DESC LIMIT $2`, query, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var num uint64
			if err := rows.Scan(&num); err != nil {
				return nil, err
			}
			suggestions = append(suggestions, types.SearchSuggestion{
				Type:  "block",
				Value: fmt.Sprintf("%d", num),
				Label: fmt.Sprintf("Block #%d", num),
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	if len(query) >= 2 && query[:2] == "0x" {
		rows, err := d.pool.Query(ctx, `
			SELECT hash FROM transactions
			WHERE LOWER(hash) LIKE LOWER($1) || '%'
			ORDER BY block_number DESC LIMIT $2`, query, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var hash string
			if err := rows.Scan(&hash); err != nil {
				return nil, err
			}
			suggestions = append(suggestions, types.SearchSuggestion{
				Type:  "transaction",
				Value: hash,
				Label: truncateHash(hash),
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	if len(query) >= 2 && query[:2] == "0x" {
		rows, err := d.pool.Query(ctx, `
			SELECT address, tx_count FROM address_stats
			WHERE LOWER(address) LIKE LOWER($1) || '%'
			ORDER BY tx_count DESC LIMIT $2`, query, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var address string
			var txCount int
			if err := rows.Scan(&address, &txCount); err != nil {
				return nil, err
			}
			suggestions = append(suggestions, types.SearchSuggestion{
				Type:  "address",
				Value: address,
				Label: fmt.Sprintf("%s (%d txs)", truncateHash(address), txCount),
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	if len(query) >= 1 {
		rows, err := d.pool.Query(ctx, `
			SELECT address, symbol, name FROM tokens
			WHERE LOWER(symbol) LIKE LOWER($1) || '%' OR LOWER(name) LIKE '%' || LOWER($1) || '%'
			ORDER BY holder_count DESC LIMIT $2`, query, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var address, symbol string
			var name *string
			if err := rows.Scan(&address, &symbol, &name); err != nil {
				return nil, err
			}
			label := symbol
			if name != nil && *name != "" {
				label = fmt.Sprintf("%s (%s)", symbol, *name)
			}
			suggestions = append(suggestions, types.SearchSuggestion{
				Type:  "token",
				Value: address,
				Label: label,
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	return suggestions, nil
}

func isNumeric(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

func truncateHash(hash string) string {
	if len(hash) <= 16 {
		return hash
	}
	return hash[:10] + "..." + hash[len(hash)-6:]
}

type GasPercentiles struct {
	SlowWei   *uint64
	NormalWei *uint64
	FastWei   *uint64
	BaseFee   *uint64
}

func (d *DB) GetGasPercentiles(ctx context.Context, numBlocks int, slowPct, avgPct, fastPct float64) (*GasPercentiles, error) {
	var slow, normal, fast, baseFee *uint64

	query := `
		WITH recent_txs AS (
			SELECT t.gas_price
			FROM transactions t
			WHERE t.gas_price > 0
			ORDER BY t.block_number DESC
			LIMIT 10000
		)
		SELECT
			COALESCE(PERCENTILE_CONT($1) WITHIN GROUP (ORDER BY gas_price)::bigint, 0) as slow,
			COALESCE(PERCENTILE_CONT($2) WITHIN GROUP (ORDER BY gas_price)::bigint, 0) as normal,
			COALESCE(PERCENTILE_CONT($3) WITHIN GROUP (ORDER BY gas_price)::bigint, 0) as fast,
			(SELECT base_fee_per_gas FROM blocks WHERE base_fee_per_gas IS NOT NULL ORDER BY number DESC LIMIT 1) as base_fee
		FROM recent_txs
	`

	err := d.pool.QueryRow(ctx, query, slowPct, avgPct, fastPct).Scan(&slow, &normal, &fast, &baseFee)
	if err != nil {
		return nil, err
	}

	return &GasPercentiles{
		SlowWei:   slow,
		NormalWei: normal,
		FastWei:   fast,
		BaseFee:   baseFee,
	}, nil
}

// Daily Stats operations

func (d *DB) UpsertDailyStats(ctx context.Context, stats *types.DailyStats) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO daily_stats (date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
			successful_txs, failed_txs, active_addresses, new_addresses, avg_block_time, avg_block_size,
			new_contracts, token_transfer_count, cumulative_transactions, cumulative_addresses, cumulative_contracts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (date) DO UPDATE SET
			total_blocks = EXCLUDED.total_blocks,
			total_transactions = EXCLUDED.total_transactions,
			total_gas_used = EXCLUDED.total_gas_used,
			avg_gas_price = EXCLUDED.avg_gas_price,
			successful_txs = EXCLUDED.successful_txs,
			failed_txs = EXCLUDED.failed_txs,
			active_addresses = EXCLUDED.active_addresses,
			new_addresses = EXCLUDED.new_addresses,
			avg_block_time = EXCLUDED.avg_block_time,
			avg_block_size = EXCLUDED.avg_block_size,
			new_contracts = EXCLUDED.new_contracts,
			token_transfer_count = EXCLUDED.token_transfer_count,
			cumulative_transactions = EXCLUDED.cumulative_transactions,
			cumulative_addresses = EXCLUDED.cumulative_addresses,
			cumulative_contracts = EXCLUDED.cumulative_contracts`,
		stats.Date, stats.TotalBlocks, stats.TotalTransactions, stats.TotalGasUsed, stats.AvgGasPrice,
		stats.SuccessfulTxs, stats.FailedTxs, stats.ActiveAddresses, stats.NewAddresses, stats.AvgBlockTime,
		stats.AvgBlockSize, stats.NewContracts, stats.TokenTransferCount, stats.CumulativeTransactions,
		stats.CumulativeAddresses, stats.CumulativeContracts)
	return err
}

func (d *DB) GetDailyStats(ctx context.Context, from, to time.Time) ([]types.DailyStats, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
			successful_txs, failed_txs, active_addresses, new_addresses, avg_block_time, avg_block_size,
			new_contracts, token_transfer_count, cumulative_transactions, cumulative_addresses, cumulative_contracts
		FROM daily_stats
		WHERE date >= $1 AND date <= $2
		ORDER BY date ASC`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []types.DailyStats
	for rows.Next() {
		var s types.DailyStats
		var date time.Time
		if err := rows.Scan(&date, &s.TotalBlocks, &s.TotalTransactions, &s.TotalGasUsed, &s.AvgGasPrice,
			&s.SuccessfulTxs, &s.FailedTxs, &s.ActiveAddresses, &s.NewAddresses, &s.AvgBlockTime,
			&s.AvgBlockSize, &s.NewContracts, &s.TokenTransferCount, &s.CumulativeTransactions,
			&s.CumulativeAddresses, &s.CumulativeContracts); err != nil {
			return nil, err
		}
		s.Date = date.Format("2006-01-02")
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

func (d *DB) GetDailyStatsForDate(ctx context.Context, date time.Time) (*types.DailyStats, error) {
	var s types.DailyStats
	var dt time.Time
	err := d.pool.QueryRow(ctx, `
		SELECT date, total_blocks, total_transactions, total_gas_used, avg_gas_price,
			successful_txs, failed_txs, active_addresses, new_addresses, avg_block_time, avg_block_size,
			new_contracts, token_transfer_count, cumulative_transactions, cumulative_addresses, cumulative_contracts
		FROM daily_stats
		WHERE date = $1`, date).Scan(&dt, &s.TotalBlocks, &s.TotalTransactions, &s.TotalGasUsed, &s.AvgGasPrice,
		&s.SuccessfulTxs, &s.FailedTxs, &s.ActiveAddresses, &s.NewAddresses, &s.AvgBlockTime,
		&s.AvgBlockSize, &s.NewContracts, &s.TokenTransferCount, &s.CumulativeTransactions,
		&s.CumulativeAddresses, &s.CumulativeContracts)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.Date = dt.Format("2006-01-02")
	return &s, nil
}

func (d *DB) ComputeDailyStats(ctx context.Context, date time.Time) (*types.DailyStats, error) {
	dateStr := date.Format("2006-01-02")
	s := &types.DailyStats{Date: dateStr}

	// total_blocks, total_gas_used, avg_block_size
	err := d.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(SUM(gas_used)::bigint, 0), COALESCE(AVG(size)::bigint, 0)
		FROM blocks b
		WHERE b.timestamp >= extract(epoch from $1::date)::bigint
		  AND b.timestamp < extract(epoch from ($1::date + interval '1 day'))::bigint`, dateStr).
		Scan(&s.TotalBlocks, &s.TotalGasUsed, &s.AvgBlockSize)
	if err != nil {
		return nil, fmt.Errorf("compute blocks stats: %w", err)
	}

	// total_transactions, avg_gas_price, successful_txs, failed_txs
	err = d.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(AVG(t.gas_price)::bigint, 0),
			COUNT(*) FILTER (WHERE t.status = 1),
			COUNT(*) FILTER (WHERE t.status = 0)
		FROM transactions t
		JOIN blocks b ON t.block_number = b.number
		WHERE b.timestamp >= extract(epoch from $1::date)::bigint
		  AND b.timestamp < extract(epoch from ($1::date + interval '1 day'))::bigint`, dateStr).
		Scan(&s.TotalTransactions, &s.AvgGasPrice, &s.SuccessfulTxs, &s.FailedTxs)
	if err != nil {
		return nil, fmt.Errorf("compute tx stats: %w", err)
	}

	// active_addresses (distinct from + to in transactions for this day)
	err = d.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT addr) FROM (
			SELECT from_address AS addr FROM transactions t
			JOIN blocks b ON t.block_number = b.number
			WHERE b.timestamp >= extract(epoch from $1::date)::bigint
			  AND b.timestamp < extract(epoch from ($1::date + interval '1 day'))::bigint
			UNION
			SELECT to_address AS addr FROM transactions t
			JOIN blocks b ON t.block_number = b.number
			WHERE b.timestamp >= extract(epoch from $1::date)::bigint
			  AND b.timestamp < extract(epoch from ($1::date + interval '1 day'))::bigint
			  AND t.to_address IS NOT NULL
		) sub`, dateStr).Scan(&s.ActiveAddresses)
	if err != nil {
		return nil, fmt.Errorf("compute active addresses: %w", err)
	}

	// new_addresses
	err = d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM address_stats
		WHERE first_seen >= extract(epoch from $1::date)::bigint
		  AND first_seen < extract(epoch from ($1::date + interval '1 day'))::bigint`, dateStr).
		Scan(&s.NewAddresses)
	if err != nil {
		return nil, fmt.Errorf("compute new addresses: %w", err)
	}

	// avg_block_time
	err = d.pool.QueryRow(ctx, `
		SELECT COALESCE(
			AVG(b.timestamp - prev.timestamp)::float,
			0
		)
		FROM blocks b
		JOIN blocks prev ON prev.number = b.number - 1
		WHERE b.timestamp >= extract(epoch from $1::date)::bigint
		  AND b.timestamp < extract(epoch from ($1::date + interval '1 day'))::bigint`, dateStr).
		Scan(&s.AvgBlockTime)
	if err != nil {
		return nil, fmt.Errorf("compute avg block time: %w", err)
	}

	// new_contracts
	err = d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM address_stats
		WHERE is_contract = true
		  AND first_seen >= extract(epoch from $1::date)::bigint
		  AND first_seen < extract(epoch from ($1::date + interval '1 day'))::bigint`, dateStr).
		Scan(&s.NewContracts)
	if err != nil {
		return nil, fmt.Errorf("compute new contracts: %w", err)
	}

	// token_transfer_count
	err = d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM token_transfers tt
		JOIN blocks b ON tt.block_number = b.number
		WHERE b.timestamp >= extract(epoch from $1::date)::bigint
		  AND b.timestamp < extract(epoch from ($1::date + interval '1 day'))::bigint`, dateStr).
		Scan(&s.TokenTransferCount)
	if err != nil {
		return nil, fmt.Errorf("compute token transfers: %w", err)
	}

	// cumulative_transactions: sum of all total_transactions up to and including this date
	err = d.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(cnt), 0) FROM (
			SELECT COUNT(*) as cnt
			FROM transactions t
			JOIN blocks b ON t.block_number = b.number
			WHERE b.timestamp < extract(epoch from ($1::date + interval '1 day'))::bigint
		) sub`, dateStr).Scan(&s.CumulativeTransactions)
	if err != nil {
		return nil, fmt.Errorf("compute cumulative transactions: %w", err)
	}

	// cumulative_addresses
	err = d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM address_stats
		WHERE first_seen < extract(epoch from ($1::date + interval '1 day'))::bigint`, dateStr).
		Scan(&s.CumulativeAddresses)
	if err != nil {
		return nil, fmt.Errorf("compute cumulative addresses: %w", err)
	}

	// cumulative_contracts
	err = d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM address_stats
		WHERE is_contract = true
		  AND first_seen < extract(epoch from ($1::date + interval '1 day'))::bigint`, dateStr).
		Scan(&s.CumulativeContracts)
	if err != nil {
		return nil, fmt.Errorf("compute cumulative contracts: %w", err)
	}

	return s, nil
}

// WipeAllData truncates all indexed tables, used for chain reset recovery.
func (d *DB) WipeAllData(ctx context.Context) error {
	// Order matters: truncate tables with foreign key dependencies using CASCADE.
	// TRUNCATE ... CASCADE handles referential integrity automatically, but we
	// list child tables first for clarity.
	tables := []string{
		"daily_stats",
		"internal_transactions",
		"logs",
		"token_transfers",
		"balances",
		"op_deposits",
		"contracts",
		"transactions",
		"blocks",
		"tokens",
		"counters",
		"address_stats",
		"indexer_progress",
		"missing_block_ranges",
		"sync_status",
	}
	for _, table := range tables {
		if _, err := d.pool.Exec(ctx, "TRUNCATE TABLE "+table+" CASCADE"); err != nil {
			return fmt.Errorf("failed to truncate %s: %w", table, err)
		}
	}
	// Re-seed the singleton rows that migrations created.
	if _, err := d.pool.Exec(ctx,
		"INSERT INTO sync_status (last_indexed_block, is_syncing) VALUES (0, false) ON CONFLICT DO NOTHING"); err != nil {
		return fmt.Errorf("failed to re-seed sync_status: %w", err)
	}
	if _, err := d.pool.Exec(ctx,
		"INSERT INTO indexer_progress (min_fetched_block, max_fetched_block, backfill_complete) VALUES (0, 0, false) ON CONFLICT DO NOTHING"); err != nil {
		return fmt.Errorf("failed to re-seed indexer_progress: %w", err)
	}
	return nil
}

func (d *DB) BackfillDailyStats(ctx context.Context) error {
	// Find earliest and latest block dates
	var minTimestamp, maxTimestamp *int64
	err := d.pool.QueryRow(ctx, `SELECT MIN(timestamp), MAX(timestamp) FROM blocks`).Scan(&minTimestamp, &maxTimestamp)
	if err != nil || minTimestamp == nil || maxTimestamp == nil {
		return err
	}

	startDate := time.Unix(*minTimestamp, 0).UTC().Truncate(24 * time.Hour)
	endDate := time.Unix(*maxTimestamp, 0).UTC().Truncate(24 * time.Hour)

	for d_ := startDate; !d_.After(endDate); d_ = d_.AddDate(0, 0, 1) {
		// Skip dates that already exist
		existing, err := d.GetDailyStatsForDate(ctx, d_)
		if err != nil {
			return fmt.Errorf("check existing stats for %s: %w", d_.Format("2006-01-02"), err)
		}
		if existing != nil {
			continue
		}

		stats, err := d.ComputeDailyStats(ctx, d_)
		if err != nil {
			return fmt.Errorf("compute stats for %s: %w", d_.Format("2006-01-02"), err)
		}

		if err := d.UpsertDailyStats(ctx, stats); err != nil {
			return fmt.Errorf("upsert stats for %s: %w", d_.Format("2006-01-02"), err)
		}
	}

	return nil
}
