package db

import (
	"context"

	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/jackc/pgx/v5"
)

func (d *DB) InsertOPDepositsBatch(ctx context.Context, deposits []*types.OPDeposit) error {
	if len(deposits) == 0 {
		return nil
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, dep := range deposits {
		batch.Queue(`
			INSERT INTO op_deposits (l2_tx_hash, l1_block_number, l1_block_timestamp, l1_tx_hash, l1_tx_origin)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (l2_tx_hash) DO NOTHING`,
			dep.L2TxHash, dep.L1BlockNumber, dep.L1BlockTimestamp, dep.L1TxHash, dep.L1TxOrigin)
	}

	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (d *DB) GetOPDeposit(ctx context.Context, l2TxHash string) (*types.OPDeposit, error) {
	var dep types.OPDeposit
	err := d.pool.QueryRow(ctx, `
		SELECT l2_tx_hash, l1_block_number, l1_block_timestamp, l1_tx_hash, l1_tx_origin, created_at
		FROM op_deposits
		WHERE l2_tx_hash = $1`, l2TxHash).Scan(
		&dep.L2TxHash, &dep.L1BlockNumber, &dep.L1BlockTimestamp,
		&dep.L1TxHash, &dep.L1TxOrigin, &dep.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &dep, nil
}

func (d *DB) GetLastIndexedL1Block(ctx context.Context) (uint64, error) {
	var blockNum *uint64
	err := d.pool.QueryRow(ctx, `
		SELECT MAX(l1_block_number) FROM op_deposits`).Scan(&blockNum)
	if err != nil {
		return 0, err
	}
	if blockNum == nil {
		return 0, nil
	}
	return *blockNum, nil
}

// GetOPDepositByL1Hash looks up a deposit by its L1 transaction hash.
func (d *DB) GetOPDepositByL1Hash(ctx context.Context, l1TxHash string) (*types.OPDeposit, error) {
	var dep types.OPDeposit
	err := d.pool.QueryRow(ctx, `
		SELECT l2_tx_hash, l1_block_number, l1_block_timestamp, l1_tx_hash, l1_tx_origin, created_at
		FROM op_deposits
		WHERE l1_tx_hash = $1`, l1TxHash).Scan(
		&dep.L2TxHash, &dep.L1BlockNumber, &dep.L1BlockTimestamp,
		&dep.L1TxHash, &dep.L1TxOrigin, &dep.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &dep, nil
}

// ListOPDeposits returns deposits ordered by l1_block_number DESC with
// optional cursor pagination. Filters on `fromAddress` / `toAddress` are
// passed through as nil when unset.
//
// `afterL1Block` implements cursor pagination: only rows with
// l1_block_number strictly less than this value are returned. Pass 0 for
// the first page.
func (d *DB) ListOPDeposits(ctx context.Context, fromAddress, toAddress *string, l1From, l1To *uint64, afterL1Block uint64, limit int) ([]types.OPDeposit, error) {
	query := `
		SELECT l2_tx_hash, l1_block_number, l1_block_timestamp, l1_tx_hash, l1_tx_origin, created_at
		FROM op_deposits
		WHERE ($1::text IS NULL OR l1_tx_origin = $1)
		  AND ($2::bigint IS NULL OR l1_block_number >= $2)
		  AND ($3::bigint IS NULL OR l1_block_number < $3)
		  AND ($4::bigint = 0 OR l1_block_number < $4)
		ORDER BY l1_block_number DESC
		LIMIT $5`
	// toAddress is accepted for API symmetry but the current schema stores
	// only the L1 origin. Filter is treated as the same as fromAddress if set.
	origin := fromAddress
	if origin == nil {
		origin = toAddress
	}
	rows, err := d.pool.Query(ctx, query, origin, l1From, l1To, afterL1Block, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.OPDeposit
	for rows.Next() {
		var dep types.OPDeposit
		if err := rows.Scan(&dep.L2TxHash, &dep.L1BlockNumber, &dep.L1BlockTimestamp,
			&dep.L1TxHash, &dep.L1TxOrigin, &dep.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, dep)
	}
	return out, rows.Err()
}
