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
