package db

import (
	"context"
	"errors"
	"math/big"

	"github.com/jackc/pgx/v5"
)

var ErrDatasetIdentity = errors.New("configured node differs from the indexed dataset")

// EnsureDatasetIdentity records the identity supplied by the configured node,
// never by an API caller. Ordinary starts keep the durable epoch unchanged.
// It runs after startup recovery/wipe and refuses a conflicting existing chain.
func (d *DB) EnsureDatasetIdentity(ctx context.Context, chainID string, genesis []byte) error {
	n, ok := new(big.Int).SetString(chainID, 10)
	if !ok || n.Sign() < 0 || n.BitLen() > 256 || n.String() != chainID || len(genesis) != 32 {
		return ErrDatasetIdentity
	}
	return pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		// READ COMMITTED takes a fresh snapshot after this gate is acquired.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1,1)`, historyDDLGate); err != nil {
			return err
		}
		var wiping bool
		if err := tx.QueryRow(ctx, `SELECT to_regclass('public.wipe_in_progress') IS NOT NULL`).Scan(&wiping); err != nil {
			return err
		}
		if wiping {
			return ErrHistoryWipeInProgress
		}
		tag, err := tx.Exec(ctx, `UPDATE indexer_dataset_metadata SET chain_id=$1,genesis_hash=$2
		 WHERE id=1 AND (chain_id IS NULL OR (chain_id=$1 AND genesis_hash=$2))`, chainID, genesis)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrDatasetIdentity
		}
		return nil
	})
}

// RotateDatasetEpoch invalidates all continuations after an operator restores
// or replaces this database. It is an offline administration action; a normal
// process restart must never call it. Chain identity remains subject to the
// configured-node comparison when the process next starts.
func (d *DB) RotateDatasetEpoch(ctx context.Context) error {
	release, err := d.holdHistoryDDLGate(ctx, cutGateWait)
	if err != nil {
		return err
	}
	defer release()
	tag, err := d.pool.Exec(ctx, `UPDATE indexer_dataset_metadata SET dataset_epoch=gen_random_uuid() WHERE id=1`)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrDatasetIdentity
	}
	return nil
}
