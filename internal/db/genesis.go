package db

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/jackc/pgx/v5"
)

// EnsureInitialGenesis retains a real, empty genesis header when full-history
// indexing starts. The standard ingest cursor begins at 1 in an empty store;
// leaving block 0 out would make an exact [0,head] aggregate incomplete.
// A retention cut is never undone by reinserting genesis.
func (d *DB) EnsureInitialGenesis(ctx context.Context, b *types.Block) error {
	if b == nil || b.Number != 0 || b.TransactionCount != 0 {
		return ErrDatasetIdentity
	}
	hash, err := hex.DecodeString(strings.TrimPrefix(b.Hash, "0x"))
	if err != nil || len(hash) != 32 {
		return ErrDatasetIdentity
	}
	var inserted bool
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1,1)`, historyDDLGate); err != nil {
			return err
		}
		var floor uint64
		var identityOK, wiping bool
		if err := tx.QueryRow(ctx, `SELECT history_from(),genesis_hash=$1,to_regclass('public.wipe_in_progress') IS NOT NULL FROM indexer_dataset_metadata WHERE id=1`, hash).Scan(&floor, &identityOK, &wiping); err != nil {
			return err
		}
		if wiping {
			return ErrHistoryWipeInProgress
		}
		if !identityOK {
			return ErrDatasetIdentity
		}
		if floor > 0 {
			return nil
		}
		var err error
		inserted, err = insertBlockRow(ctx, tx, b)
		return err
	})
	if err == nil && inserted {
		d.lateDays.noteBlock(0, b.Timestamp)
	}
	return err
}
