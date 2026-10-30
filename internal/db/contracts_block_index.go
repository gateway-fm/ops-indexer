package db

import "context"

// EnsureContractsBlockIndex indexes contracts by block_number. The ledger reads a span's contracts by block range
// at every close, and contracts is neither partitioned nor otherwise indexed by block.
func (d *DB) EnsureContractsBlockIndex(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_contracts_block_number ON contracts (block_number)`)
	return err
}
