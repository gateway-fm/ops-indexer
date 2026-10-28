package db

import (
	"context"
	"fmt"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
)

// txHashLookupTables lost their last index on tx_hash with migration 013. Consumers that read the database
// directly look logs and transfers up by transaction hash alone.
var txHashLookupTables = []string{"logs", "token_transfers"}

// EnsureTxHashIndexes creates a plain index on tx_hash for each of txHashLookupTables that has none on the table
// itself (TX_HASH_INDEXES). Postgres names it; on a partitioned table it covers every partition and attaches an
// equivalent index a partition already has instead of building another.
func (d *DB) EnsureTxHashIndexes(ctx context.Context) error {
	for _, table := range txHashLookupTables {
		var present bool
		if err := d.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_index i
				JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
				WHERE i.indrelid = $1::regclass AND i.indnatts = 1 AND a.attname = 'tx_hash'
				  AND i.indexprs IS NULL AND i.indpred IS NULL AND i.indisvalid)`, table).Scan(&present); err != nil {
			return err
		}
		if present {
			continue
		}
		log.Info("building the tx_hash lookup index; writes to the table wait until it is built", "table", table)
		start := time.Now()
		if _, err := d.pool.Exec(ctx, fmt.Sprintf(`CREATE INDEX ON %s (tx_hash)`, table)); err != nil {
			return fmt.Errorf("tx_hash index on %s: %w", table, err)
		}
		log.Info("tx_hash lookup index built", "table", table, "duration", time.Since(start).Round(time.Millisecond))
	}
	return nil
}
