package db

import (
	"context"
	_ "embed"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/gateway-fm/chain-indexer/internal/log"
)

// tokens.holder_count is maintained incrementally by InsertBalancesBatch, in
// the same transaction as the token_balances_current rows it describes, the
// way transfer_count and total_supply are maintained by InsertBlockDataBatch.
//
// Before PRST-4669 it was recomputed as COUNT(*) FROM token_balances_current
// WHERE token_address = $1 AND balance > 0 -- once per touched token per block
// and once per token per balance-worker flush. That count is linear in the
// token's holder count: 77.8 ms at ~100k holders, 918 ms at 826k, and it was
// 96.9% of all database time in a 500 tx/s load test, capping ingest at ~70 tx/s.
// The incremental form costs one row read and one row write per flushed key
// regardless of how many holders the token has.
//
// The statements live in sql/*.sql so an external check can execute the
// exact text the binary runs against a live database.
var (
	//go:embed sql/tbc_lock_keys.sql
	tbcLockKeysSQL string
	//go:embed sql/tbc_old_pos.sql
	tbcOldPosSQL string
	//go:embed sql/tbc_upsert_delta.sql
	tbcUpsertDeltaSQL string
	//go:embed sql/holder_count_apply.sql
	holderCountApplySQL string
	//go:embed sql/holder_count_reseed.sql
	holderCountReseedSQL string
)

// tbcLockKey is the advisory-lock key for one (token, address) pair: FNV-1a
// over "token|address", both lowercased, folded to int32 for the two-int
// pg_advisory_xact_lock form (see sql/tbc_lock_keys.sql).
func tbcLockKey(token, address string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(token)))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(strings.ToLower(address)))
	return int32(h.Sum32())
}

// tbcLockKeys returns the sorted, deduplicated advisory keys for a set of
// (token, address) pairs given as parallel slices.
func tbcLockKeys(tokens, addresses []string) []int32 {
	seen := make(map[int32]struct{}, len(tokens))
	keys := make([]int32, 0, len(tokens))
	for i := range tokens {
		k := tbcLockKey(tokens[i], addresses[i])
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(a, b int) bool { return keys[a] < keys[b] })
	return keys
}

// ReseedHolderCounts rewrites tokens.holder_count from token_balances_current
// for every token whose stored value differs. Called once at startup.
func (d *DB) ReseedHolderCounts(ctx context.Context) error {
	tag, err := d.pool.Exec(ctx, holderCountReseedSQL)
	if err != nil {
		return fmt.Errorf("re-seed holder_count: %w", err)
	}
	log.Info("holder_count re-seeded from token_balances_current", "rows_repaired", tag.RowsAffected())
	return nil
}

// countTokenHolders is the absolute form of holder_count: addresses whose
// current balance of the token is positive. It is no longer on any ingest
// path; the startup re-seed and the tests use it as the reference the
// incremental counter must agree with.
func (d *DB) countTokenHolders(ctx context.Context, lowercasedToken string) (int64, error) {
	var holderCount int64
	if err := d.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM token_balances_current
		WHERE token_address = $1 AND balance > 0`, lowercasedToken,
	).Scan(&holderCount); err != nil {
		return 0, fmt.Errorf("count holders: %w", err)
	}
	return holderCount, nil
}
