package db

import (
	"context"
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// A balance flush takes the per-key advisory lock before it reads a key's old balance, so two
// flushes on a key not yet in token_balances_current cannot both read "absent" and both count a holder.
func TestBalanceFlushTakesTheKeyAdvisoryLock(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	waitsWhileHeld(t, d, `SELECT pg_advisory_xact_lock(4669, $1)`, []any{tbcLockKey("0x70c1", "0xa")}, func() error {
		return d.InsertBalancesBatch(context.Background(), []*types.Balance{
			{Address: "0xa", TokenAddress: "0x70c1", BlockNumber: 1, Balance: "5"},
		})
	})
}
